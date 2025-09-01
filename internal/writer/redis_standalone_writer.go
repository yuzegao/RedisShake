package writer

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"go.uber.org/ratelimit"

	"RedisShake/internal/client"
	"RedisShake/internal/client/proto"
	"RedisShake/internal/config"
	"RedisShake/internal/entry"
	"RedisShake/internal/log"
)

type RedisWriterOptions struct {
	Cluster   bool                   `mapstructure:"cluster" default:"false"`
	Address   string                 `mapstructure:"address" default:""`
	Username  string                 `mapstructure:"username" default:""`
	Password  string                 `mapstructure:"password" default:""`
	Tls       bool                   `mapstructure:"tls" default:"false"`
	TlsConfig client.TlsConfig       `mapstructure:"tls_config" default:"{}"`
	OffReply  bool                   `mapstructure:"off_reply" default:"false"`
	Sentinel  client.SentinelOptions `mapstructure:"sentinel"`
}

type redisStandaloneWriter struct {
	address string
	client  *client.Redis
	DbId    int

	chWaitReply chan *entry.Entry
	chWaitWg    sync.WaitGroup
	offReply    bool
	ch          chan *entry.Entry
	chWg        sync.WaitGroup

	// For skip_existing_keys feature
	skippedKeys map[string]bool // Track keys that should be skipped in current session
	checkClient *client.Redis   // Separate client for EXISTS checks to avoid pipeline conflicts

	stat struct {
		Name              string `json:"name"`
		UnansweredBytes   int64  `json:"unanswered_bytes"`
		UnansweredEntries int64  `json:"unanswered_entries"`
	}
}

func NewRedisStandaloneWriter(ctx context.Context, opts *RedisWriterOptions) Writer {
	rw := new(redisStandaloneWriter)
	rw.address = opts.Address
	rw.stat.Name = "writer_" + strings.Replace(opts.Address, ":", "_", -1)
	rw.client = client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
	rw.ch = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit)
	rw.skippedKeys = make(map[string]bool) // Initialize skipped keys map

	// Create separate client for EXISTS checks if skip_existing_keys is enabled
	if config.Opt.Advanced.SkipExistingKeys {
		rw.checkClient = client.NewRedisClient(ctx, opts.Address, opts.Username, opts.Password, opts.Tls, opts.TlsConfig, false)
	}

	if opts.OffReply {
		log.Infof("turn off the reply of write")
		rw.offReply = true
		rw.client.Send("CLIENT", "REPLY", "OFF")
	} else {
		rw.chWaitReply = make(chan *entry.Entry, config.Opt.Advanced.PipelineCountLimit*2)
		rw.chWaitWg.Add(1)
		go rw.processReply()
	}
	return rw
}

func (w *redisStandaloneWriter) Close() {
	if !w.offReply {
		close(w.ch)
		w.chWg.Wait()
		close(w.chWaitReply)
		w.chWaitWg.Wait()
	}
	// Close the separate check client if it exists
	if w.checkClient != nil {
		w.checkClient.Close()
	}
}

func (w *redisStandaloneWriter) StartWrite(ctx context.Context) chan *entry.Entry {
	w.chWg = sync.WaitGroup{}
	w.chWg.Add(1)
	go w.processWrite(ctx)
	return w.ch
}

func (w *redisStandaloneWriter) Write(e *entry.Entry) {
	w.ch <- e
}

func (w *redisStandaloneWriter) switchDbTo(newDbId int) {
	log.Debugf("[%s] switch db to [%d]", w.stat.Name, newDbId)
	w.client.Send("select", strconv.Itoa(newDbId))
	w.DbId = newDbId
	if !w.offReply {
		w.chWaitReply <- &entry.Entry{
			Argv:    []string{"select", strconv.Itoa(newDbId)},
			CmdName: "select",
		}
	}
}

func (w *redisStandaloneWriter) processWrite(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	var rl ratelimit.Limiter = nil
	rl = ratelimit.New(config.Opt.Advanced.TargetRedisMaxQPS)
	log.Infof("set target redis max qps to %d", config.Opt.Advanced.TargetRedisMaxQPS)
	for {
		select {
		case <-ctx.Done():
			// do nothing until w.ch is closed
		case <-ticker.C:
			w.client.Flush()
		case e, ok := <-w.ch:
			if !ok {
				// clean up and exit
				w.client.Flush()
				w.chWg.Done()
				return
			}
			// switch db if we need
			if w.DbId != e.DbId {
				w.switchDbTo(e.DbId)
			}
			// handle skip_existing_keys logic
			if config.Opt.Advanced.SkipExistingKeys && w.shouldProcessForSkip(e) {
				continue
			}
			// send
			bytes := e.Serialize()
			for e.SerializedSize+atomic.LoadInt64(&w.stat.UnansweredBytes) > config.Opt.Advanced.TargetRedisClientMaxQuerybufLen {
				time.Sleep(1 * time.Nanosecond)
			}
			rl.Take()
			log.Debugf("[%s] send cmd. cmd=[%s]", w.stat.Name, e.String())
			if !w.offReply {
				select {
				case w.chWaitReply <- e:
				default:
					w.client.Flush()
					w.chWaitReply <- e
				}
				atomic.AddInt64(&w.stat.UnansweredBytes, e.SerializedSize)
				atomic.AddInt64(&w.stat.UnansweredEntries, 1)
			}
			w.client.SendBytesBuff(bytes)
		}
	}
}

func (w *redisStandaloneWriter) processReply() {
	for e := range w.chWaitReply {
		reply, err := w.client.Receive()
		log.Debugf("[%s] receive reply. reply=[%v], cmd=[%s]", w.stat.Name, reply, e.String())

		// It's good to skip the nil error since some write commands will return the null reply. For example,
		// the SET command with NX option will return nil if the key already exists.
		if err != nil && !errors.Is(err, proto.Nil) {
			if err.Error() == "BUSYKEY Target key name already exists." {
				if config.Opt.Advanced.RDBRestoreCommandBehavior == "skip" {
					log.Debugf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				} else if config.Opt.Advanced.RDBRestoreCommandBehavior == "panic" {
					log.Panicf("[%s] redisStandaloneWriter received BUSYKEY reply. cmd=[%s]", w.stat.Name, e.String())
				}
			} else {
				log.Panicf("[%s] receive reply failed. cmd=[%s], error=[%v]", w.stat.Name, e.String(), err)
			}
		}
		if strings.EqualFold(e.CmdName, "select") { // skip select command
			continue
		}
		atomic.AddInt64(&w.stat.UnansweredBytes, -e.SerializedSize)
		atomic.AddInt64(&w.stat.UnansweredEntries, -1)
	}
	w.chWaitWg.Done()
}

func (w *redisStandaloneWriter) Status() interface{} {
	return w.stat
}

func (w *redisStandaloneWriter) StatusString() string {
	return fmt.Sprintf("[%s]: unanswered_entries=%d", w.stat.Name, atomic.LoadInt64(&w.stat.UnansweredEntries))
}

func (w *redisStandaloneWriter) StatusConsistent() bool {
	return atomic.LoadInt64(&w.stat.UnansweredBytes) == 0 && atomic.LoadInt64(&w.stat.UnansweredEntries) == 0
}

// shouldProcessForSkip handles the skip_existing_keys logic
// Returns true if the command should be skipped
func (w *redisStandaloneWriter) shouldProcessForSkip(e *entry.Entry) bool {
	// Parse entry to get command info if not already parsed
	if e.CmdName == "" {
		e.Parse()
	}

	// Only process commands that have at least one key
	if len(e.Keys) == 0 {
		return false
	}

	key := e.Keys[0]
	keyWithDb := w.getKeyWithDb(key)
	cmdName := strings.ToUpper(e.CmdName)

	// Handle DEL command - check if key should be skipped, if so, mark it as skipped
	if cmdName == "DEL" {
		if w.keyExists(key) {
			w.skippedKeys[keyWithDb] = true
			log.Debugf("[%s] mark key as skipped: %s", w.stat.Name, key)
			return true // Skip the DEL command
		}
		return false // Key doesn't exist, execute DEL normally
	}

	// For data-writing commands, check if key is already marked as skipped
	switch cmdName {
	case "SET", "HSET", "HMSET", "LPUSH", "RPUSH", "SADD", "ZADD", "MSET":
		if w.skippedKeys[keyWithDb] {
			log.Debugf("[%s] skip command for existing key: %s", w.stat.Name, key)
			return true // Skip this command
		}
	}

	return false // Don't skip
}

// getKeyWithDb returns a unique key identifier including database
func (w *redisStandaloneWriter) getKeyWithDb(key string) string {
	return fmt.Sprintf("db%d:%s", w.DbId, key)
}

// keyExists checks if a key exists in the target Redis
func (w *redisStandaloneWriter) keyExists(key string) bool {
	if w.checkClient == nil {
		// Fallback: if checkClient is not available, assume key doesn't exist
		return false
	}

	// Ensure we're on the correct database
	if w.DbId != 0 {
		w.checkClient.DoWithStringReply("SELECT", strconv.Itoa(w.DbId))
	}

	reply := w.checkClient.Do("EXISTS", key)
	switch v := reply.(type) {
	case int64:
		return v == 1
	case int:
		return v == 1
	case string:
		return v == "1"
	default:
		return false // On unexpected type, assume key doesn't exist and proceed with write
	}
}
