package scheduler

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"time"

	schedulerv1 "agentenv/services/api/proto"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// NodeStreamBus carries one replica's view of a node to the replicas that did
// not receive that node's heartbeat.
//
// It is an interface so the registry that sits on it can be tested without a
// Redis, and so a deployment that runs one scheduler pays for none of it.
type NodeStreamBus interface {
	// Publish sends one node's state to every other replica. It must not block
	// the heartbeat RPC that produced the event: a Redis that has stalled is a
	// reason to lose convergence for one interval, never a reason to fail a
	// node's heartbeat.
	Publish(ctx context.Context, nodeID string, ev *schedulerv1.NodeStateEvent) error
	// Subscribe delivers every event on the bus to fn until ctx is done. The
	// returned channel closes once the retained backlog has been replayed, so
	// a starting replica can wait to serve traffic until it knows the fleet.
	Subscribe(ctx context.Context, fn func(*schedulerv1.NodeStateEvent)) (<-chan struct{}, error)
	Close() error
}

// nodeStreamShards is how many streams the fleet's node events are spread over.
//
// Fixed rather than configurable on purpose. A configurable count means a
// rolling change moves nodes between shards while some replicas still read the
// old set, and a reader that silently stops covering a shard loses those nodes
// with nothing to say so. Sixteen costs sixteen goroutines and sixteen
// connections per replica, and spreads across slots in a Redis Cluster because
// the keys carry no hash tag.
const nodeStreamShards = 16

const (
	defaultNodeStreamKeyPrefix = "agentenv:scheduler:nodes"
	// defaultNodeStreamMaxLen bounds each shard. It has to cover more than
	// twice the report TTL of the whole fleet's heartbeats, or a restarting
	// replica's warm-up misses nodes that are alive and heartbeating.
	defaultNodeStreamMaxLen = 100000
	// defaultNodeStreamPublishQueue bounds what is waiting to be written when
	// Redis is slow. Full means dropping, which costs one interval of staleness
	// for that node because the next heartbeat republishes everything.
	defaultNodeStreamPublishQueue = 4096
	// nodeStreamReadBlock is how long a shard reader parks on an empty stream.
	// Not indefinitely: a blocked read notices neither a cancelled context nor
	// a connection that has died under it.
	nodeStreamReadBlock = 5 * time.Second
	// nodeStreamReadErrorBackoff keeps a reader whose connection is refused
	// from spinning on it.
	nodeStreamReadErrorBackoff = 500 * time.Millisecond
	nodeStreamPayloadField     = "payload"
)

// NodeStreamOptions are the knobs a deployment has over the bus.
type NodeStreamOptions struct {
	Logger *zap.Logger
	// MaxLen bounds each shard, approximately: the trim is `MAXLEN ~`, which
	// lets Redis trim on radix-node boundaries instead of exactly.
	MaxLen int64
	// PublishQueue bounds the events waiting to be written.
	PublishQueue int
	// ReportTTL is how far back a starting replica reads. Twice the TTL, so
	// every node that is still inside its liveness window is replayed.
	ReportTTL time.Duration
	// OperationTimeout bounds a single non-blocking Redis call.
	OperationTimeout time.Duration
	// KeyPrefix names the streams. Tests give each case its own so a shared
	// server does not carry state between them.
	KeyPrefix string
}

func (o NodeStreamOptions) resolve() NodeStreamOptions {
	resolved := o
	if resolved.Logger == nil {
		resolved.Logger = zap.NewNop()
	}
	if resolved.MaxLen <= 0 {
		resolved.MaxLen = defaultNodeStreamMaxLen
	}
	if resolved.PublishQueue <= 0 {
		resolved.PublishQueue = defaultNodeStreamPublishQueue
	}
	if resolved.ReportTTL <= 0 {
		resolved.ReportTTL = defaultObservedReportTTL
	}
	if resolved.OperationTimeout <= 0 {
		resolved.OperationTimeout = defaultRedisOperationTimeout
	}
	if resolved.KeyPrefix == "" {
		resolved.KeyPrefix = defaultNodeStreamKeyPrefix
	}
	return resolved
}

// nodeStreamShard picks the stream a node's events go to. Every replica hashes
// the same way, so a node's heartbeat and any later event for it stay in one
// stream and therefore in one order.
func nodeStreamShard(nodeID string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(nodeID))
	return h.Sum32() % nodeStreamShards
}

// RedisNodeStream is the Redis Streams implementation of NodeStreamBus.
//
// Every replica reads every shard with a plain XREAD and no consumer group:
// each replica needs every message, not a share of them. Ordering across nodes
// is not required — each event carries the stamp it is applied with, and an
// event that arrives late is dropped by the receiving registry rather than
// applied out of order — which is what makes sharding by node id free.
type RedisNodeStream struct {
	client redis.UniversalClient
	opts   NodeStreamOptions
	// queue decouples the heartbeat RPC from the write. One writer drains it,
	// so events keep their per-node order without a lock.
	queue  chan nodeStreamPublish
	closed chan struct{}
}

type nodeStreamPublish struct {
	shard   uint32
	payload []byte
}

// NewRedisNodeStream dials its own client rather than sharing the binding
// store's.
//
// Sixteen blocking XREADs hold sixteen pool connections for as long as they
// run. Borrowing them from the store's pool would leave binding lookups
// queueing behind reads that are meant to be idle, which is a deadlock under
// any pool smaller than the shard count.
//
// ctx bounds the writer goroutine; Close releases the connections.
func NewRedisNodeStream(ctx context.Context, addr string, opts NodeStreamOptions) (*RedisNodeStream, error) {
	resolved := opts.resolve()
	addrs, redisOpts, err := parseRedisAddress(addr)
	if err != nil {
		return nil, err
	}
	// One connection per shard reader, plus room for the writer and for the
	// retries a reconnect makes.
	client, err := connectRedis(addrs, redisOpts, resolved.OperationTimeout, nodeStreamShards+8)
	if err != nil {
		return nil, err
	}

	stream := &RedisNodeStream{
		client: client,
		opts:   resolved,
		queue:  make(chan nodeStreamPublish, resolved.PublishQueue),
		closed: make(chan struct{}),
	}
	go stream.drainPublishQueue(ctx)
	return stream, nil
}

func (s *RedisNodeStream) key(shard uint32) string {
	return fmt.Sprintf("%s:%d", s.opts.KeyPrefix, shard)
}

func (s *RedisNodeStream) Publish(_ context.Context, nodeID string, ev *schedulerv1.NodeStateEvent) error {
	payload, err := proto.Marshal(ev)
	if err != nil {
		schedulerNodeStreamDroppedTotal.WithLabelValues("marshal_error").Inc()
		return err
	}

	select {
	case s.queue <- nodeStreamPublish{shard: nodeStreamShard(nodeID), payload: payload}:
		return nil
	default:
		// Dropping is safe for the same reason event loss is: the node's next
		// heartbeat republishes its whole state, so the cost is one interval of
		// staleness on the replicas that missed this one.
		schedulerNodeStreamDroppedTotal.WithLabelValues("queue_full").Inc()
		return nil
	}
}

func (s *RedisNodeStream) drainPublishQueue(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-s.closed:
			return
		case job := <-s.queue:
			s.write(ctx, job)
		}
	}
}

func (s *RedisNodeStream) write(ctx context.Context, job nodeStreamPublish) {
	writeCtx, cancel := context.WithTimeout(ctx, s.opts.OperationTimeout)
	defer cancel()

	err := s.client.XAdd(writeCtx, &redis.XAddArgs{
		Stream: s.key(job.shard),
		MaxLen: s.opts.MaxLen,
		Approx: true,
		Values: map[string]any{nodeStreamPayloadField: job.payload},
	}).Err()
	if err != nil {
		schedulerNodeStreamDroppedTotal.WithLabelValues("publish_error").Inc()
		s.opts.Logger.Debug("scheduler node stream publish failed", zap.Error(err))
		return
	}
	schedulerNodeStreamPublishedTotal.Inc()
}

func (s *RedisNodeStream) Subscribe(ctx context.Context, fn func(*schedulerv1.NodeStateEvent)) (<-chan struct{}, error) {
	ready := make(chan struct{})
	warm := make(chan struct{}, nodeStreamShards)

	for shard := uint32(0); shard < nodeStreamShards; shard++ {
		go s.readShard(ctx, shard, fn, warm)
	}

	go func() {
		defer close(ready)
		for i := 0; i < nodeStreamShards; i++ {
			select {
			case <-ctx.Done():
				return
			case <-warm:
			}
		}
	}()
	return ready, nil
}

// readShard replays what the shard still holds, then follows it.
func (s *RedisNodeStream) readShard(ctx context.Context, shard uint32, fn func(*schedulerv1.NodeStateEvent), warm chan<- struct{}) {
	lastID := s.warmUpShard(ctx, shard, fn)
	select {
	case warm <- struct{}{}:
	case <-ctx.Done():
		return
	}

	key := s.key(shard)
	for {
		if ctx.Err() != nil {
			return
		}
		// The read blocks server-side, so the client deadline has to outlive
		// the block or every read would end in a timeout.
		readCtx, cancel := context.WithTimeout(ctx, nodeStreamReadBlock+s.opts.OperationTimeout)
		streams, err := s.client.XRead(readCtx, &redis.XReadArgs{
			Streams: []string{key, lastID},
			Block:   nodeStreamReadBlock,
		}).Result()
		cancel()

		if err != nil {
			// An empty block is the common case, not a failure.
			if errors.Is(err, redis.Nil) || ctx.Err() != nil {
				continue
			}
			schedulerNodeStreamDroppedTotal.WithLabelValues("read_error").Inc()
			s.opts.Logger.Debug("scheduler node stream read failed",
				zap.String("stream", key),
				zap.Error(err),
			)
			select {
			case <-ctx.Done():
				return
			case <-time.After(nodeStreamReadErrorBackoff):
			}
			continue
		}

		for _, stream := range streams {
			for _, message := range stream.Messages {
				lastID = message.ID
				deliver(message, fn)
			}
		}
	}
}

// warmUpShard replays the entries the shard retains that are new enough to
// still describe a live node, and reports the id to continue from.
//
// The id it returns is never "$". A reader that started at the live tail would
// miss anything written between the replay and its first blocking read, and
// that node would then be invisible to this replica until its next heartbeat.
// Continuing from the horizon instead makes the two reads overlap rather than
// abut, and the replay stays bounded by the same window.
func (s *RedisNodeStream) warmUpShard(ctx context.Context, shard uint32, fn func(*schedulerv1.NodeStateEvent)) string {
	key := s.key(shard)
	horizon := time.Now().Add(-2 * s.opts.ReportTTL).UnixMilli()
	if horizon < 0 {
		horizon = 0
	}
	// Stream ids are `<unix-ms>-<seq>`, so the horizon is expressible as an id
	// without looking anything up.
	horizonID := fmt.Sprintf("%d-0", horizon)

	readCtx, cancel := context.WithTimeout(ctx, s.opts.OperationTimeout)
	defer cancel()
	messages, err := s.client.XRangeN(readCtx, key, horizonID, "+", int64(s.opts.MaxLen)).Result()
	if err != nil {
		s.opts.Logger.Warn("scheduler node stream warm-up failed; following from the horizon",
			zap.String("stream", key),
			zap.Error(err),
		)
		schedulerNodeStreamWarmupIncomplete.Set(1)
		return horizonID
	}
	if len(messages) == 0 {
		return horizonID
	}

	// `messages[0]` is the first entry at or after the horizon. If the stream's
	// true oldest entry is that same one, then nothing older than the horizon
	// survives -- the tail was trimmed inside the window this replica needed,
	// and some live node may be missing from its view until the next heartbeat.
	//
	// The comparison is `==`, not `!=`: a stream that still holds entries from
	// before the horizon is the healthy case, and testing for difference
	// reported every healthy replica as incomplete while staying silent on the
	// trimmed one this exists to catch.
	if oldest, err := s.client.XRangeN(readCtx, key, "-", "+", 1).Result(); err == nil && len(oldest) > 0 {
		if oldest[0].ID == messages[0].ID {
			schedulerNodeStreamWarmupIncomplete.Set(1)
			s.opts.Logger.Warn("scheduler node stream retention is shorter than the report TTL; raise scheduler.node_stream_maxlen",
				zap.String("stream", key),
			)
		}
	}

	lastID := horizonID
	for _, message := range messages {
		lastID = message.ID
		deliver(message, fn)
	}
	return lastID
}

// MarkNodeStreamWarmupIncomplete records that this replica began serving over a
// registry it could not fully replay.
func MarkNodeStreamWarmupIncomplete() {
	schedulerNodeStreamWarmupIncomplete.Set(1)
}

func deliver(message redis.XMessage, fn func(*schedulerv1.NodeStateEvent)) {
	raw, ok := message.Values[nodeStreamPayloadField]
	if !ok {
		schedulerNodeStreamAppliedTotal.WithLabelValues("invalid").Inc()
		return
	}
	text, ok := raw.(string)
	if !ok {
		schedulerNodeStreamAppliedTotal.WithLabelValues("invalid").Inc()
		return
	}
	event := &schedulerv1.NodeStateEvent{}
	if err := proto.Unmarshal([]byte(text), event); err != nil {
		schedulerNodeStreamAppliedTotal.WithLabelValues("invalid").Inc()
		return
	}
	fn(event)
}

func (s *RedisNodeStream) Close() error {
	select {
	case <-s.closed:
	default:
		close(s.closed)
	}
	return s.client.Close()
}
