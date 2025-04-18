// Copyright (c) 2023 BVK Chaitanya

// Package topic implements a generic, buffering publish-subscribe messaging
// system with dynamic fanout.
//
// Messages sent to a Topic are duplicated and delivered to all subscribed
// receivers. Incoming messages are queued in-memory when receivers are not
// ready, with configurable buffering behavior per receiver. Receivers can be
// added or removed dynamically, and the most recent message can be queried.
//
// The Topic type is safe for concurrent use by multiple goroutines.
package topic

import (
	"context"
	"os"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
)

// Topic represents a publish-subscribe channel that duplicates messages to all
// subscribed receivers. Messages are queued in-memory for slow receivers, and
// the buffering behavior is configured per receiver via Subscribe. Topics are
// created with New and support generic message types.
type Topic[T any] struct {
	// closeCtx and closeCause are used to cancel background tasks when topic is
	// closed.
	closeCtx   context.Context
	closeCause context.CancelCauseFunc

	// wg is used to block for internal goroutine cleanup.
	wg sync.WaitGroup

	// sendCh receives incoming messages for the topic.
	sendCh chan T

	// subscribeCh receives a message when the topic has a new receiver.
	subscribeCh chan *Receiver[T]

	// unsubscribeCh receives a message when a topic receiver is unsubscribed.
	unsubscribeCh chan *Receiver[T]

	// receivers is the list of all receivers for the topic.
	receivers []*Receiver[T]

	// recentValue holds the latest value sent to the topic.
	recentValue atomic.Value
}

// Receiver represents a subscriber to a Topic. It provides methods to manage
// subscription lifecycle, such as unsubscribing from the Topic.
type Receiver[T any] struct {
	topic *Topic[T]

	// ok channel signals completion of a subscribe/unsubscribe operation.
	ok chan struct{}

	// limit indicates maximum number of messages to buffer in the queue. A zero
	// limit means queue is unbounded; with a +ve limit N, queue holds the newest
	// N values and with a -ve limit N, queue holds the oldest N values.
	limit int

	// includeRecent flag when true begins the receiver with the most recent
	// message before the receiver is created.
	includeRecent bool

	// relayCh is the channel where receiver waits to receive messages.
	relayCh chan T

	// queue holds zero or more incoming messages not yet received by this
	// receiver.
	queue []reflect.Value
}

// New creates a new Topic for messages of type T. The returned Topic is ready
// to accept messages via Send/SendCh and subscribers via Subscribe.
//
// Example:
//
//	topic := topic.New[string]()
//	ch := topic.SendCh()
//	ch <- "Hello, world!"
func New[T any]() *Topic[T] {
	ctx, cause := context.WithCancelCause(context.Background())
	t := &Topic[T]{
		closeCtx:      ctx,
		closeCause:    cause,
		sendCh:        make(chan T),
		subscribeCh:   make(chan *Receiver[T]),
		unsubscribeCh: make(chan *Receiver[T]),
	}
	t.wg.Add(1)
	go t.goDispatch()
	return t
}

// Close shuts down the Topic, closing all subscriber channels and preventing
// further subscriptions or message sends. After closing, SendCh will panic,
// Subscribe will return an error, and Recent may return false. Close is
// idempotent; multiple calls have no additional effect.
//
// Example:
//
//	topic := topic.New[float64]()
//	err := topic.Close()
//	if err != nil { /* handle error */ }
func (t *Topic[T]) Close() error {
	t.closeCause(os.ErrClosed)
	t.wg.Wait()
	return nil
}

func (t *Topic[T]) goDispatch() {
	defer t.wg.Done()

	nreceivers := len(t.receivers)
	pending := make([]reflect.SelectCase, 0, nreceivers+4)

	for {

		if n := len(t.receivers); n != nreceivers {
			nreceivers = n
			pending = make([]reflect.SelectCase, 0, nreceivers+4)
		} else {
			pending = pending[:0]
		}

		closeCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(t.closeCtx.Done()),
		}
		pending = append(pending, closeCase)

		sendCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(t.sendCh),
		}
		pending = append(pending, sendCase)

		subscribeCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(t.subscribeCh),
		}
		pending = append(pending, subscribeCase)

		unsubscribeCase := reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(t.unsubscribeCh),
		}
		pending = append(pending, unsubscribeCase)

		// Each receiver has one channel starting at offset 4.
		for _, r := range t.receivers {
			relayCase := reflect.SelectCase{
				Dir: reflect.SelectSend,
			}
			if len(r.queue) > 0 {
				relayCase.Chan = reflect.ValueOf(r.relayCh)
				relayCase.Send = r.queue[0]
			}
			pending = append(pending, relayCase)
		}

		chosen, recv, recvOK := reflect.Select(pending)
		switch chosen {
		case 0: // <-t.closeCtx
			{
				for _, r := range t.receivers {
					close(r.relayCh)
				}
				t.receivers = nil
				return
			}

		case 1: // <-t.sendCh
			if recvOK {
				v := recv.Interface().(T)
				for _, r := range t.receivers {
					r.add(recv)
				}
				t.recentValue.Store(v)
			}

		case 2: // <-t.subscribeCh
			if recvOK {
				r := recv.Interface().(*Receiver[T])
				chsize := 0
				r.topic = t
				r.relayCh = make(chan T, chsize)
				t.receivers = append(t.receivers, r)
				if v, ok := Recent(t); ok && r.includeRecent {
					r.add(reflect.ValueOf(v))
				}
				r.ok <- struct{}{}
			}

		case 3: // <-t.unsubscribeCh
			if recvOK {
				r := recv.Interface().(*Receiver[T])
				if i := slices.Index(t.receivers, r); i >= 0 {
					t.receivers = slices.Delete(t.receivers, i, i+1)
					r.queue = nil
					close(r.relayCh)
					r.ok <- struct{}{}
				}
			}

		default:
			r := t.receivers[chosen-4]
			r.remove()
		}
	}
}

// Send publishes a message directly to the Topic. The message is duplicated
// and delivered to all subscribed receivers. If the Topic is closed, Send
// returns false.
//
// Example:
//
//	topic := topic.New[int]()
//	if closed := topic.Send(42); closed {
//		/* handle error */
//	}
func (t *Topic[T]) Send(v T) bool {
	select {
	case <-t.closeCtx.Done():
		return false
	case t.sendCh <- v:
		return true
	}
}

// SendCh returns a send-only channel for publishing messages to the Topic.
// Messages sent to this channel are duplicated and delivered to all subscribed
// receivers. If the Topic is closed, sending to the channel will panic.
//
// Example:
//
//	topic := topic.New[int]()
//	ch := topic.SendCh()
//	ch <- 42
func (t *Topic[T]) SendCh() chan<- T {
	select {
	case <-t.closeCtx.Done():
		return nil
	default:
		return t.sendCh
	}
}

// Subscribe adds a new receiver to the Topic, returning a Receiver and a
// receive-only channel for consuming messages. The limit parameter controls
// the receiver's queue behavior:
//
//   - limit == 0: Unbounded queue, buffering all messages (memory-limited).
//   - limit > 0: Buffers the most recent limit messages, discarding older ones if full.
//   - limit < 0: Buffers the oldest |limit| messages, discarding newer ones if full.
//
// If the Topic is closed, Subscribe returns an error. The returned channel is
// closed when the receiver unsubscribes or the Topic is closed.
//
// Example:
//
//	topic := topic.New[string]()
//	receiver, ch, err := topic.Subscribe(5, false /* includeRecent */) // Buffer up to 5 recent messages
//	if err != nil { /* handle error */ }
//	for msg := range ch {
//	    fmt.Println(msg)
//	}
func (t *Topic[T]) Subscribe(limit int, includeRecent bool) (*Receiver[T], <-chan T, error) {
	r := &Receiver[T]{
		ok:            make(chan struct{}),
		limit:         limit,
		includeRecent: includeRecent,
	}

	select {
	case <-t.closeCtx.Done():
		return nil, nil, context.Cause(t.closeCtx)
	case t.subscribeCh <- r:
		<-r.ok
		return r, r.relayCh, nil
	}
}

// Unsubscribe removes the receiver from the Topic, closing its associated
// receive-only channel. Pending messages in the receiver's queue are
// discarded.  Unsubscribe is idempotent; multiple calls have no effect. After
// unsubscribing, the receiver cannot be reused.
//
// Example:
//
//	topic := topic.New[bool]()
//	receiver, _, _ := topic.Subscribe(0)
//	receiver.Unsubscribe()
func (r *Receiver[T]) Unsubscribe() {
	if r.topic == nil {
		return
	}

	select {
	case <-r.topic.closeCtx.Done():
		return

	case r.topic.unsubscribeCh <- r:
		<-r.ok
		r.topic = nil
	}
}

func (r *Receiver[T]) add(v reflect.Value) {
	if len(r.queue) == 0 {
		if reflect.ValueOf(r.relayCh).TrySend(v) {
			return
		}
	}

	if r.limit == 0 {
		r.queue = append(r.queue, v)
		return
	}

	if r.limit > 0 {
		if len(r.queue) < r.limit {
			r.queue = append(r.queue, v)
			return
		}
		// limit must be enforced; drop from the front of the queue.
		_ = slices.Delete(r.queue, 0, 1)
		r.queue[r.limit-1] = v
	}

	if r.limit < 0 {
		if len(r.queue) < -r.limit {
			r.queue = append(r.queue, v)
			return
		}
		// queue already holds oldest values, so new value is ignored.
	}
}

func (r *Receiver[T]) remove() reflect.Value {
	v := r.queue[0]
	r.queue = slices.Delete(r.queue, 0, 1)
	return v
}
