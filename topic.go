// Copyright (c) 2023 BVK Chaitanya

// Package topic implements a buffered channel with dynamic fanout size. Unlike
// the normal Go channels, messages sent to a Topic are duplicated to all
// receivers. Incoming messages are queued in-memory when a receiver is not
// ready. Users can add/remove receivers from a topic dynamically.
package topic

import (
	"context"
	"os"
	"reflect"
	"slices"
	"sync"
)

// Topic implements a buffered channel with dynamic fanout.
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
}

type Receiver[T any] struct {
	topic *Topic[T]

	// ok channel signals completion of a subscribe/unsubscribe operation.
	ok chan struct{}

	// limit indicates maximum number of messages to buffer in the queue. A zero
	// limit means queue is unbounded; with a +ve limit N, queue holds the newest
	// N values and with a -ve limit N, queue holds the oldest N values.
	limit int

	// relayCh is the channel where receiver waits to receive messages.
	relayCh chan T

	// queue holds zero or more incoming messages not yet received by this
	// receiver.
	queue []T
}

// New creates a new topic.
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

// Close destroys the topic. Blocking operations will return with os.ErrClosed
// error. All receivers are forcibly unsubscribed. Caller is blocked till all
// background goroutines complete.
func (t *Topic[T]) Close() error {
	t.closeCause(os.ErrClosed)
	t.wg.Wait()
	return nil
}

func (t *Topic[T]) goDispatch() {
	defer t.wg.Done()

	for {
		var pending []reflect.SelectCase

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
				relayCase.Send = reflect.ValueOf(r.queue[0])
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
					r.add(v)
				}
			}

		case 2: // <-t.subscribeCh
			if recvOK {
				r := recv.Interface().(*Receiver[T])
				r.topic = t
				r.relayCh = make(chan T)
				t.receivers = append(t.receivers, r)
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

// SendCh returns a channel for the Topic where users can send messages.
// topic in other select clauses. Returns nil if topic is closed.
func (t *Topic[T]) SendCh() chan<- T {
	select {
	case <-t.closeCtx.Done():
		return nil
	default:
		return t.sendCh
	}
}

// Subscribe adds a new receiver to the topic. Maximium number of messages
// bufferred in the queue is controlled by the limit. A zero limit indicates
// unbounded queue; with a positive limit N, queue buffers the most recent N
// messages and with a negative limit N queue buffers the oldest N messages.
func (t *Topic[T]) Subscribe(limit int) (*Receiver[T], <-chan T, error) {
	r := &Receiver[T]{
		ok:    make(chan struct{}),
		limit: limit,
	}

	select {
	case <-t.closeCtx.Done():
		return nil, nil, context.Cause(t.closeCtx)
	case t.subscribeCh <- r:
		<-r.ok
		return r, r.relayCh, nil
	}
}

// Unsubscribe removes a receiver from a topic. Buffered messages that were not
// yet received will be discarded.
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

func (r *Receiver[T]) add(v T) {
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

func (r *Receiver[T]) remove() T {
	v := r.queue[0]
	r.queue = slices.Delete(r.queue, 0, 1)
	return v
}
