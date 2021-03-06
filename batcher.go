package batcher

import (
	"context"
	"time"
)

// Handler configures a desired behavior for a queue category.
type Handler struct {
	// Wait is the total duration that a queue will continue enqueuing like messages after the first
	// one is received.
	Wait time.Duration

	// Match defined a function that is invoked upon each incoming message. If it returns true then the
	// given message will be enqueued in a new or existing queue of the returned name.
	Match func(interface{}) (string, bool)
}

type Batcher struct {
	in           chan interface{}
	Out          chan []interface{}
	ctx          context.Context
	handlers     []*Handler
	queueTimeout chan string
}

// In takes messages in from the stream.
func (p *Batcher) In(message interface{}) {
	p.in <- message
}

// NewBatcher is the factory.
func NewBatcher(ctx context.Context, handlers []*Handler) *Batcher {
	batcher := &Batcher{
		ctx:          ctx,
		handlers:     handlers,
		in:           make(chan interface{}),
		Out:          make(chan []interface{}),
		queueTimeout: make(chan string),
	}
	go batcher.listen()
	return batcher
}

func (p *Batcher) listen() {
	q := make(queue)

	for {
		select {
		case message := <-p.in:
			var handled bool

			for _, handler := range p.handlers {
				name, isMatch := handler.Match(message)
				if !isMatch {
					continue
				}

				handled = true

				hasExistingTimeout := q.enqueue(name, message)
				if hasExistingTimeout {
					continue
				}

				go p.runTimeout(handler.Wait, name)

				// break out of the handlers loop so that only one matched handler is ever triggered for a given message
				break
			}

			if !handled {
				// a message that doesn't match any handlers is sent out immediately
				p.Out <- []interface{}{message}
			}
		case name := <-p.queueTimeout:
			p.Out <- q[name]
			delete(q, name)
		case <-p.ctx.Done():
			// flush all queues manually if context is done
			for _, item := range q {
				p.Out <- item
			}
			close(p.Out)
			return
		}
	}
}

func (p *Batcher) runTimeout(dur time.Duration, name string) {
	select {
	case <-time.After(dur):
		p.queueTimeout <- name
		return
	case <-p.ctx.Done():
		// when the context is done return from the listen immediately
		// and let the ctx.Done select case in (*Batcher).listen send out
		// all of the messages in the queue, rather than using queueTimeout case.
		return
	}
}
