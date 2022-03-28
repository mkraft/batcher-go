package batcher

import (
	"context"
	"time"
)

type Message interface {
	ID() string
}

// Handler configures a desired behavior for a category or type of messages.
type Handler struct {
	// Wait is the total duration that a queue will continue enqueuing like messages after the first
	// one is received.
	Wait time.Duration

	// Match defined a function that is invoked upon each incoming message. If it returns true then the
	// given message will be enqueued in a new or existing queue of the returned name.
	Match func(Message) (string, bool)
}

type Batcher struct {
	in           chan Message
	Out          chan []Message
	ctx          context.Context
	handlers     []*Handler
	queueTimeout chan string
}

func (p *Batcher) In(message Message) {
	p.in <- message
}

type queue map[string][]Message

func (q queue) enqueue(name string, message Message) (appended bool) {
	var newMessages []Message
	forName, has := q[name]
	if has {
		newMessages = append(forName, message)
	} else {
		newMessages = []Message{message}
	}
	q[name] = newMessages
	return has
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
				p.Out <- []Message{message}
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
		// and let the ctx.Done select case in (*Proxy).listen send out
		// all of the messages in the queue, rather than using queueTimeout case.
		return
	}
}

// NewBatcher is the factory.
func NewBatcher(ctx context.Context, handlers []*Handler) *Batcher {
	proxy := &Batcher{
		ctx:          ctx,
		handlers:     handlers,
		in:           make(chan Message),
		Out:          make(chan []Message),
		queueTimeout: make(chan string),
	}
	go proxy.listen()
	return proxy
}
