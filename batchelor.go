package batchelor

import (
	"context"
	"sync"
	"time"
)

type Message interface {
	Type() string
	Data() interface{}
}

// Handler configures a desired behavior for a category or type of messages.
type Handler struct {
	// Wait is the total duration that a queue will continue enqueuing like messages after the first
	// one is received.
	Wait time.Duration

	// Match defined a function that is invoked upon each incoming message. If it returns true then the
	// given message will be enqueued in a new or existing queue of the returned name.
	Match func(Message) (string, bool)

	// Reduce defines the function that is invoked on a list of outgoing messages from a given queue.
	Reduce ReducerFunc
}

// ReducerFunc defines the signature of a Reduce function that is invoked on the list of outgoing
// messages.
type ReducerFunc func(messages []Message) Message

type Proxy struct {
	in           chan Message
	Out          chan Message
	ctx          context.Context
	handlers     []*Handler
	queueTimeout chan *timeoutInfo
}

func (p *Proxy) In(message Message) {
	p.in <- message
}

type queueItem struct {
	reducer  ReducerFunc
	messages []Message
}

type queue map[string]*queueItem

func (q queue) enqueue(name string, message Message, reduer ReducerFunc) (appended bool) {
	var newMessages []Message
	forName, has := q[name]
	if has {
		newMessages = append(forName.messages, message)
	} else {
		newMessages = []Message{message}
	}
	q[name] = &queueItem{messages: newMessages, reducer: reduer}
	return has
}

type timeoutInfo struct {
	name    string
	reducer ReducerFunc
}

func (p *Proxy) listen() {
	q := make(queue)
	var wg sync.WaitGroup

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

				hasExistingTimeout := q.enqueue(name, message, handler.Reduce)
				if hasExistingTimeout {
					continue
				}

				wg.Add(1)
				go p.runTimeout(handler.Wait, &timeoutInfo{name: name, reducer: handler.Reduce})

				// break out of the handlers loop so that only one matched handler is ever triggered for a given message
				break
			}

			if !handled {
				// a message that doesn't match any handlers is sent out immediately
				p.Out <- message
			}
		case info := <-p.queueTimeout:
			p.Out <- info.reducer(q[info.name].messages)
			delete(q, info.name)
			wg.Done()
		case <-p.ctx.Done():
			// flush all queues manually if context is done
			for _, item := range q {
				p.Out <- item.reducer(item.messages)
				wg.Done()
			}
			wg.Wait()
			close(p.Out)
			return
		}
	}
}

func (p *Proxy) runTimeout(dur time.Duration, info *timeoutInfo) {
	select {
	case <-time.After(dur):
		p.queueTimeout <- info
		return
	case <-p.ctx.Done():
		// when the context is done return from the listen immediately
		// and let the ctx.Done select case in (*Proxy).listen send out
		// all of the messages in the queue, rather than using queueTimeout case.
		return
	}
}

// NewProxy is the factory.
func NewProxy(ctx context.Context, handlers []*Handler) *Proxy {
	proxy := &Proxy{
		ctx:          ctx,
		handlers:     handlers,
		in:           make(chan Message),
		Out:          make(chan Message),
		queueTimeout: make(chan *timeoutInfo),
	}
	go proxy.listen()
	return proxy
}
