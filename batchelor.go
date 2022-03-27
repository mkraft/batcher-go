package batchelor

import (
	"context"
	"sync"
	"time"
)

type Message struct {
	Type string      `json:"type"`
	Data interface{} `json:"data"`
}

type Handler struct {
	Wait   time.Duration
	Match  func(*Message) (string, bool)
	Reduce reducerFunc
}

type reducerFunc func(messages []*Message) *Message

type Proxy struct {
	in           chan *Message
	Out          chan *Message
	ctx          context.Context
	handlers     []*Handler
	queueTimeout chan *timeoutInfo
}

func (p *Proxy) In(message *Message) {
	p.in <- message
}

type queueItem struct {
	reducer  reducerFunc
	messages []*Message
}

type queue map[string]*queueItem

func (q queue) enqueue(name string, message *Message, reduer reducerFunc) (appended bool) {
	var newMessages []*Message
	forName, has := q[name]
	if has {
		newMessages = append(forName.messages, message)
	} else {
		newMessages = []*Message{message}
	}
	q[name] = &queueItem{messages: newMessages, reducer: reduer}
	return has
}

type timeoutInfo struct {
	name    string
	reducer reducerFunc
}

func (p *Proxy) listen() {
	q := make(queue)
	var wg sync.WaitGroup

	for {
		select {
		case <-p.ctx.Done():
			// flush all queues manually
			for _, item := range q {
				p.Out <- item.reducer(item.messages)
				wg.Done()
			}
			wg.Wait()
			close(p.Out)
			return
		case message := <-p.in:
			var wasHandled bool
			for _, handler := range p.handlers {
				if name, isMatch := handler.Match(message); isMatch {
					wasHandled = true
					hasTimeout := q.enqueue(name, message, handler.Reduce)
					if !hasTimeout {
						wg.Add(1)
						go p.runTimeout(handler.Wait, &timeoutInfo{name: name, reducer: handler.Reduce})
					}
					break
				}
			}

			if !wasHandled {
				p.Out <- message
			}
		case info := <-p.queueTimeout:
			p.Out <- info.reducer(q[info.name].messages)
			delete(q, info.name)
			wg.Done()
		}
	}
}

func (p *Proxy) runTimeout(dur time.Duration, info *timeoutInfo) {
	select {
	case <-time.After(dur):
		p.queueTimeout <- info
		return
	case <-p.ctx.Done():
		return
	}
}

func NewProxy(ctx context.Context, handlers []*Handler) *Proxy {
	proxy := &Proxy{
		ctx:          ctx,
		handlers:     handlers,
		in:           make(chan *Message),
		Out:          make(chan *Message),
		queueTimeout: make(chan *timeoutInfo),
	}
	go proxy.listen()
	return proxy
}
