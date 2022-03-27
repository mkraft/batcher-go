package batchelor

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNotHandled(t *testing.T) {
	noOpMatcher := func(evt *Message) (string, bool) { return "", false }

	testHandler := &Handler{
		Wait:   0,
		Match:  noOpMatcher,
		Reduce: func(messages []*Message) *Message { return messages[0] },
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := NewProxy(ctx, []*Handler{testHandler})

	wait := make(chan bool)
	actual := []*Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &Message{Type: "foo", Data: "test123"}
	testMessage2 := &Message{Type: "bar", Data: "test456"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	cancel()

	<-wait

	require.Contains(t, actual, testMessage1)
	require.Contains(t, actual, testMessage2)
}

func TestHandled_ContextCancel(t *testing.T) {
	testHandler := &Handler{
		Wait:  time.Minute,
		Match: func(evt *Message) (string, bool) { return "fooQueue", true },
		Reduce: func(messages []*Message) *Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data, messages[1].Data)
			return &Message{Type: "reducedFoos", Data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := NewProxy(ctx, []*Handler{testHandler})

	wait := make(chan bool)
	actual := []*Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &Message{Type: "foo", Data: "test123"}
	testMessage2 := &Message{Type: "foo", Data: "test456"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	cancel()

	<-wait

	require.Equal(t, actual[0].Type, "reducedFoos")
	require.Equal(t, actual[0].Data, "test123:test456")
}

func TestHandled_QueueTimeout(t *testing.T) {
	testWaitDur := time.Second
	testHandler := &Handler{
		Wait:  testWaitDur,
		Match: func(evt *Message) (string, bool) { return "fooQueue", true },
		Reduce: func(messages []*Message) *Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data, messages[1].Data)
			return &Message{Type: "reducedFoos", Data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := NewProxy(ctx, []*Handler{testHandler})

	wait := make(chan bool)
	actual := []*Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &Message{Type: "foo", Data: "test123"}
	testMessage2 := &Message{Type: "foo", Data: "test456"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	go func() {
		time.Sleep(2 * testWaitDur)
		cancel()
	}()

	<-wait

	require.Equal(t, actual[0].Type, "reducedFoos")
	require.Equal(t, actual[0].Data, "test123:test456")
}

func TestHandled_ContextCancel_MultipleQueues(t *testing.T) {
	fooHandler := &Handler{
		Wait: time.Minute,
		Match: func(evt *Message) (string, bool) {
			if evt.Type != "foo" {
				return "", false
			}
			return "fooQueue", true
		},
		Reduce: func(messages []*Message) *Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data, messages[1].Data)
			return &Message{Type: "reducedFoos", Data: combinedData}
		},
	}
	barHandler := &Handler{
		Wait: time.Minute,
		Match: func(evt *Message) (string, bool) {
			if evt.Type != "bar" {
				return "", false
			}
			return "barQueue", true
		},
		Reduce: func(messages []*Message) *Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data, messages[1].Data)
			return &Message{Type: "reducedBars", Data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := NewProxy(ctx, []*Handler{fooHandler, barHandler})

	wait := make(chan bool)
	actual := []*Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &Message{Type: "foo", Data: "foodata1"}
	testMessage2 := &Message{Type: "foo", Data: "foodata2"}
	testMessage3 := &Message{Type: "bar", Data: "bardata1"}
	testMessage4 := &Message{Type: "bar", Data: "bardata2"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)
	proxy.In(testMessage3)
	proxy.In(testMessage4)

	cancel()

	<-wait

	expect1 := &Message{Type: "reducedFoos", Data: "foodata1:foodata2"}
	expect2 := &Message{Type: "reducedBars", Data: "bardata1:bardata2"}

	require.Contains(t, actual, expect1)
	require.Contains(t, actual, expect2)
}

func TestHandled_QueueTimeout_MultipleQueues(t *testing.T) {
	testWaitDur := time.Second
	fooHandler := &Handler{
		Wait: testWaitDur,
		Match: func(evt *Message) (string, bool) {
			if evt.Type != "foo" {
				return "", false
			}
			return "fooQueue", true
		},
		Reduce: func(messages []*Message) *Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data, messages[1].Data)
			return &Message{Type: "reducedFoos", Data: combinedData}
		},
	}
	barHandler := &Handler{
		Wait: testWaitDur,
		Match: func(evt *Message) (string, bool) {
			if evt.Type != "bar" {
				return "", false
			}
			return "barQueue", true
		},
		Reduce: func(messages []*Message) *Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data, messages[1].Data)
			return &Message{Type: "reducedBars", Data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := NewProxy(ctx, []*Handler{fooHandler, barHandler})

	wait := make(chan bool)
	actual := []*Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &Message{Type: "foo", Data: "foodata1"}
	testMessage2 := &Message{Type: "foo", Data: "foodata2"}
	testMessage3 := &Message{Type: "bar", Data: "bardata1"}
	testMessage4 := &Message{Type: "bar", Data: "bardata2"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)
	proxy.In(testMessage3)
	proxy.In(testMessage4)

	go func() {
		time.Sleep(2 * testWaitDur)
		cancel()
	}()

	<-wait

	expect1 := &Message{Type: "reducedFoos", Data: "foodata1:foodata2"}
	expect2 := &Message{Type: "reducedBars", Data: "bardata1:bardata2"}

	require.Contains(t, actual, expect1)
	require.Contains(t, actual, expect2)
}
