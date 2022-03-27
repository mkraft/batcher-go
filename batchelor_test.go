package batchelor_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	batchelor "github.com/mkraft/batchelorgo"
	"github.com/stretchr/testify/require"
)

type testMessage struct {
	id   string
	data interface{}
}

func (m *testMessage) Type() string {
	return m.id
}

func (m *testMessage) Data() interface{} {
	return m.data
}

func TestNotHandled(t *testing.T) {
	noOpMatcher := func(msg batchelor.Message) (string, bool) { return "", false }

	testHandler := &batchelor.Handler{
		Wait:   0,
		Match:  noOpMatcher,
		Reduce: func(messages []batchelor.Message) batchelor.Message { return messages[0] },
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{testHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &testMessage{id: "foo", data: "test123"}
	testMessage2 := &testMessage{id: "bar", data: "test456"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	cancel()

	<-wait

	require.Contains(t, actual, testMessage1)
	require.Contains(t, actual, testMessage2)
}

func TestHandled_ContextCancel(t *testing.T) {
	testHandler := &batchelor.Handler{
		Wait:  time.Minute,
		Match: func(msg batchelor.Message) (string, bool) { return "fooQueue", true },
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data(), messages[1].Data())
			return &testMessage{id: "reducedFoos", data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{testHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &testMessage{id: "foo", data: "test123"}
	testMessage2 := &testMessage{id: "foo", data: "test456"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	cancel()

	<-wait

	require.Equal(t, actual[0].Type(), "reducedFoos")
	require.Equal(t, actual[0].Data(), "test123:test456")
}

func TestHandled_QueueTimeout(t *testing.T) {
	testWaitDur := time.Second
	testHandler := &batchelor.Handler{
		Wait:  testWaitDur,
		Match: func(msg batchelor.Message) (string, bool) { return "fooQueue", true },
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data(), messages[1].Data())
			return &testMessage{id: "reducedFoos", data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{testHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &testMessage{id: "foo", data: "test123"}
	testMessage2 := &testMessage{id: "foo", data: "test456"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	go func() {
		time.Sleep(2 * testWaitDur)
		cancel()
	}()

	<-wait

	require.Equal(t, actual[0].Type(), "reducedFoos")
	require.Equal(t, actual[0].Data(), "test123:test456")
}

func TestHandled_ContextCancel_MultipleQueues(t *testing.T) {
	fooHandler := &batchelor.Handler{
		Wait: time.Minute,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "foo" {
				return "", false
			}
			return "fooQueue", true
		},
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data(), messages[1].Data())
			return &testMessage{id: "reducedFoos", data: combinedData}
		},
	}
	barHandler := &batchelor.Handler{
		Wait: time.Minute,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "bar" {
				return "", false
			}
			return "barQueue", true
		},
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data(), messages[1].Data())
			return &testMessage{id: "reducedBars", data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{fooHandler, barHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &testMessage{id: "foo", data: "foodata1"}
	testMessage2 := &testMessage{id: "foo", data: "foodata2"}
	testMessage3 := &testMessage{id: "bar", data: "bardata1"}
	testMessage4 := &testMessage{id: "bar", data: "bardata2"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)
	proxy.In(testMessage3)
	proxy.In(testMessage4)

	cancel()

	<-wait

	expect1 := &testMessage{id: "reducedFoos", data: "foodata1:foodata2"}
	expect2 := &testMessage{id: "reducedBars", data: "bardata1:bardata2"}

	require.Contains(t, actual, expect1)
	require.Contains(t, actual, expect2)
}

func TestHandled_QueueTimeout_MultipleQueues(t *testing.T) {
	testWaitDur := time.Second
	fooHandler := &batchelor.Handler{
		Wait: testWaitDur,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "foo" {
				return "", false
			}
			return "fooQueue", true
		},
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data(), messages[1].Data())
			return &testMessage{id: "reducedFoos", data: combinedData}
		},
	}
	barHandler := &batchelor.Handler{
		Wait: testWaitDur,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "bar" {
				return "", false
			}
			return "barQueue", true
		},
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			combinedData := fmt.Sprintf("%v:%v", messages[0].Data(), messages[1].Data())
			return &testMessage{id: "reducedBars", data: combinedData}
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{fooHandler, barHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	testMessage1 := &testMessage{id: "foo", data: "foodata1"}
	testMessage2 := &testMessage{id: "foo", data: "foodata2"}
	testMessage3 := &testMessage{id: "bar", data: "bardata1"}
	testMessage4 := &testMessage{id: "bar", data: "bardata2"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)
	proxy.In(testMessage3)
	proxy.In(testMessage4)

	go func() {
		time.Sleep(2 * testWaitDur)
		cancel()
	}()

	<-wait

	expect1 := &testMessage{id: "reducedFoos", data: "foodata1:foodata2"}
	expect2 := &testMessage{id: "reducedBars", data: "bardata1:bardata2"}

	require.Contains(t, actual, expect1)
	require.Contains(t, actual, expect2)
}

func BenchmarkQueue(b *testing.B) {
	testHandler := &batchelor.Handler{
		Wait:  5 * time.Millisecond,
		Match: func(msg batchelor.Message) (string, bool) { return "fooQueue", true },
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			return messages[0]
		},
	}

	proxy := batchelor.NewProxy(context.Background(), []*batchelor.Handler{testHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for message := range proxy.Out {
			actual = append(actual, message)
		}
		wait <- true
	}()

	go func() {
		for range proxy.Out {
		}
	}()

	for n := 0; n < b.N; n++ {
		proxy.In(&testMessage{id: "foo", data: fmt.Sprintf("data%d", n)})
	}
}
