package batchelor_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	batchelor "github.com/mkraft/batchelorgo/v2"
	"github.com/stretchr/testify/require"
)

type testMessage struct {
	id   string
	data interface{}
}

func (m *testMessage) Type() string {
	return m.id
}

func (m *testMessage) String() string {
	return fmt.Sprintf("id: %s, data: %v", m.id, m.data)
}

func TestNotHandled(t *testing.T) {
	noOpMatcher := func(msg batchelor.Message) (string, bool) { return "", false }

	testHandler := &batchelor.Handler{
		Wait:  0,
		Match: noOpMatcher,
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{testHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for messages := range proxy.Out {
			actual = append(actual, messages...)
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
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{testHandler})

	testMessage1 := &testMessage{id: "foo"}
	testMessage2 := &testMessage{id: "foo"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	cancel()

	actual := <-proxy.Out

	require.Len(t, actual, 2)
}

func TestHandled_QueueTimeout(t *testing.T) {
	testWaitDur := time.Second
	testHandler := &batchelor.Handler{
		Wait:  testWaitDur,
		Match: func(msg batchelor.Message) (string, bool) { return "fooQueue", true },
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{testHandler})

	testMessage1 := &testMessage{id: "foo"}
	testMessage2 := &testMessage{id: "foo"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)

	go func() {
		time.Sleep(2 * testWaitDur)
		cancel()
	}()

	actual := <-proxy.Out

	require.Equal(t, actual[0].Type(), "foo")
	require.Equal(t, actual[1].Type(), "foo")
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
	}
	barHandler := &batchelor.Handler{
		Wait: time.Minute,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "bar" {
				return "", false
			}
			return "barQueue", true
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{fooHandler, barHandler})

	testMessage1 := &testMessage{id: "foo"}
	testMessage2 := &testMessage{id: "foo"}
	testMessage3 := &testMessage{id: "bar"}
	testMessage4 := &testMessage{id: "bar"}

	proxy.In(testMessage1)
	proxy.In(testMessage2)
	proxy.In(testMessage3)
	proxy.In(testMessage4)

	cancel()

	actual1 := <-proxy.Out
	actual2 := <-proxy.Out

	if actual1[0].Type() == "foo" {
		require.Equal(t, actual1[0].Type(), "foo")
		require.Equal(t, actual2[0].Type(), "bar")
	} else {
		require.Equal(t, actual1[0].Type(), "bar")
		require.Equal(t, actual2[0].Type(), "foo")
	}

	require.Len(t, actual1, 2)
	require.Len(t, actual2, 2)
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
	}
	barHandler := &batchelor.Handler{
		Wait: testWaitDur,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "bar" {
				return "", false
			}
			return "barQueue", true
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{fooHandler, barHandler})

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

	actual1 := <-proxy.Out
	actual2 := <-proxy.Out

	if actual1[0].Type() == "foo" {
		require.Equal(t, actual1[0].Type(), "foo")
		require.Equal(t, actual2[0].Type(), "bar")
	} else {
		require.Equal(t, actual1[0].Type(), "bar")
		require.Equal(t, actual2[0].Type(), "foo")
	}

	require.Len(t, actual1, 2)
	require.Len(t, actual2, 2)
}

func BenchmarkQueue(b *testing.B) {
	testHandler := &batchelor.Handler{
		Wait:  5 * time.Millisecond,
		Match: func(msg batchelor.Message) (string, bool) { return "fooQueue", true },
	}

	proxy := batchelor.NewProxy(context.Background(), []*batchelor.Handler{testHandler})

	wait := make(chan bool)
	actual := []batchelor.Message{}

	go func() {
		for messages := range proxy.Out {
			actual = append(actual, messages...)
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
