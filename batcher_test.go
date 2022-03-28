package batcher_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/mkraft/batcher-go"
	"github.com/stretchr/testify/require"
)

type testMessage struct {
	id   string
	data interface{}
}

func (m *testMessage) String() string {
	return fmt.Sprintf("id: %s, data: %v", m.id, m.data)
}

func TestNotHandled(t *testing.T) {
	noOpMatcher := func(msg interface{}) (string, bool) { return "", false }

	testHandler := &batcher.Handler{
		Wait:  0,
		Match: noOpMatcher,
	}
	ctx, cancel := context.WithCancel(context.Background())
	btcr := batcher.NewBatcher(ctx, []*batcher.Handler{testHandler})

	wait := make(chan bool)
	actual := []interface{}{}

	go func() {
		for messages := range btcr.Out {
			actual = append(actual, messages...)
		}
		wait <- true
	}()

	testMessage1 := &testMessage{id: "foo", data: "test123"}
	testMessage2 := &testMessage{id: "bar", data: "test456"}

	btcr.In(testMessage1)
	btcr.In(testMessage2)

	cancel()

	<-wait

	require.Contains(t, actual, testMessage1)
	require.Contains(t, actual, testMessage2)
}

func TestHandled_ContextCancel(t *testing.T) {
	testHandler := &batcher.Handler{
		Wait:  time.Minute,
		Match: func(msg interface{}) (string, bool) { return "fooQueue", true },
	}
	ctx, cancel := context.WithCancel(context.Background())
	btcr := batcher.NewBatcher(ctx, []*batcher.Handler{testHandler})

	testMessage1 := &testMessage{id: "foo"}
	testMessage2 := &testMessage{id: "foo"}

	btcr.In(testMessage1)
	btcr.In(testMessage2)

	cancel()

	actual := <-btcr.Out

	require.Len(t, actual, 2)
}

func TestHandled_QueueTimeout(t *testing.T) {
	testWaitDur := time.Second
	testHandler := &batcher.Handler{
		Wait:  testWaitDur,
		Match: func(msg interface{}) (string, bool) { return "fooQueue", true },
	}
	ctx, cancel := context.WithCancel(context.Background())
	btcr := batcher.NewBatcher(ctx, []*batcher.Handler{testHandler})

	testMessage1 := &testMessage{id: "foo"}
	testMessage2 := &testMessage{id: "foo"}

	btcr.In(testMessage1)
	btcr.In(testMessage2)

	go func() {
		time.Sleep(2 * testWaitDur)
		cancel()
	}()

	output := <-btcr.Out

	actual := outputToTestMessages(output)

	require.Equal(t, actual[0].id, "foo")
	require.Equal(t, actual[1].id, "foo")
}

func outputToTestMessages(raw []interface{}) []*testMessage {
	var messages []*testMessage
	for _, item := range raw {
		message, ok := item.(*testMessage)
		if !ok {
			return nil
		}
		messages = append(messages, message)
	}
	return messages
}

func TestHandled_ContextCancel_MultipleQueues(t *testing.T) {
	fooHandler := &batcher.Handler{
		Wait: time.Minute,
		Match: func(raw interface{}) (string, bool) {
			msg, ok := raw.(*testMessage)
			require.True(t, ok)
			if msg.id != "foo" {
				return "", false
			}
			return "fooQueue", true
		},
	}
	barHandler := &batcher.Handler{
		Wait: time.Minute,
		Match: func(raw interface{}) (string, bool) {
			msg, ok := raw.(*testMessage)
			require.True(t, ok)
			if msg.id != "bar" {
				return "", false
			}
			return "barQueue", true
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	btcr := batcher.NewBatcher(ctx, []*batcher.Handler{fooHandler, barHandler})

	testMessage1 := &testMessage{id: "foo"}
	testMessage2 := &testMessage{id: "foo"}
	testMessage3 := &testMessage{id: "bar"}
	testMessage4 := &testMessage{id: "bar"}

	btcr.In(testMessage1)
	btcr.In(testMessage2)
	btcr.In(testMessage3)
	btcr.In(testMessage4)

	cancel()

	actual1 := outputToTestMessages(<-btcr.Out)
	actual2 := outputToTestMessages(<-btcr.Out)

	if actual1[0].id == "foo" {
		require.Equal(t, actual1[0].id, "foo")
		require.Equal(t, actual2[0].id, "bar")
	} else {
		require.Equal(t, actual1[0].id, "bar")
		require.Equal(t, actual2[0].id, "foo")
	}

	require.Len(t, actual1, 2)
	require.Len(t, actual2, 2)
}

func TestHandled_QueueTimeout_MultipleQueues(t *testing.T) {
	testWaitDur := time.Second
	fooHandler := &batcher.Handler{
		Wait: testWaitDur,
		Match: func(raw interface{}) (string, bool) {
			msg, ok := raw.(*testMessage)
			require.True(t, ok)
			if msg.id != "foo" {
				return "", false
			}
			return "fooQueue", true
		},
	}
	barHandler := &batcher.Handler{
		Wait: testWaitDur,
		Match: func(raw interface{}) (string, bool) {
			msg, ok := raw.(*testMessage)
			require.True(t, ok)
			if msg.id != "bar" {
				return "", false
			}
			return "barQueue", true
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	btcr := batcher.NewBatcher(ctx, []*batcher.Handler{fooHandler, barHandler})

	testMessage1 := &testMessage{id: "foo", data: "foodata1"}
	testMessage2 := &testMessage{id: "foo", data: "foodata2"}
	testMessage3 := &testMessage{id: "bar", data: "bardata1"}
	testMessage4 := &testMessage{id: "bar", data: "bardata2"}

	btcr.In(testMessage1)
	btcr.In(testMessage2)
	btcr.In(testMessage3)
	btcr.In(testMessage4)

	go func() {
		time.Sleep(2 * testWaitDur)
		cancel()
	}()

	actual1 := outputToTestMessages(<-btcr.Out)
	actual2 := outputToTestMessages(<-btcr.Out)

	if actual1[0].id == "foo" {
		require.Equal(t, actual1[0].id, "foo")
		require.Equal(t, actual2[0].id, "bar")
	} else {
		require.Equal(t, actual1[0].id, "bar")
		require.Equal(t, actual2[0].id, "foo")
	}

	require.Len(t, actual1, 2)
	require.Len(t, actual2, 2)
}

func BenchmarkQueue(b *testing.B) {
	testHandler := &batcher.Handler{
		Wait:  5 * time.Millisecond,
		Match: func(msg interface{}) (string, bool) { return "fooQueue", true },
	}

	btcr := batcher.NewBatcher(context.Background(), []*batcher.Handler{testHandler})

	go func() {
		for range btcr.Out {
		}
	}()

	for n := 0; n < b.N; n++ {
		btcr.In(&testMessage{id: "foo", data: fmt.Sprintf("data%d", n)})
	}
}
