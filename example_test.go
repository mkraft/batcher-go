package batchelor_test

import (
	"context"
	"fmt"
	"strings"
	"time"

	batchelor "github.com/mkraft/batchelorg"
)

func ExampleNewProxy() {
	myHandler := &batchelor.Handler{
		Wait: 3 * time.Second,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "some-type" {
				return "", false
			}
			return "some-type-queue", true
		},
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			datas := []string{}
			for _, msg := range messages {
				datas = append(datas, msg.Data().(string))
			}
			return &testMessage{id: "some-type-combined", data: strings.Join(datas, ":")}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{myHandler})

	proxy.In(&testMessage{id: "some-type", data: "data1"})
	proxy.In(&testMessage{id: "some-type", data: "data2"})

	cancel()

	fmt.Printf("%+v", <-proxy.Out)

	// Output: &{id:some-type-combined data:data1:data2}
}

func ExampleReducerFunc() {
	var keepFirst batchelor.ReducerFunc
	keepFirst = func(messages []batchelor.Message) batchelor.Message {
		return messages[0]
	}
	queueMessages := []batchelor.Message{
		&testMessage{id: "test", data: "data1"},
		&testMessage{id: "test", data: "data2"},
	}
	fmt.Println(keepFirst(queueMessages))
	// Output: &{test data1}
}
