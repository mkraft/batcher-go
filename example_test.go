package batchelor_test

import (
	"context"
	"fmt"
	"time"

	batchelor "github.com/mkraft/batchelorg"
)

func ExampleNewProxy() {
	firstMessageOnlyHandler := &batchelor.Handler{
		Wait: 3 * time.Second,
		Match: func(msg batchelor.Message) (string, bool) {
			if msg.Type() != "some-type" {
				return "", false
			}
			return "some-type-queue", true
		},
		Reduce: func(messages []batchelor.Message) batchelor.Message {
			return messages[0]
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{firstMessageOnlyHandler})

	proxy.In(&testMessage{id: "some-type", data: "data1"})
	proxy.In(&testMessage{id: "some-type", data: "data2"})

	cancel()

	fmt.Println(<-proxy.Out)

	// Output: &{some-type data1}
}
