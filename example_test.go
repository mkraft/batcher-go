package batchelor_test

import (
	"context"
	"fmt"
	"strings"
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
			datas := []string{}
			for _, msg := range messages {
				datas = append(datas, msg.Data().(string))
			}
			return &testMessage{id: "some-type-combined", data: strings.Join(datas, ":")}
		},
	}

	ctx, cancel := context.WithCancel(context.Background())

	proxy := batchelor.NewProxy(ctx, []*batchelor.Handler{firstMessageOnlyHandler})

	proxy.In(&testMessage{id: "some-type", data: "data1"})
	proxy.In(&testMessage{id: "some-type", data: "data2"})

	cancel()

	fmt.Printf("%+v", <-proxy.Out)

	// Output: &{id:some-type-combined data:data1:data2}
}
