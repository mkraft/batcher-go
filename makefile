.PHONY: test
test:
	go test . -v -race -count=1 -timeout 10s

.PHONY: benchmark
benchmark:
	go test -run=XXX -bench=. -v