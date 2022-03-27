.PHONY: test
test:
	go test . -v -race -count=1

.PHONY: benchmark
benchmark:
	go test -run=XXX -bench=. -v