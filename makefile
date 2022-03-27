.PHONY: test
test:
	go test . -v -race

.PHONY: benchmark
benchmark:
	go test -run=XXX -bench=. -v