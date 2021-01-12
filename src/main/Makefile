.PHONY: clean master worker

clean:
	rm -f mr-*-*
	go build -buildmode=plugin ../mrapps/wc.go

master:
	go run mrmaster.go pg-*.txt

worker:
	go run mrworker.go wc.so
