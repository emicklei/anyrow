.PHONY: pb test

pb:
	protofmt -w  fieldset.proto
	protoc fieldset.proto --go_out=.

test:
	go test -v -cover