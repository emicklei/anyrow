.PHONY: pb

pb:
	protofmt -w  fieldset.proto
	protoc fieldset.proto --go_out=.