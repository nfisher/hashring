
.PHONY: all
all:
	go test -race -coverprofile=coverage.txt -covermode=atomic ./...
