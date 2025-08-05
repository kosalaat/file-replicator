.PHONY: generate build clean run all

generate:
	# Install the necessary tools for generating Go code from protobuf
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

	# Install the necessary tools for generating Go code from protobuf
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest

	# make sure the output directory exists
	mkdir -p replicator

	# Generate the Go code from the protobuf file
	protoc \
    --go_out=replicator \
    --go_opt=paths=source_relative \
    --go-grpc_out=replicator \
    --go-grpc_opt=paths=source_relative \
	replicator.proto
	go mod tidy

build: *.go
	go build -o bin/filereplication main.go

clean: 
	rm bin/*

run:
	go run main.go

test-verbose:
	go test -v -timeout 30s ./pkg/*
test:
	go test -timeout 30s ./pkg/*

all: generate run

