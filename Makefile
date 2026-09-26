.PHONY: build test tidy fmt vet proto-gen gates

build:
	go build ./...

test:
	go test ./... -count=1

# Generated code records the compiler version, so regenerating with a different
# protoc rewrites the descriptor blob and stamps a version nobody else uses —
# a large spurious diff on every alternating regen. Match the versions, or
# change this line deliberately.
proto-gen:
	protoc \
	  --go_out=. --go_opt=module=github.com/sparkflow-space/sparkflow-agentd \
	  --go-grpc_out=. --go-grpc_opt=module=github.com/sparkflow-space/sparkflow-agentd \
	  -I api/proto \
	  api/proto/agentd/v1/agentd.proto

gates:
	go build ./... && go test ./... -count=1 && test -z "$$(gofmt -l .)" && go vet ./...
	@echo "gates: OK"
