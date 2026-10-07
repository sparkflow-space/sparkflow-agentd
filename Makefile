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
	  --go_opt=Magent/v1/hostchannel.proto=github.com/sparkflow-space/sparkflow-agentd/internal/generated/agent/v1\;agentv1 \
	  --go-grpc_opt=Magent/v1/hostchannel.proto=github.com/sparkflow-space/sparkflow-agentd/internal/generated/agent/v1\;agentv1 \
	  -I api/proto \
	  api/proto/agent/v1/hostchannel.proto

# tidy-check belongs in the gates because this repo is a PUBLISHED MODULE: a
# direct dependency left marked `// indirect` is a go.mod that does not describe
# the module anyone downloads.
tidy-check:
	go mod tidy
	git diff --exit-code go.mod go.sum

# gofmt is pointed at tracked files, not at `.`: a GOPATH inside the project
# (which CI uses for caching) would otherwise put a dependency's formatting in
# our gate.
gates:
	go build ./... && go test ./... -count=1 && test -z "$$(git ls-files '*.go' | xargs gofmt -l)" && go vet ./...
	@echo "gates: OK"

# The race detector is where the control-mode fan-out is actually proved: its
# subscriber lifecycle is concurrent by nature.
race:
	go test -race ./... -count=1
