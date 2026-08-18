package tephra

// Regenerate the wire types from the vendored protobuf schema. Requires `protoc` and
// `protoc-gen-go` on PATH (both provided by devenv); run with `go generate ./...`.
//
//go:generate protoc --proto_path=proto --go_out=. --go_opt=module=github.com/tqwewe/tephra-go proto/tephra/v1/tephra.proto
