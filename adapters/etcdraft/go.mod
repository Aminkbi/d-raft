module github.com/aminkbi/d-raft/adapters/etcdraft

go 1.26

toolchain go1.27.1

require (
	github.com/aminkbi/d-raft v0.0.0
	go.etcd.io/raft/v3 v3.7.0
	google.golang.org/protobuf v1.36.11
)

replace github.com/aminkbi/d-raft => ../..
