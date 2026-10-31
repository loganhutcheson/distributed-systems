# Distributed systems in Go

Three implementations from MIT’s 6.5840 / 6.824 distributed-systems labs, collected in one place. They explore RPC, concurrency, retries, scheduling, and consensus.

| Lab | Implementation | Status |
| --- | --- | --- |
| [Key/value server](kv-server/) | Get/Put/Append RPCs, mutex-protected state, client transaction IDs, and duplicate-request handling | Learning implementation with a linearizability test harness |
| [MapReduce](map-reduce/) | RPC coordinator and workers, partitioned intermediate files, map/reduce phases, and timeout-based task reassignment | Learning implementation; includes word-count and failure test applications |
| [Raft](raft/) | Leader election, log replication, and persisted term, vote, and log state | Work in progress: snapshot support is unimplemented; race detection finds shared-state races |

## Run checks

Each lab retains its original Go module and test harness. Run commands from its `src` directory; the lab modules intentionally share the original `6.5840` module name.

```sh
cd kv-server/src
go test -race ./kvsrv -count=1
```

```sh
cd raft/src
go test -race ./raft -run 'TestInitialElection3A|TestBasicAgree3B' -count=1
```

```sh
cd map-reduce/src/main
bash test-mr.sh
```

The Raft command currently fails the race detector. These are educational implementations, not production services. The MapReduce harness builds Go plugins and requires a supported platform and C toolchain.

## History and attribution

Imported with full Git history from `raft`, `kv-server`, and `map-reduce`; each original repository is retained as an archive. The lab scaffolding, simulated RPC network, tests, and checking tools are from MIT 6.5840 / 6.824 and their respective authors. My implementation work is in `raft/src/raft/raft.go`, `kv-server/src/kvsrv/`, and `map-reduce/src/mr/`.

[MIT distributed-systems labs](https://pdos.csail.mit.edu/6.824/)
