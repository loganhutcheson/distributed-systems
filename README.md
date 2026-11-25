# distributed systems in Go

MIT 6.5840 / 6.824 labs, collected in one place with full Git history.

| lab | focus | status |
| --- | --- | --- |
| [key/value server](kv-server/) | RPC, retries, duplicate-request handling | race-enabled test suite passes |
| [MapReduce](map-reduce/) | workers, scheduling, partitioning, timeout reassignment | full `test-mr.sh` suite passes |
| [Raft](raft/) | elections (3A), replication (3B), persistence (3C), snapshots (3D) | all 28 tests pass under `-race` |

## checks

run inside each lab’s `src/` directory:

```sh
# key/value server
go test -race ./kvsrv -count=1
# Raft (3A–3D)
cd raft && go test -race -count=1
# MapReduce: requires Go plugin support
cd main && bash test-mr.sh
```

## attribution

[MIT labs](https://pdos.csail.mit.edu/6.824/) supply the scaffolding and test harnesses. my implementations live in `raft/src/raft/raft.go`, `kv-server/src/kvsrv/`, and `map-reduce/src/mr/`.

the original repositories remain archived. these are learning projects, not production services.
