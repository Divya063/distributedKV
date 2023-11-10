## distributedKV

A distributed key value store built on top of Raft consensus algorithm. 

### Getting Started

Add a key:

```bash
$ curl -X POST 'localhost:9023/put' -d '{"key": "x", "value": "23"}' -H 'content-type: application/json'
```

Get the key:

```bash
$ curl 'localhost:9023/get?key=x'

```

Add a follower node:

```bash
$ curl 'localhost:9023/join?id=2'
```
### TODO

- Implement Graceful Shutdown

### Bug

- Sender is getting blocked after adding a new node. 