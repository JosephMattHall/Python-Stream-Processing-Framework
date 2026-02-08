# PSPF CLI (`pspfctl`)

PSPF includes a command-line interface tool, `pspfctl`, to manage and inspect your stream processing applications.

## Installation

The CLI is included with the `pspf` package.
If you installed via pip:
```bash
pspfctl --help
```

## Commands

### Inspecting Data
You can inspect the contents of the `LocalLog` directly on disk, without running a worker. This is useful for debugging corruption or checking data state in environment where you have file access.

```bash
# View first 10 records of partition 0
pspfctl inspect ./data --partition 0 --limit 10

# View last 5 records (tail)
pspfctl inspect ./data --partition 0 --tail --limit 5
```

### Remote Control
If your worker has the Admin API enabled (default port 8001), you can control it remotely.

#### Check Status
```bash
pspfctl status --url http://localhost:8001
# Output: ✅ Worker Online: {'status': 'ok', 'worker_state': 'running'}
```

#### Pause Processing
Pauses the consumer loop. The process remains alive but stops pulling new messages.
```bash
pspfctl pause --url http://localhost:8001
```

#### Resume Processing
Resumes the consumer loop efficiently.
```bash
pspfctl resume --url http://localhost:8001
```
