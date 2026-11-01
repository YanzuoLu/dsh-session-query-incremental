# dsh-session-query-incremental

Incremental live indexing for DeepSeek Harness (dsh) history search.

## Why

Before every search page, the native `@deepseek-ai/dsh-session-query-sqlite` backend re-observes every loaded session. It deep-clones, serializes and hashes each complete event log, then deletes and re-inserts every full-text document of each session that grew. Query filters apply only afterwards. With a dozen loaded sessions totalling about 1.4 GB of logs, each `session_search` or `session_event_search` blocks the whole dsh event loop for 25–40 seconds, freezing every conversation.

## Behavior

- Subclasses the native SQLite engine. Search, ranking, snippets, cursors, filters and the on-disk index are unchanged, and an existing index file is reused.
- A live session is unchanged while its session instance and event count are. Session logs are append-only, so nothing is read or written for it.
- When a session grows, only its new events are indexed. Surface labels and text are computed exactly as the native document builder does.
- When new events reclassify already indexed ones, as compaction does, that session's live documents are rebuilt in full.
- Index state advances only after the reconciliation transaction commits.
- Persisted sessions keep the native revision-based reuse, without the unused content hash.

## Install

```sh
dsh plugin --profile web add github:YanzuoLu/dsh-session-query-incremental
```

The bundle mounts nothing by itself. In the profile patch, point the history search index entry at this package and keep its config:

```yaml
- id: history-query-index
  name: dsh-session-query-incremental
  config:
    path: /Users/you/.dsh/cache/session-search.db
    openAt: first-search
```

No dsh restart is required with a live profile.

## Compatibility

Built for dsh 0.2.0-rc.2. It overrides internal methods of the native engine, so its peer dependencies pin that exact version and dsh refuses to load it after an upgrade until it is re-verified.
