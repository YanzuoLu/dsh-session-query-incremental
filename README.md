# dsh-session-query-incremental

Incremental indexing for DeepSeek Harness (dsh) history search.

## Why

Before every search page, the native `@deepseek-ai/dsh-session-query-sqlite` backend re-observes every loaded session. It deep-clones, serializes and hashes each complete event log, then deletes and re-inserts every full-text document of each session that grew. Deleting a session's documents scans the whole full-text table, because `session_id` is an unindexed FTS5 column. Query filters apply only afterwards. With a dozen loaded sessions totalling about 1.4 GB of logs and a 1.2 GB on-disk index, each `session_search` or `session_event_search` blocks the whole dsh event loop for 20–40 seconds, freezing every conversation and breaking request uploads that are in flight.

## Behavior

- Subclasses the native SQLite engine. Search, ranking, snippets, cursors, filters and the on-disk index are unchanged, and an existing index file is reused.
- A live session is unchanged while its session instance and event count are. Session logs are append-only, so nothing is read or written for it.
- When a session grows, only its new events are indexed. Surface labels and text are computed exactly as the native document builder does.
- When new events reclassify already indexed ones, as compaction does, that session's live documents are rebuilt in full.
- Every document's rowid is remembered, so replacing or removing a session deletes exactly its rows instead of scanning the table. Persisted rowids are read once, on the first persisted change.
- Index state advances only after the reconciliation transaction commits.
- Persisted sessions keep the native revision-based reuse, without the unused content hash.

With two loaded sessions of about 500 MB, a search after new events takes about 0.2 s instead of 5–6 s.

## Install

```sh
dsh plugin --profile web add github:YanzuoLu/dsh-session-query-incremental
```

The bundle mounts nothing by itself. In the profile patch, use this package for the history search index entry and keep its config:

```yaml
- id: history-query-incremental
  name: dsh-session-query-incremental
  config:
    path: /Users/you/.dsh/cache/session-search.db
    openAt: first-search
```

dsh does not re-import the module of a running entry whose `name` changes. To switch a live profile from the native engine without a restart, replace the entry with one under a new `id`, as above. The first search afterwards indexes the loaded sessions once.

## Compatibility

Built for dsh 0.2.0-rc.2. It overrides internal methods of the native engine, so its peer dependencies pin that exact version and dsh refuses to load it after an upgrade until it is re-verified.
