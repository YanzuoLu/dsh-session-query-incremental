/**
 * Incremental live indexing for DeepSeek Harness (dsh) history search.
 *
 * Before every search page the native SQLite backend re-observes every loaded
 * session: it deep-clones, serializes and hashes each complete event log, then
 * deletes and re-inserts all full-text documents of every session that grew.
 * With large running sessions this blocks the dsh event loop for tens of
 * seconds per search. Session logs are append-only, so this engine detects a
 * change from the session instance and its event count, and indexes only the
 * new events. A session is re-indexed in full only when new events reclassify
 * already indexed ones, which compaction does.
 *
 * Search, ranking, cursors and the on-disk index stay native. This subclass
 * overrides internal methods of the dsh 0.2.0-rc.2 SQLite engine, so it pins
 * that version and must be re-verified on every dsh upgrade.
 *
 * @module dsh-session-query-incremental
 */
import SqliteSessionQueryEngine from '@deepseek-ai/dsh-session-query-sqlite'
import {
  SessionQueryError,
  assertSessionHeadersCompatible,
  buildSessionEventSearchDocuments,
  extractSessionEventText,
  readColdSessionLog,
} from '@deepseek-ai/dsh-session-query'
import { foldSurface } from '@deepseek-ai/dsh-session'
import { currentSessionMessageProjections } from '@deepseek-ai/dsh-session-format-catalog/message-projections'

const STABLE_OBSERVATION_ATTEMPTS = 2

export default class IncrementalSqliteSessionQueryEngine extends SqliteSessionQueryEngine {
  /** Index state of each live Session instance as of the last committed reconciliation. */
  _indexed = new WeakMap()
  /** Index states written by the reconciliation in progress, kept only if it commits. */
  _pending
  /** Live session whose replacement keeps its existing documents and appends new ones. */
  _appending
  _instances = new WeakMap()
  _nextInstance = 0

  async _reconcile(signal) {
    this._pending = new Map()
    try {
      const binding = await super._reconcile(signal)
      for (const [session, state] of this._pending) this._indexed.set(session, state)
      return binding
    } finally {
      this._pending = undefined
    }
  }

  /** The native observation, with live sessions observed by identity and length instead of content. */
  async _observeStable(indexed, signal) {
    for (let attempt = 0; attempt < STABLE_OBSERVATION_ATTEMPTS; attempt += 1) {
      assertNotAborted(signal)
      const persistenceBinding = this._persistenceBinding
      const persistence = persistenceBinding.service
      const initiallyLive = new Set(this.ctx.sessions.list().map((session) => session.id))
      let persisted = new Map()
      if (persistence !== undefined) try {
        const canReuseIndexed = this._lastPersistenceIdentity === undefined || this._lastPersistenceIdentity === persistenceBinding.identity
        const listOptions = signal === undefined ? undefined : { signal }
        const before = await persistence.list(listOptions)
        assertNotAborted(signal)
        persisted = materializePersistenceSnapshots(before)
        for (const entry of persisted.values()) {
          if (canReuseIndexed && indexed.get(entry.header.id)?.revision === entry.revision) continue
          if (initiallyLive.has(entry.header.id) || this.ctx.sessions.get(entry.header.id) !== undefined) continue
          assertNotAborted(signal)
          const loaded = await readColdSessionLog(persistence, entry.header.id, signal)
          assertNotAborted(signal)
          assertSessionHeadersCompatible(entry.header, loaded.header)
          entry.loaded = {
            header: structuredClone(loaded.header),
            inheritedEventCount: loaded.inheritedEventCount,
            documents: buildSessionEventSearchDocuments(loaded.header.id, loaded.events),
          }
        }
        assertNotAborted(signal)
        const after = materializePersistenceSnapshots(await persistence.list(listOptions))
        assertNotAborted(signal)
        if (!samePersistenceSnapshots(persisted, after)) continue
        if (this._persistenceBinding !== persistenceBinding) continue
      } catch (error) {
        if (isAbort(error) || signal?.aborted) throw new SessionQueryError('session-search aborted', 'SESSION_QUERY_ABORTED', { cause: error })
        if (this._persistenceBinding !== persistenceBinding) continue
        if (error instanceof SessionQueryError) throw error
        throw new SessionQueryError(`session-search persistence observation failed: ${errorMessage(error)}`, 'SESSION_QUERY_PERSISTENCE_FAILED', { cause: error })
      }
      const live = new Map()
      for (const session of this.ctx.sessions.list()) {
        const observed = this._observeLive(session)
        const durable = persisted.get(session.id)
        if (durable !== undefined) assertSessionHeadersCompatible(observed.header, durable.header)
        live.set(session.id, observed)
      }
      if (!sameSessionIds(initiallyLive, live)) continue
      return { persistenceBinding, persisted, live }
    }
    throw new SessionQueryError('session-search persistence observation did not stabilize after one retry', 'SESSION_QUERY_PERSISTENCE_FAILED')
  }

  /** Observe one live session without reading its content. Documents are built only if it is written. */
  _observeLive(session) {
    let instance = this._instances.get(session)
    if (instance === undefined) this._instances.set(session, instance = ++this._nextInstance)
    const events = session.snapshotEvents()
    return {
      session,
      events,
      header: structuredClone(session.header),
      inheritedEventCount: session.inheritedEventCount,
      fingerprint: `${instance}:${events.length}`,
    }
  }

  _replaceLiveSession(entry, generation, persisted) {
    const { session, events } = entry
    const id = entry.header.id
    const previous = this._indexed.get(session)
    const row = this._requireDb().prepare('SELECT fingerprint FROM temp.live_sessions WHERE id = ?').get(id)
    let append = false
    let documents
    if (previous !== undefined && row?.fingerprint === previous.fingerprint) {
      const surfaces = classifySurface(events)
      if (sameSurfaces(previous.labels, surfaces)) {
        append = true
        documents = []
        for (const event of events.slice(previous.count)) {
          const text = extractSessionEventText(event)
          if (text.length > 0) documents.push({ sessionId: id, seq: event.seq, type: event.type, time: event.time, surface: surfaces.get(event.seq) ?? 'log-only', text })
        }
      }
    }
    if (!append) documents = buildSessionEventSearchDocuments(id, events)
    const labels = append ? new Map(previous.labels) : new Map()
    for (const document of documents) labels.set(document.seq, document.surface)
    this._appending = append ? id : undefined
    try {
      super._replaceLiveSession({ ...entry, documents }, generation, persisted)
    } finally {
      this._appending = undefined
    }
    this._pending.set(session, { fingerprint: entry.fingerprint, count: events.length, labels })
  }

  /** Appending replaces the session row only, keeping its indexed documents. */
  _deleteSession(source, id) {
    if (source === 'live' && id === this._appending) {
      this._requireDb().prepare('DELETE FROM temp.live_sessions WHERE id = ?').run(id)
      return
    }
    super._deleteSession(source, id)
  }
}

/** Surface labels exactly as `buildSessionEventSearchDocuments` assigns them. */
function classifySurface(events) {
  let folded
  try {
    folded = foldSurface(events, currentSessionMessageProjections)
  } catch (error) {
    throw new SessionQueryError(`invalid session surface: ${error instanceof Error ? error.message : 'unknown error'}`, 'SESSION_QUERY_INVALID_SURFACE', { cause: error })
  }
  const result = new Map()
  for (const seq of folded.nodes) result.set(seq, 'current')
  for (const replacement of folded.replacements) for (const seq of replacement.shadowedSeqs) result.set(seq, 'shadowed')
  return result
}

function sameSurfaces(indexed, surfaces) {
  for (const [seq, surface] of indexed) if ((surfaces.get(seq) ?? 'log-only') !== surface) return false
  return true
}

// The helpers below mirror the private ones of the native engine.

function materializePersistenceSnapshots(snapshots) {
  if (!Array.isArray(snapshots)) throw new Error('persistence snapshots must be an array')
  const result = new Map()
  for (const snapshot of snapshots) {
    if (typeof snapshot.revision !== 'string') throw new Error('persistence snapshot revision must be a string')
    const header = structuredClone(snapshot.header)
    if (result.has(header.id)) throw new Error(`persistence listed duplicate session "${header.id}"`)
    result.set(header.id, { header, revision: snapshot.revision })
  }
  return result
}

function samePersistenceSnapshots(before, after) {
  if (before.size !== after.size) return false
  for (const [id, first] of before) {
    const second = after.get(id)
    if (second === undefined || first.revision !== second.revision || !sameHeader(first.header, second.header)) return false
  }
  return true
}

function sameHeader(a, b) {
  return a.id === b.id && a.createdAt === b.createdAt && a.cwd === b.cwd && a.parentSession === b.parentSession && a.isSeeded === b.isSeeded && (a.delegationDepth ?? 0) === (b.delegationDepth ?? 0) && a.agentPreset === b.agentPreset
}

function sameSessionIds(before, after) {
  if (before.size !== after.size) return false
  for (const id of before) if (!after.has(id)) return false
  return true
}

function errorMessage(error) {
  return error instanceof Error ? error.message : 'unknown error'
}

function isAbort(error) {
  return error instanceof SessionQueryError && error.code === 'SESSION_QUERY_ABORTED'
}

function assertNotAborted(signal) {
  if (signal?.aborted) throw new SessionQueryError('session-search aborted', 'SESSION_QUERY_ABORTED')
}
