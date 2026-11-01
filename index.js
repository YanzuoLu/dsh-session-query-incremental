/**
 * Incremental indexing for DeepSeek Harness (dsh) history search.
 *
 * Before every search page the native SQLite backend re-observes every loaded
 * session: it deep-clones, serializes and hashes each complete event log, then
 * deletes and re-inserts all full-text documents of every session that grew.
 * Deleting a session's documents scans the whole full-text table, because
 * `session_id` is an unindexed FTS5 column. With large sessions each search
 * blocks the dsh event loop for tens of seconds.
 *
 * Session logs are append-only, so this engine detects a change from the
 * session instance and its event count, and indexes only the new events. A
 * live session is re-indexed in full only when new events reclassify already
 * indexed ones, which compaction does. Every document's rowid is remembered,
 * so removing a session's documents deletes exactly those rows.
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
  /** Live index state per session id as of the last committed reconciliation. */
  _live = new Map()
  /** Rowids of each persisted session's documents, loaded on first use. */
  _persisted
  /** Index changes of the reconciliation in progress, applied only if it commits. A null value removes. */
  _staged
  _instances = new WeakMap()
  _nextInstance = 0

  async _reconcile(signal) {
    this._staged = { live: new Map(), persisted: new Map() }
    try {
      const binding = await super._reconcile(signal)
      for (const [id, state] of this._staged.live) state === null ? this._live.delete(id) : this._live.set(id, state)
      for (const [id, rowids] of this._staged.persisted) rowids === null ? this._persisted.delete(id) : this._persisted.set(id, rowids)
      return binding
    } catch (error) {
      // The rowid index may have been read inside the rolled-back transaction.
      this._persisted = undefined
      throw error
    } finally {
      this._staged = undefined
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

  /** Write one changed live session, appending its new documents when the indexed ones still hold. */
  _replaceLiveSession(entry, generation, persisted) {
    const { session, events } = entry
    const id = entry.header.id
    const db = this._requireDb()
    const previous = this._liveState(id)
    const row = db.prepare('SELECT fingerprint FROM temp.live_sessions WHERE id = ?').get(id)
    let documents
    if (previous?.session === session && row?.fingerprint === previous.fingerprint) {
      const surfaces = classifySurface(events)
      if (sameSurfaces(previous.labels, surfaces)) {
        documents = []
        for (const event of events.slice(previous.count)) {
          const text = extractSessionEventText(event)
          if (text.length > 0) documents.push({ sessionId: id, seq: event.seq, type: event.type, time: event.time, surface: surfaces.get(event.seq) ?? 'log-only', text })
        }
      }
    }
    const append = documents !== undefined
    if (!append) {
      documents = buildSessionEventSearchDocuments(id, events)
      if (row !== undefined) this._deleteLiveDocuments(id, previous)
    }
    db.prepare('DELETE FROM temp.live_sessions WHERE id = ?').run(id)
    db.prepare(`
      INSERT INTO temp.live_sessions
        (id, version, created_at, cwd, parent_session, seed_length, delegation_depth, agent_preset, fingerprint, persisted, generation)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(...headerBindings(entry.header, entry.inheritedEventCount), entry.fingerprint, persisted ? 1 : 0, generation)
    const labels = append ? new Map(previous.labels) : new Map()
    for (const document of documents) labels.set(document.seq, document.surface)
    const rowids = [...(append ? previous.rowids : []), ...this._insertDocuments('temp.live_docs', documents)]
    this._staged.live.set(id, { session, fingerprint: entry.fingerprint, count: events.length, labels, rowids })
  }

  /** Write one changed persisted session, remembering its document rowids. */
  _replacePersistedSession(entry, revision, generation) {
    this._deleteSession('persisted', entry.header.id)
    this._requireDb().prepare(`
      INSERT INTO persisted_sessions
        (id, version, created_at, cwd, parent_session, seed_length, delegation_depth, agent_preset, revision, generation)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    `).run(...headerBindings(entry.header, entry.inheritedEventCount), revision, generation)
    this._staged.persisted.set(entry.header.id, this._insertDocuments('persisted_docs', entry.documents))
  }

  /** Remove one session's documents by rowid instead of scanning the full-text table. */
  _deleteSession(source, id) {
    const db = this._requireDb()
    if (source === 'persisted') {
      for (const rowid of this._persistedRowids(id)) db.prepare('DELETE FROM persisted_docs WHERE rowid = ?').run(rowid)
      db.prepare('DELETE FROM persisted_sessions WHERE id = ?').run(id)
      this._staged.persisted.set(id, null)
    } else {
      this._deleteLiveDocuments(id, this._liveState(id))
      db.prepare('DELETE FROM temp.live_sessions WHERE id = ?').run(id)
      this._staged.live.set(id, null)
    }
  }

  _liveState(id) {
    return this._staged.live.has(id) ? this._staged.live.get(id) ?? undefined : this._live.get(id)
  }

  _deleteLiveDocuments(id, state) {
    const db = this._requireDb()
    if (state === undefined) db.prepare('DELETE FROM temp.live_docs WHERE session_id = ?').run(id)
    else for (const rowid of state.rowids) db.prepare('DELETE FROM temp.live_docs WHERE rowid = ?').run(rowid)
  }

  _persistedRowids(id) {
    if (this._staged.persisted.has(id)) return this._staged.persisted.get(id) ?? []
    if (this._persisted === undefined) {
      this._persisted = new Map()
      for (const { rowid, session_id: sessionId } of this._requireDb().prepare('SELECT rowid, session_id FROM persisted_docs').all()) {
        let rowids = this._persisted.get(sessionId)
        if (rowids === undefined) this._persisted.set(sessionId, rowids = [])
        rowids.push(rowid)
      }
    }
    return this._persisted.get(id) ?? []
  }

  /** Insert documents exactly as the native engine does and return their rowids. */
  _insertDocuments(table, documents) {
    const insert = this._requireDb().prepare(`
      INSERT INTO ${table} (text, session_id, seq, type, time, surface, codepoint_length)
      VALUES (?, ?, ?, ?, ?, ?, ?)
    `)
    return documents.map((document) => {
      const text = sanitizeFtsText(document.text)
      return Number(insert.run(text, document.sessionId, document.seq, document.type, document.time, document.surface, Array.from(text).length).lastInsertRowid)
    })
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

function sanitizeFtsText(text) {
  return text.replaceAll('\0', '\uFFFD').replaceAll('\uFDD0', '\uFFFD').replaceAll('\uFDD1', '\uFFFD')
}

function headerBindings(header, inheritedEventCount) {
  return [
    header.id,
    header.version,
    header.createdAt,
    header.cwd ?? null,
    header.parentSession ?? null,
    header.isSeeded ? inheritedEventCount : null,
    header.delegationDepth ?? null,
    header.agentPreset ?? null,
  ]
}

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
