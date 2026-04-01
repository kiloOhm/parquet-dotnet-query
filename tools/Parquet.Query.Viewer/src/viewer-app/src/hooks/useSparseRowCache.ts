import { useCallback, useRef, useState } from 'react'
import type { DataPage } from '@/api/types'

const CHUNK_SIZE = 500
const MAX_CHUNKS = 200 // 100,000 rows in memory at most

export interface SparseRowCache {
  /** Get a row by absolute index. Returns null if the chunk isn't loaded. */
  getRow: (index: number) => unknown[] | null
  /** Request that a range of rows be ensured in the cache. */
  ensureRange: (startRow: number, endRow: number) => void
  /** Seed a specific chunk directly (for the initial fetch whose data we already have). */
  seedChunk: (chunkIndex: number, rows: unknown[][]) => void
  /** Clear the entire cache (on file/query change). */
  clear: () => void
  /** Opaque counter that increments whenever the cache contents change. */
  version: number
}

export function useSparseRowCache(
  fetchPage: (offset: number, limit: number) => Promise<DataPage>,
): SparseRowCache {
  const cacheRef = useRef(new Map<number, unknown[][]>())
  const inFlightRef = useRef(new Set<number>())
  const lruRef = useRef<number[]>([])
  const generationRef = useRef(0)
  // Version counter — bumped on every cache mutation to trigger re-renders
  const [version, setVersion] = useState(0)

  const touchLru = (chunkIndex: number) => {
    const arr = lruRef.current
    const idx = arr.indexOf(chunkIndex)
    if (idx >= 0) arr.splice(idx, 1)
    arr.unshift(chunkIndex)
  }

  const evictIfNeeded = () => {
    const arr = lruRef.current
    const cache = cacheRef.current
    while (arr.length > MAX_CHUNKS) {
      const evicted = arr.pop()!
      cache.delete(evicted)
    }
  }

  // Stable identity — reads from the ref, never stale.
  // Consumers use `version` as a prop/dep to know when to re-render.
  const getRow = useCallback((rowIndex: number): unknown[] | null => {
    const chunkIndex = Math.floor(rowIndex / CHUNK_SIZE)
    const chunk = cacheRef.current.get(chunkIndex)
    if (!chunk) return null
    const localIndex = rowIndex % CHUNK_SIZE
    return chunk[localIndex] ?? null
  }, [])

  const seedChunk = useCallback((chunkIndex: number, rows: unknown[][]) => {
    cacheRef.current.set(chunkIndex, rows)
    touchLru(chunkIndex)
    evictIfNeeded()
    inFlightRef.current.delete(chunkIndex)
    setVersion(v => v + 1)
  }, [])

  const ensureRange = useCallback((startRow: number, endRow: number) => {
    const startChunk = Math.floor(startRow / CHUNK_SIZE)
    const endChunk = Math.floor(endRow / CHUNK_SIZE)
    const gen = generationRef.current

    for (let ci = startChunk; ci <= endChunk; ci++) {
      if (cacheRef.current.has(ci) || inFlightRef.current.has(ci)) {
        if (cacheRef.current.has(ci)) touchLru(ci)
        continue
      }

      inFlightRef.current.add(ci)
      const offset = ci * CHUNK_SIZE

      fetchPage(offset, CHUNK_SIZE)
        .then(page => {
          if (generationRef.current !== gen) return
          cacheRef.current.set(ci, page.rows)
          touchLru(ci)
          evictIfNeeded()
          setVersion(v => v + 1)
        })
        .catch(err => {
          console.error(`Failed to fetch chunk ${ci}:`, err)
        })
        .finally(() => {
          inFlightRef.current.delete(ci)
        })
    }
  }, [fetchPage])

  const clear = useCallback(() => {
    cacheRef.current.clear()
    inFlightRef.current.clear()
    lruRef.current.length = 0
    generationRef.current += 1
    setVersion(v => v + 1)
  }, [])

  return { getRow, ensureRange, seedChunk, clear, version }
}

export { CHUNK_SIZE }
