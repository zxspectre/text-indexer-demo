package text.indexer.demo.lib.impl.storage

/**
 * Storage structure for reverse index.
 * K is the keyword type, V is reference to the document, holding the keyword.
 * Normally there are several V references for one keyword K.
 */
interface ReverseIndexStorage<K, V> {
    fun get(keyword: K): Collection<V>
    fun put(keyword: K, document: V)
    fun remove(document: V) {
        remove(setOf(document))
    }

    fun remove(documents: Set<V>)
    fun size(): Int
}
