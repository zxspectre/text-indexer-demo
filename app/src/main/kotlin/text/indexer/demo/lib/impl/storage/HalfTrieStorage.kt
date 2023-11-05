package text.indexer.demo.lib.impl.storage

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

private val sizeCounter = AtomicInteger(0)
private val trieLock = ReentrantReadWriteLock()


/**
 * Char-ordinal array based trie for english words, Multimap for other words.
 * For 13Mb of text files:
 *
 *  case insensitive
 */
class HalfTrieStorage : ReverseIndexStorage<String, Path> {
    private val root = TrieNode()

    override fun put(keyword: String, document: Path) {
        trieLock.writeLock().lock()
        try {

            var currentNode = root

            for (char in keyword) {
                currentNode = currentNode.addChild(char)
            }

            currentNode.addDocument(document)
        } finally {
            trieLock.writeLock().unlock()
        }
    }


    override fun get(keyword: String): Collection<Path> {
        //TODO improve search traverse?
        trieLock.readLock().lock()
        try {

            var currentNode = root
            for (char in keyword) {
                currentNode = currentNode.getChild(char) ?: return emptyList()
            }

            return currentNode.getDocuments()
        } finally {
            trieLock.readLock().unlock()
        }

    }

    override fun remove(document: Path) {
        //TODO("Not yet implemented")
        //sizeCounter ?
    }

    override fun size(): Int {
        return sizeCounter.get()
    }
}