package text.indexer.demo.lib.impl.storage

import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

private val sizeCounter = AtomicInteger(0)
private val trieLock = ReentrantReadWriteLock()


/**
 * Trie using Hashmaps and Hashsets
 * For 13Mb of text files:
 * DEBUG DemoApp - Using 45MB, indexed 58169 words
 * DEBUG DemoApp - Using 38MB, indexed 50086 words case insensitive
 *
 * For 968Mb of text files (wiki dump):
 * DEBUG DemoApp - Using 1459MB, indexed 2148614 words - 68sec
 */
@Deprecated("Remove method does not work")
class MapTrieStorage : ReverseIndexStorage<String, Path> {
    private val root = SimpleTrieNode()

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

class SimpleTrieNode {
    val children = hashMapOf<Char, SimpleTrieNode>()
    val documents = hashSetOf<Path>()

    fun addDocument(document: Path) {
        if (documents.isEmpty()) {
            sizeCounter.incrementAndGet()
        }
        documents.add(document)
    }

    fun getDocuments(): List<Path> {
        return documents.toList()
    }

    fun addChild(c: Char): SimpleTrieNode {
        return children.computeIfAbsent(c) { SimpleTrieNode() }
    }

    fun getChild(c: Char): SimpleTrieNode? {
        return children[c]
    }
}