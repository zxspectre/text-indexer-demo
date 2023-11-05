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
 */
class SimpleTrieStorage : ReverseIndexStorage<String, Path> {
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

class TrieNode {
    val children = hashMapOf<Char, TrieNode>()
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

    fun addChild(c: Char): TrieNode {
        return children.computeIfAbsent(c) { TrieNode() }
    }

    fun getChild(c: Char): TrieNode? {
        return children[c]
    }
}