package text.indexer.demo.lib.impl.storage

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.nio.file.Path
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

private val sizeCounter = AtomicInteger(0)
private val trieLock = ReentrantReadWriteLock()


/**
 * Char-ordinal array based trie for lowercase english + digits. Does not work yet for other words.
 * For 13Mb of text files:
 * -//-
 * DEBUG DemoApp - Using 41MB, indexed 50086 words case insensitive
 */
@Deprecated("Not fully implemented")
class ArrayTrieStorage : ReverseIndexStorage<String, Path> {
    private val log: Logger = LoggerFactory.getLogger(ArrayTrieStorage::class.java)

    private val root = ArrayTrieNode()

    override fun put(keyword: String, document: Path) {
        trieLock.writeLock().lock()
        try {

            var node = root
            for (char in keyword) {
                val index = code(char)
                if (node.children[index] == null) {
                    node.children[index] = ArrayTrieNode()
                }
                node = node.children[index]!!
            }
            if (node.documents.isEmpty()) {
                sizeCounter.incrementAndGet()
            }
            node.documents.add(document)
        } finally {
            trieLock.writeLock().unlock()
        }
    }


    override fun get(keyword: String): Collection<Path> {
        trieLock.readLock().lock()
        try {

            var node = root
            for (char in keyword) {
                val index = code(char)
                if (node.children[index] == null) {
                    return emptyList()
                }
                node = node.children[index]!!
            }
            return node.documents
        } finally {
            trieLock.readLock().unlock()
        }
    }

    private fun code(char: Char): Int{
        return when {
            char.isLetter() -> char - 'a'
            char.isDigit() -> char - '0' + 26
            else -> throw IllegalArgumentException("Invalid character: $char")
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

class ArrayTrieNode {
    val children = Array<ArrayTrieNode?>(36) { null }
    val documents = hashSetOf<Path>()
}