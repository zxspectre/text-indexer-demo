package text.indexer.demo.lib.impl

import java.nio.file.Path

enum class ErrorType {
    /**
     * Event of this type tell about how many words were not indexed because they were larger than
     * the threshold specified in [text.indexer.demo.lib.impl.IndexerService.maxWordLength].
     */
    WORDS_SKIPPED,

    /**
     * File was not indexed because its size was larger than the free heap size memory,
     * adjusted by [text.indexer.demo.lib.impl.IndexerServiceKt.FILE_MEMORY_PRINT_FACTOR]
     */
    FILE_TOO_BIG
}

/**
 * Instances of this class are used for notification about problems encountered while indexing documents.
 * See [ErrorType] for details about every event type, [value] refers to some numerical relevant (if any) characteristic
 * [file] refers to the problematic document encountered.
 */
data class IndexError(val type: ErrorType, val value: Long?, val file: Path)