package text.indexer.demo.lib.impl.fswatcher

import java.nio.file.Path

enum class EventType {
    NEW, DELETED
}

class FSEvent(val type: EventType, val files: Set<Path>)