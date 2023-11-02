package text.indexer.demo.lib.impl

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.MainScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import java.io.File
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchKey
import kotlin.io.path.isDirectory

@Deprecated("does not work for files and recursive dirs, also double polling")
class FSWatcherNioBroken {
    private val watchService = FileSystems.getDefault().newWatchService()

    private val scope = MainScope()

    init {
        scope.launch {
            while (true) {
                println("watch loop")
                val watchKey: WatchKey = withContext(Dispatchers.IO) {
                    watchService.take()
                }

                println("got key $watchKey")
                for (event in watchKey.pollEvents()) {
                    val kind = event.kind()
                    val context = event.context() as Path

                    // Handle different types of events (create, modify, delete)
                    when (kind) {
                        StandardWatchEventKinds.ENTRY_CREATE -> {
                            // File or directory created
                            println("Created: $context")
                        }

                        StandardWatchEventKinds.ENTRY_MODIFY -> {
                            // File or directory modified
                            println("Modified: $context")
                        }

                        StandardWatchEventKinds.ENTRY_DELETE -> {
                            // File or directory deleted
                            println("Deleted: $context")
                        }
                    }
                }

                if (!watchKey.reset()) {
                    break // Exit the loop if the watch key is no longer valid
                }
            }
        }
    }

    fun register(file: File) {
        register(file.toPath())

    }

    fun register(path: Path) {
        var eventsToWatch = arrayOf(StandardWatchEventKinds.ENTRY_MODIFY)
        if (path.isDirectory()) {
            eventsToWatch = arrayOf(
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_DELETE)
        }
        path.register(
            watchService,
            eventsToWatch
        )
    }
}
