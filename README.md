# text-indexer-demo
Demo of a Kotlin library for text indexation

## TODO
- test working with several instances of services
- test working multithreaded with same service instance


## Agreed upon requirements
- Support add and remove file/dir methods
- Implement search method that will return reference to documents containing words
- Support for concurrent access
- React to changes in the watched part of filesystem
- Support tokenizers for word extraction
- No state persitence between sessions
- Tests and sample program included
- Coroutines & channels
- No callback for indexation completion callback or tracking indexation progress, just get...() methods for current state
- Search method returns results based on currently indexed subset of watched files/dirs
- Index stored in memory
- OOM protection
- No 3rd party libs for indexation engine
- All files can be considered as UTF8 text files


## Nice to have further improvements
- Remove or refactor IndexerServiceFactory, instead document various implementation specifics somewhere else
- Support (indexed/searched) words postprocessor. One specific example is implementing case-insensitive index/search
- If file not a text - skip
- handle concurrent modification of indexed file - perhaps check TS before indexing and after - reindex if changed
- Dynamically adjust various delay/bulk parameters, like DocumentProcessor.SEQUENCE_BULK_SIZE
- Better callbacks for indexation status and faster startup times
- Review classes access modifiers
- Implement machine independent load testing
- Review Job / Dispatcher / CoContext structure - tested that current impl works and terminates, but maybe it's not the kotlin way
- Handle potential exceptions in children contexts gracefully
- Remove duplicated code from tests