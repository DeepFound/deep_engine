# Warnings
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall")

# Reentrant
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D_REENTRANT")

# tcmalloc / jemalloc (used for performance and functionality)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DTCMALLOC")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DJEMALLOC")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DSYSALLOC")

# Turn this flag on to speed up builds
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_REDUCE_TEMPLATES")

# Turn this flag on to validate memory (i.e. see Converter)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCXX_LANG_MEMORY_DEBUG")

# Logging system (this includes, INFO, WARN, ERROR, and DEBUG)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCXX_UTIL_LOGGING")

# Turn this flag on to use binary-search-b+tree (i.e. tree::get)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCXX_UTIL_TREE_BSEARCH")

# Turn this flag on to use slotted-node-b+tree (i.e. tree::next)
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCXX_UTIL_TREE_SLOTTED")

# Turn this flag on to support rekeying (i.e. case insensitive strings)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCOM_DEEPIS_DB_REKEY")

# Turn this flag on for charset support
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCOM_DEEPIS_DB_CHARSET")

# Turn this flag on to perform segment index-referencing
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCOM_DEEPIS_DB_INDEX_REF")

# Turn this flag on for cardinality statistics
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCOM_DEEPIS_DB_CARDINALITY")

# Turn this flag on for cardinality verification
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DCOM_DEEPIS_DB_CARDINALITY_VERIFY")

# Turn this flag on if threads are less than cpu cores
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_USERLOCK")

# Turn this flag on for io statistic calculation
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_IO_STATS")

# Turn this flag on for commit statistic calculation
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_COMMIT_STATS")

# Turn this flag on for data validation
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_VALIDATE_DATA")

# Turn this flag on to force the use of the v1.1 protocol
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_PROTOCOL_V1_1")

# Turn on this flag on to enable recovery replay debug
# NOTE: introduces LRT protocol changes
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_RECOVERY_DEBUG")

# Turn this flag on for atomic segment state flag operations
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_ATOMIC_SEGMENT_FLAGS")

# Turn this flag on to enable FLUX checkpointing
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_CHECKPOINT")

# Turn this flag on for durable persistence synchronization grouping
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_SYNCHRONIZATION_GROUPING")

# Turn this flag on to allow unlocking of last row read if necessary
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_LAST_ROW_UNLOCK")

# Turn this flag on to read all compressed values through the primary
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_COMPRESS_PRIMARY_READ")

# Turn this flag on to extra packing for multibyte character sets
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DDEEP_MULTIBYTE_PACK")
