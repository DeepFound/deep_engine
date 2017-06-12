cmake_minimum_required(VERSION 2.8)

project(deepis-platform)

set(TOP ${CMAKE_SOURCE_DIR}/..)

include(${TOP}/build/cmake/Common.cmake)

# 
# Import packages
#

#
# Includes
#
set(DEEPIS_PLATFORM_INC ${CMAKE_SOURCE_DIR}/src/main/native)

include_directories(${DEEPIS_STANDARD_INCS} ${DEEPIS_PLATFORM_INC})

#
# Source Files
#
set (DEEPIS_PLATFORM_SRC
  # lang
  src/main/native/cxx/lang/Object.cxx 
  src/main/native/cxx/lang/System.cxx 
  src/main/native/cxx/lang/Thread.cxx 
  src/main/native/cxx/lang/Character.cxx

  # util
  src/main/native/cxx/util/Converter.cxx
  src/main/native/cxx/util/Logger.cxx 
  src/main/native/cxx/util/HashMap.cxx 
  src/main/native/cxx/util/HashSet.cxx
  src/main/native/cxx/util/TreeMap.cxx
  src/main/native/cxx/util/TreeSet.cxx
  src/main/native/cxx/util/concurrent/locks/Lock.cxx

  # io
  src/main/native/cxx/io/File.cxx 
  src/main/native/cxx/io/FileDescriptor.cxx
  src/main/native/cxx/io/FileUtil.cxx 
  src/main/native/cxx/io/RandomAccessFile.cxx

  # org/w3c/dom
  src/main/native/org/w3c/dom/Node.cxx)

#
# Create library
#
set(DEEPIS_PLATFORM_LIB ${PROJECT_NAME})

add_library(${DEEPIS_PLATFORM_LIB} STATIC ${DEEPIS_PLATFORM_SRC})

set(DEEPIS_TEST_LIBS ${DEEPIS_PLATFORM_LIB} ${DEEPIS_STANDARD_LIBS})

#
# Export package
#
set(PLATFORM_CONFIG_FILE ${REPOSITORY}/Find${PROJECT_NAME}.cmake)
set(PLATFORM_EXPORTS_FILE ${PROJECT_BINARY_DIR}/Export${PROJECT_NAME}.cmake)

export(TARGETS ${DEEPIS_PLATFORM_LIB} FILE ${PLATFORM_EXPORTS_FILE})
 
file(WRITE ${PLATFORM_CONFIG_FILE} "set(DEEPIS_PLATFORM_INC ${DEEPIS_PLATFORM_INC})\n")
file(APPEND ${PLATFORM_CONFIG_FILE} "set(DEEPIS_PLATFORM_LIB ${DEEPIS_PLATFORM_LIB})\n")
file(APPEND ${PLATFORM_CONFIG_FILE} "include(${PLATFORM_EXPORTS_FILE})\n")

#
# Test
#
add_deep_test(LangTest src/test/native/cxx/lang/TestUnit.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(IoTest src/test/native/cxx/io/TestUnit.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(UtilTest src/test/native/cxx/util/TestUnit.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(MemoryTest src/test/native/cxx/lang/TestMemory.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(RuntimeTest src/test/native/cxx/lang/TestRuntime.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(CompositeUtilTest src/test/native/cxx/util/TestUnitComposite.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(PrimitiveUtilTest src/test/native/cxx/util/TestUnitPrimitive.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(ReentrantReadWriteLockTest src/test/native/cxx/util/concurrent/TestReentrantReadWriteLock.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(FileStoreTest src/test/native/cxx/nio/file/TestFileStore.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(ArrayListTest src/test/native/cxx/util/ArrayListTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(FreeListTest src/test/native/cxx/util/FreeListTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(TestFreeList src/test/native/cxx/util/concurrent/TestFreeList.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(CollectionsTest src/test/native/cxx/util/TestCollections.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(TreeSetTest src/test/native/cxx/util/TreeSetTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(TreeMapTest src/test/native/cxx/util/TreeMapTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(HashMapTest src/test/native/cxx/util/HashMapTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(AtomicTest src/test/native/cxx/util/concurrent/atomic/TestAtomic.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(ConcurrentUtilTest src/test/native/cxx/util/concurrent/TestUnit.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(SynchronizeTest src/test/native/cxx/util/concurrent/TestSynchronize.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(CopyOnWriteArrayListTest src/test/native/cxx/util/concurrent/CopyOnWriteArrayListTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(UserSpaceLockTest src/test/native/cxx/util/concurrent/TestUserSpaceLock.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(CountDownLatchTest src/test/native/cxx/util/concurrent/TestCountDownLatch.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(XMLTest src/test/native/org/w3c/dom/TestXML.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(RegexTest src/test/native/cxx/util/regex/TestRegex.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(PriorityQueueTest src/test/native/cxx/util/PriorityQueueTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(QueueSetTest src/test/native/cxx/util/QueueSetTest.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(UserSpaceReadWriteLockTest src/test/native/cxx/util/concurrent/TestUserSpaceReadWriteLock.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(ReadWriteWriterStarvationTest src/test/native/cxx/util/concurrent/TestReadWriteWriterStarvation.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(NumberRangeSetTest src/test/native/cxx/util/NumberRangeSetTest.cxx ${DEEPIS_TEST_LIBS})

#add_deep_test(FragmentTest src/test/native/cxx/lang/TestFragment.cxx ${DEEPIS_TEST_LIBS})
#add_deep_test(WaitTest src/test/native/cxx/util/concurrent/TestWait.cxx ${DEEPIS_TEST_LIBS})
