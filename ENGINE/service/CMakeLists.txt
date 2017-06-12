cmake_minimum_required(VERSION 2.6)

project(deepis-db-service)

set(TOP ${CMAKE_SOURCE_DIR}/../..)

include(${TOP}/build/cmake/Common.cmake)

#
# Import packages
#
find_package(deepis-platform REQUIRED)
find_package(deepis-db-common REQUIRED)

#
# Includes
#
set(DEEPIS_DB_SERVICE_INC ${CMAKE_SOURCE_DIR}/src/main/native)

include_directories(${DEEPIS_STANDARD_INCS} ${DEEPIS_PLATFORM_INC} ${DEEPIS_DB_COMMON_INC} ${DEEPIS_DB_SERVICE_INC})

#
# Source Files
#
set (DEEPIS_DB_SERVICE_SRC
  src/main/native/com/deepis/db/store/relative/core/RealTimeProtocol_v1_0.cxx
  src/main/native/com/deepis/db/store/relative/core/RealTimeMap.cxx
  src/main/native/com/deepis/db/store/relative/core/Properties.cxx
  src/main/native/com/deepis/db/store/relative/util/Versions.cxx
  src/main/native/com/deepis/db/store/relative/util/MapFileUtil.cxx
  src/main/native/com/deepis/db/store/relative/util/BufferedRandomAccessFile.cxx)

#
# Create library
#
set(DEEPIS_DB_SERVICE_LIB ${PROJECT_NAME})

add_library(${DEEPIS_DB_SERVICE_LIB} STATIC ${DEEPIS_DB_SERVICE_SRC})

target_link_libraries(${DEEPIS_DB_SERVICE_LIB} ${DEEPIS_PLATFORM_LIB})

set(DEEPIS_TEST_LIBS ${DEEPIS_DB_SERVICE_LIB} ${DEEPIS_PLATFORM_LIB} ${DEEPIS_STANDARD_LIBS})

#
# Export package
#
set(DB_SERVICE_CONFIG_FILE ${REPOSITORY}/Find${PROJECT_NAME}.cmake)
set(DB_SERVICE_EXPORTS_FILE ${PROJECT_BINARY_DIR}/Export${PROJECT_NAME}.cmake)

export(TARGETS ${DEEPIS_DB_SERVICE_LIB} FILE ${DB_SERVICE_EXPORTS_FILE})

file(WRITE ${DB_SERVICE_CONFIG_FILE} "set(DEEPIS_DB_SERVICE_INC ${DEEPIS_DB_SERVICE_INC})\n")
file(APPEND ${DB_SERVICE_CONFIG_FILE} "set(DEEPIS_DB_SERVICE_LIB ${DEEPIS_DB_SERVICE_LIB})\n")
file(APPEND ${DB_SERVICE_CONFIG_FILE} "include(${DB_SERVICE_EXPORTS_FILE})\n")

#
# Test
#
add_deep_test(IntUnitTest src/test/native/com/deepis/db/store/relative/core/TestUnitInt.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(FileRangeTest src/test/native/com/deepis/db/store/relative/core/TestFileRange.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(StringUnitTest src/test/native/com/deepis/db/store/relative/core/TestUnitString.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(SecondaryUnitTest src/test/native/com/deepis/db/store/relative/core/TestUnitSecondary.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(LockTest src/test/native/com/deepis/db/store/relative/core/TestLock.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(NextPrevTest src/test/native/com/deepis/db/store/relative/core/TestNextPrev.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(PerfTest src/test/native/com/deepis/db/store/relative/core/TestPerf.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(RandomTest src/test/native/com/deepis/db/store/relative/core/TestRandom.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(UpdateTest src/test/native/com/deepis/db/store/relative/core/TestUpdate.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(DeleteTest src/test/native/com/deepis/db/store/relative/core/TestDelete.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(MultiThreadTest src/test/native/com/deepis/db/store/relative/core/TestMultiThread.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(SecondaryDynamicTest src/test/native/com/deepis/db/store/relative/core/TestDynamicSecondary.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(VariableUnitTest src/test/native/com/deepis/db/store/relative/core/TestUnitVariable.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(CompressionTest src/test/native/com/deepis/db/store/relative/core/TestCompression.cxx ${DEEPIS_TEST_LIBS})
add_deep_test(BufferedRandomAccessFileTest src/test/native/com/deepis/db/store/relative/util/TestBufferedRandomAccessFile.cxx ${DEEPIS_TEST_LIBS})

#add_deep_test(FileTest src/test/native/com/deepis/db/store/relative/util/TestMeasuredRandomAccessFile.cxx ${DEEPIS_TEST_LIBS})
#add_deep_test(IsolationTest src/test/native/com/deepis/db/store/relative/core/TestIsolation.cxx ${DEEPIS_TEST_LIBS})
#add_deep_test(TransactionTest src/test/native/com/deepis/db/store/relative/core/TestTransaction.cxx ${DEEPIS_TEST_LIBS})
#add_deep_test(ConcurrentTest src/test/native/com/deepis/db/store/relative/core/TestConcurrent.cxx ${DEEPIS_TEST_LIBS})
#add_deep_test(MemoryGrowthTest src/test/native/com/deepis/db/store/relative/core/TestMemoryGrowth.cxx ${DEEPIS_TEST_LIBS})
