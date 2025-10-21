enable_testing()

find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

add_executable(test_c test_c.c)
target_link_libraries(test_c PRIVATE hw2_internal pthread GTest::gtest GTest::gtest_main)
target_include_directories(test_c PRIVATE ../src)
add_test(NAME test_c COMMAND test_c)