cmake_minimum_required(VERSION 3.16)
project(os_hw2 LANGUAGES C CXX)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
add_compile_options(-Wall -Wextra -Wpedantic -Wno-unused-parameter)

option(TSAN "Enable ThreadSanitizer (TSan)" OFF)

if(TSAN)
    message(STATUS "Building with ThreadSanitizer enabled")
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer)
    add_link_options(-fsanitize=thread)
endif()


# --- internal library with all implementation files ---
add_library(hw2_internal STATIC
    src/pthread_demo.c
    src/prod_cons.c
    src/char_stat.c
)
target_include_directories(hw2_internal PRIVATE src)
target_link_libraries(hw2_internal pthread)

# --- public executable ---
add_executable(os_hw2 main.c)
target_link_libraries(os_hw2 PRIVATE hw2_internal)

# --- Tests ---
enable_testing()
add_subdirectory(tests)
