cmake_minimum_required(VERSION 3.10)

project(word_count)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)

add_compile_options(-Wall -Wextra -O2)

set(SOURCES
        src/prod_cons.c
)

add_executable(word_count ${SOURCES})
