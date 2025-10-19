cmake_minimum_required(VERSION 3.10)

project(word_count)
project(char_stat)

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED True)

add_compile_options(-Wall -Wextra -O2)

set(WORD_COUNT_SOURCES
        src/prod_cons.c
)
set(CHAR_STAT_SOURCES
        src/char_stat.c
)

add_executable(word_count ${WORD_COUNT_SOURCES})
add_executable(char_stat ${CHAR_STAT_SOURCES})
