cmake_minimum_required(VERSION 3.5)
project(palmtree)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

set(SOURCE_FILES
    main.cpp
    palmtree.h)

add_executable(palmtree ${SOURCE_FILES})