cmake_minimum_required(VERSION 3.5)

project(BigSort LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(BigSort main.cpp)
target_link_libraries(BigSort stdc++fs)
target_compile_features(BigSort PUBLIC cxx_std_17)
