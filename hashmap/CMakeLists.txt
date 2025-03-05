cmake_minimum_required(VERSION 3.12)
project(graph_bind)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_SOURCE_DIR}/../dataset)

# 找到 pybind11
find_package(pybind11 REQUIRED)

# 添加 libcuckoo（假设在项目目录下）
include_directories(libcuckoo)

# 添加源文件
add_library(graph_bind MODULE t_graph.cpp)

# 链接 pybind11
target_link_libraries(graph_bind PRIVATE pybind11::module)

# 添加头文件路径
target_include_directories(graph_bind PRIVATE ${CMAKE_SOURCE_DIR})

# 设置生成的 .so 文件名（避免加上 lib 前缀）
set_target_properties(graph_bind PROPERTIES PREFIX "" SUFFIX ".so")

