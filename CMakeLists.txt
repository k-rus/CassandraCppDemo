cmake_minimum_required(VERSION 3.0.0)
project(cassandracppdemo C CXX)

include_directories(/usr/local/include)
link_directories(/usr/local/lib)
add_executable(cassandracppdemo main.cpp)
target_link_libraries(cassandracppdemo /usr/local/lib/libcassandra.dylib)
