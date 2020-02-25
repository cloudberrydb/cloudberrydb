include_directories(include)

include_directories(${PROJECT_SOURCE_DIR}/libgpopt/include/)
include_directories(${PROJECT_SOURCE_DIR}/libnaucrates/include/)

file(GLOB_RECURSE hdrs ${CMAKE_CURRENT_SOURCE_DIR}/include/*.h)
file(GLOB_RECURSE srcs ${CMAKE_CURRENT_SOURCE_DIR}/src/*.cpp)

# Add headers to make them visible in some IDEs (Clion, VS, Xcode)
list(APPEND srcs ${hdrs})

add_library(gpos ${srcs})

target_link_libraries(gpos ${CMAKE_DL_LIBS})

# Tests reside in the 'server' subdirectory.
add_subdirectory(server)
