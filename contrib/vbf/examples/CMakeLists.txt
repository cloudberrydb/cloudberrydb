add_executable(quickstart quickstart.c)
target_link_libraries(quickstart vbf-static)

set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/quickstart)

add_test(quickstart
    ${CMAKE_COMMAND} -E chdir ${Vbf_SOURCE_DIR}/examples
    ${exec_name}
)
