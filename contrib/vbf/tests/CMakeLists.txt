macro(add_vbf_executable name)
    set(source "${ARGV1}")
    if (NOT source)
        set(source "${name}.c")
    endif (NOT source)
    add_executable(${name} ${source})
    target_link_libraries(${name} vbf-static)
endmacro(add_vbf_executable)

macro(add_vbf_test name)
    add_vbf_executable(${name} ${ARGN})
    set(exec_name ${CMAKE_CURRENT_BINARY_DIR}/${name})

    add_test(${name}
        ${CMAKE_COMMAND} -E chdir ${Vbf_SOURCE_DIR}/tests
        ${exec_name}
    )
endmacro(add_vbf_test)

macro(add_vbf_test_checkmem name)
    add_vbf_test(${name} ${ARGN})
    find_program(MEMORYCHECK_COMMAND valgrind )
    if(MEMORYCHECK_COMMAND)
        add_test(memcheck_${name}
            ${CMAKE_COMMAND} -E chdir ${Vbf_SOURCE_DIR}/tests
            ${MEMORYCHECK_COMMAND}
                --log-file=${CMAKE_CURRENT_BINARY_DIR}/memcheck_${name}.log
                --leak-check=full
                --show-reachable=yes
                --error-exitcode=1
                ${exec_name}
        )
    endif(MEMORYCHECK_COMMAND)
endmacro(add_vbf_test_checkmem)

add_vbf_test(test_vbf)
