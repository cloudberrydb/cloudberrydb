# Linux kernel source indent format options
set(INDENT_OPTS -nbad -bap -nbc -bbo -hnl -br -brs -c33 -cd33 -ncdb -ce -ci4
    -cli0 -d0 -di1 -nfc1 -i8 -ip0 -l80 -lp -npcs -nprs -npsl -sai
    -saf -saw -ncs -nsc -sob -nfca -cp33 -ss -ts8)

foreach(dir src tests examples)
	exec_program(indent
                 ARGS ${INDENT_OPTS} ${CMAKE_CURRENT_SOURCE_DIR}/../${dir}/*.[c,h]
                 OUTPUT_VARIABLE indent_output
                 RETURN_VALUE ret)
    message(STATUS ${indent_output})
endforeach()

