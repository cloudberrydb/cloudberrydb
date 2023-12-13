set(VBF_SRC
	vbf/allocation.h
	vbf/block_layout.h
	vbf/block_header.h
	vbf/codec.h
	vbf/errors.h
	vbf/input_stream.h
	vbf/output_stream.h
	vbf/platform.h
	vbf/stream_reader.h
	vbf/stream_writer.h
	vbf/vbf_reader.h
	vbf/vbf_writer.h
	vbf/vfs.h
	allocation.c
	block_layout.c
	block_header.c
	codec.c
	errors.c
	input_stream.c
	output_stream.c
	stream_reader.c
	stream_writer.c
	vbf.h
	vbf_private.h
	vbf_reader.c
	vbf_writer.c
	vfs.c
	vfs.h
)

source_group(Vbf FILES ${VBF_SRC})

# The version.sh script gives us a VERSION that uses colon as a
# separator; we need periods.
string(REPLACE ":" "." LIBVBF_DOT_VERSION ${LIBVBF_VERSION})

add_library(vbf-static STATIC ${VBF_SRC})
target_link_libraries(vbf-static ${CODEC_LIBRARIES} ${THREADS_LIBRARIES})
set_target_properties(vbf-static PROPERTIES OUTPUT_NAME vbf)

add_library(vbf-shared SHARED ${VBF_SRC})
target_link_libraries(vbf-shared ${CODEC_LIBRARIES} ${THREADS_LIBRARIES})
set_target_properties(vbf-shared PROPERTIES
        OUTPUT_NAME vbf
        VERSION ${LIBVBF_DOT_VERSION}
        SOVERSION ${LIBVBF_SOVERSION})

install(FILES
        ${CMAKE_CURRENT_SOURCE_DIR}/vbf.h
        DESTINATION include)
install(DIRECTORY
        ${CMAKE_CURRENT_SOURCE_DIR}/vbf
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")

include(GNUInstallDirs)

install(TARGETS vbf-static vbf-shared
	RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
	LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
       )

# Install pkg-config file
set(prefix ${CMAKE_INSTALL_PREFIX})
set(VERSION ${VBF_VERSION})
configure_file(vbf.pc.in vbf.pc)
install(FILES ${CMAKE_CURRENT_BINARY_DIR}/vbf.pc
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/pkgconfig)
