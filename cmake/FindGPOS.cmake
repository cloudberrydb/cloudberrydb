# Module to find GPOS.

find_path(GPOS_INCLUDE_DIR gpos/base.h)

find_library(GPOS_LIBRARY NAMES gpos libgpos)

set(GPOS_LIBRARIES ${GPOS_LIBRARY})
set(GPOS_INCLUDE_DIRS ${GPOS_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(GPOS DEFAULT_MSG
                                  GPOS_LIBRARY GPOS_INCLUDE_DIR)

mark_as_advanced(GPOS_INCLUDE_DIR GPOS_LIBRARY)
