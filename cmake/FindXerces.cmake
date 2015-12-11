# Module to find Xerces-C.
# We specifically try to find the Greenplum patched version of Xerces.

find_path(XERCES_INCLUDE_DIR xercesc/sax2/DefaultHandler.hpp)

find_library(XERCES_LIBRARY NAMES xerces-c libxerces-c)

set(XERCES_LIBRARIES ${XERCES_LIBRARY})
set(XERCES_INCLUDE_DIRS ${XERCES_INCLUDE_DIR})

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Xerces DEFAULT_MSG
                                  XERCES_LIBRARY XERCES_INCLUDE_DIR)

if (XERCES_FOUND)
  # Check for patched Xerces. DOMImplementationList is a subclass of XMemory in
  # the Greenplum version of Xerces, but not (yet) in upstream.
  set(CMAKE_REQUIRED_INCLUDES
      "${XERCES_INCLUDE_DIRS} ${CMAKE_REQUIRED_INCLUDES}")
  include(CheckCXXSourceCompiles)
  check_cxx_source_compiles("
#include <stddef.h>
#include \"xercesc/util/XMemory.hpp\"
#include \"xercesc/dom/DOMImplementationList.hpp\"

int main() {
  xercesc::DOMImplementationList* derived_ptr = NULL;
  xercesc::XMemory* base_ptr = derived_ptr;
  return (base_ptr == derived_ptr) ? 0 : 1;
}
" XERCES_IS_PATCHED)
  if (NOT XERCES_IS_PATCHED)
    message(FATAL_ERROR "Found Xerces-C, but your installed version doesn't "
                        "appear to be patched for compatibility with Greenplum. "
                        "If a patched version of Xerces is installed alongside "
                        "the upstream version, please explicitly specify it "
                        "with XERCES_INCLUDE_DIR and XERCES_LIBRARY.")
  endif()
endif()

mark_as_advanced(XERCES_INCLUDE_DIR XERCES_LIBRARY)
