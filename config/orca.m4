# PGAC_CHECK_ORCA_XERCES
# ---------------------
# Check if the Greenplum patched version of Xerces-C is found
AC_DEFUN([PGAC_CHECK_ORCA_XERCES],
[
AC_CHECK_LIB(xerces-c, strnicmp, [],
  [AC_MSG_ERROR([library xerces-c is required to build with GPORCA])]
)
AC_MSG_CHECKING([[for Xerces-C]])
])# PGAC_CHECK_ORCA_XERCES

