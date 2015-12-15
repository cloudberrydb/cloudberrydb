// Copyright (c) 2015, Pivotal Software, Inc.

#include <stdio.h>

#include "gpos/version.h"

int main() {
  printf("%d.%d", GPOS_VERSION_MAJOR, GPOS_VERSION_MINOR);
  return 0;
}
