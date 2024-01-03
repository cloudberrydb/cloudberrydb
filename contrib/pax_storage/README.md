# Micro Partition

## Overview

- Storage
  - Provides efficient data access services for the computing layer, which is more conducive to the data format processed by the computing layer
  - Multiple data format support, data openness (third-party tools can be directly read through files)
- Cache
  - Provides consistent caching for external table 
  - Cache consistency
- Dynamic expansion/contraction (second level)
- Compatible with CBDB non-cloud version code
 

## Configuration and build

You **must** enable the pre-push hooks to automatically check format:

```
cp .githooks/* .git/hooks/`
```

Also before `git push`
```
clang-format -i {your changed code}
```

### Build PAX

1. make sure you have already build and install `cbdb` in your env
2. already `source greenplum_path.sh`
3. follow below steps

```
git submodule update --init

mkdir build
cd build
cmake .. 
make -j 
```

### Build GTEST

1. make sure already build pax with cmake option `-DBUILD_GTEST=on`, default value is on
2. better with debug cmake option `-DENABLE_DEBUG=on`, default value is on
3. run tests

```
cd build
./src/cpp/test_main
```

### Build extension

1. After build PAX, `pax.so` will be generated in `src/data`
2. follow below steps

```
cd src/data
make install -j
make installcheck
```
Before starting working with Pax, adding the following line to postgresql.conf is required. This change requires a restart of the PostgreSQL database server.
```
shared_preload_libraries = 'pax.so'
```

## GTEST accesses internal functions/variables

1. Using marco `RUN_GTEST` to make protected/private functions/variables public.
ex. 

**obj.h**:

```
class A {
  public:
    function a();

#ifndef RUN_GTEST
  protected:
#endif 
    function b();

#ifndef RUN_GTEST
  private:
#endif
    int c;
}
```

**obj_test.cc**:

```
#include "obj.h"

TEST_F(Example, test) {
  A a;
  a.a(); // access public function
  a.b(); // access protected function
  a.c; // access private variables
}
```

3. Will generate temp file in disk? 
- use relative paths
- generate temporary files in `SetUp` and delete files in `TearDown`
  - if generated files in test body, please delete it at the end of the test
- please make sure that no junk files remain after `gtest`

3. Using `gmock` to mock a class

### exception && try catch

There are two way to throw a exception
1. `CBDB_RAISE(ExType)`: direct throw
2. `CBDB_CHECK(check_exp, ExType)`: check failed, then throw

About try catch, you need to know 
  1. Expected exceptions, catcher can handle it.
    - Do not `rethrow` it
    - Should do `try...catch` to handle it
    - Better not to write logic in `try...catch`, but use the return value to cover the logic
    - like: network problem...
  2. Unexpected exceptions
    - Thinking should we add `try...catch` to handle it?
      - have some global resources must do `try...catch` to release it
        - memory in top memory context or session memory context
        - opened fd
        - static resources
      - do not have any global resources
        - just throw it without `try...catch`
    - like: logic error, out of range error...
  3. Do not use `catch(...)` in c++ code
    - expect access method layer
    - used `catch(...)` in below access method will drop the current stack/tracker.

About `ereport/elog(ERROR)`, you need to know
  1. Better not use `ereport/elog(ERROR)` in c++ code
    - can not use `try...catch` handle it
    - make sure resource have been clean up before call `ereport/elog(ERROR)`
  2. use it as a panic

example here:
1. Expected exceptions

```
void RequestResources(bool should_retry) {
  RequestTempArgs args;

  // have some alloc in it
  InitRequestTempArgs(&args); 

  try {
    DoHttpRequest();
  } catch {
    // free the resource and retry 
    DestroyRequestTempArgs(&args);
    if (should_retry) {
      RequestResources(false);
    }
  }
}

```

2. Unexpected exceptions with global resources

```
static ReadContext *context;

void InitReadContext() {
  context = palloc(TopMemoryContext, sizeof(ReadContext));
}

void ReadResources() {
  Assert(context);
  try {
    ReadResource(context);
  } catch {
    // should destroy global resource
    // otherwise, will got resource leak in InitReadContext()
    DestroyReadContext(context);
    throw CurrentException();
  }
}
```

3. Unexpected exceptions without global resources

```
void ParseResource(Resource * res, size_t offset) {
  // direct throw without any try...catch
  CBDB_CHECK(offset > res->size(), kExTypeOutOfRange);
  ... // normal logic
}
```

### Others

1. please change all `auto *` to `auto`
  - notice that `auto` and `auto &` are different
2. split logic code into `.cc`
  - should not add logic code(logic in class) into `.h`, `clang-format`/`cpplint` won't detect it 
  - except `inline`/`static` method
3. don't make the constructor too bloated
  - some parameters can be passed through `Set`
    - in the method starting with `Set`, if necessary, add a `Assert` to ensure that the parameter is only passed once
  - consider using a factory method to construct the object
