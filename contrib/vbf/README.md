# Vbf

## Overview
An vbf(append-only) file consists of a number of variable-sized blocks
("varblocks"), one after another. The varblocks are aligned to 4 bytes.
Each block starts with a header. There are multiple kinds of blocks, with
different header information. Each block type begins with common "basic"
header information, however. The different header types are defined in
block_header.h.

## How to Build Vbf

```
./build_vbf.sh --prefix=$GPHOME
```
