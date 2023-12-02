> [!IMPORTANT]
> Thanks to the [CMU Database Group](https://db.cs.cmu.edu/) for making these awesome public courses.

## background

This is an implementation of [CMU 15-721 SPRING 2023 project 1](https://15721.courses.cs.cmu.edu/spring2023/project1.html).
The code here is the `cmudb/extensions/db721_fdw` dir of this [repo](https://github.com/cmu-db/postgres/tree/2023-S721-P1)

> [!NOTE]
> At the year of 2023, I'm putting this code here as my reference, if the course of [CMU-15721](https://15721.courses.cs.cmu.edu/spring2024/) in the coming year continues using this same project, I will remove this repo immediately to avoid any inconvenient.


## db721 format

db721 is our custom columnar file format. Each table is stored with its metadata in its own .db721 file. The format is simple, consisting of only three components:

```
[Raw Data] [JSON Metadata] [JSON Metadata Size (4 bytes)]
```

To read this format, first you read the last 4 bytes to figure out how big the JSON metadata is. Then, you read the JSON metadata, which describes how to access the raw data:

```
metadata["Table"]: the table name (string)

metadata["Max Values Per Block"]: the maximum number of values in each block (int)

metadata["Columns"]: the table's columns (JSON dict)
    Keys: column names (string)
    Values: column data, see below (JSON dict)

metadata["Columns"]["Column Name"]: column data (JSON dict)
    Keys:
    "type": the column type (str), possible values are:
        "float" | "int" | "str"

    "start_offset": the offset in the file for the first block of this column (int)

    "num_blocks": the number of blocks for this column (int)

    "block_stats": statistics for the 0-indexed fixed-size blocks (JSON dict)
        Keys: block index (string)
        Values: statistics for the corresponding block (JSON dict)
            Keys:
            "num": the number of values in this block (int)
            "min": the minimum value in this block (same type as column)
            "max": the maximum value in this block (same type as column)
            "min_len": only exists for str column; the min length of a string in this block (int)
            "max_len": only exists for str column; the max length of a string in this block (int)

Within [Raw Data], the way values are written depends on their type.
    "float": written as four byte floats, [ float1 float2 float3 ... ]
    "int": written as four byte integers, [ int1 int2 int3 ... ]
    "str": written as 32-byte fixed-length null-terminated ASCII strings
```

Note that:

The metadata is written at the end of the file.
(Optional) Parquet also does this. Think about why; what does this optimize for?
Column data is stored as a contiguous array of fixed-size blocks.
NOTE: for 2023's project 1, every block is guaranteed to pack "Max Values Per Block" values per block, except for the last block which has the remainder.
The metadata contains block statistics to help you to decide whether you should read a block.
We encourage you to view a hex dump of the .db721 files using tools like xxd.

Note that this format has many shortcomings, but it is enough to allow for basic optimizations. For example, you can choose to only access the columns that you need, and you can choose to skip reading blocks if the min/max ranges are filtered by predicates. We encourage you to think about tradeoffs and improvements as you write code for this project, and we will ask you to document some of these thoughts in a .md file with your submission.
