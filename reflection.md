## Some strengths of the file format

1. This file format is pretty simple to parse
2. With this file format we can only read the blocks per the attributes that specified in the projection and selection
   clause.
3. It is also possible to filter out the blocks for a given attribute by checking the min/max metrics, a very primitive
   sort of "zone map"

## Some weaknesses of the file format

1. Attributes like `Sex` and `Farm Name` in chicken table have many same value, leave them as it is without compress
   would require more disk/buffer-pool space to hold the data in disk/memory.
2. All the blocks of a single attribute are placed together continuously, in other words, different attributes far from
   each other, this would increase the cost of stitching tuple back together.
3. Min/Max metric is not good for attribute that have many same values, for example the attribute `Sex` in chicken.

## Suggestions for improving the file format

1. We may introduce a block-based compression machinery to reduce the file size on disk and the memory footprint.
2. Store the data of a single attribute into multiple small chunks and group different chunks of attributes like parquet
   row group to make stitch tuple back together more efficient.
3. Support bitmap index as the filter in the block stat might a good idea.

## Other thoughts

Although we are using the columnar file format, the postgres scan executor is assuming a row-major traversal,
for example, the `ExecScanAccessMtd`(in our db721 fdw, it's `ForeignNext`) is called for each `row`
or `TupleTableSlot` in the source code. This assumption limit the power of the columnar storage for example this
row-major traversal might not be cache friendly in the case of columnar file format.
