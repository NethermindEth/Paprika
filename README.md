# :hot_pepper: Paprika

Paprika is an imeplementation of the Patricia tree used in Ethereum. It has the following key properties:

1. no external KV storage
1. no managed memory overhead - Paprika uses almost no managed memory as only `Span<byte>`, stackallocated variables and uses no managed representation of the tree
1. different commit options - depending on the commit level, it can update values in place (for initial blocks for example)

**No external KV storage** means that Paprika is selfsufficient in regards to writing and reading data from disk and requires no third party dbs like Rocks or others.

**No managed memory overhead** means that Paprika uses almost no managed memory. Only `Span<byte>`, stackallocated variables are used. There's no representation of the tree stored in the managed memory. Also, it uses `[module: SkipLocalsInit]` to not clean any memory before accessing it.

**Different commit options** mean that depending on the level of the commit of the batch, Paprika can:

1. write almost in place, just updating the root. It does by reusing nodes that were decomissioned by splits etc.
1. commit a whole batch, making it readonly (useful to commit a block so that it's readable later)
1. flush everything to disk (forces to flush, making the previous fully persistent)

The database assumes no deletes and keeping the last root in the memory. If deletes or pruning is required, it can be added later on. For example, by copying only values that are alive. The roots can be captured and stored elsewhere. They map to a single long and one can easily amend paprika to get old data by its root.

## Design

Some design principles:

1. Paprika uses `memory mapped files` to store and read data. 
1. For writing data, it writes them to the first empty space of the given size (unaligned) and maps it to a single `long`.
1. `long` identifier is used for internal addressing in the tree.
1. `Keccak` or `RLP` of the node is stored outside of tree as it can be always recalculated if needed and having it memmapped makes the files tooo big.

## Benchmarks

The following scenarios were used for benchmarking and are presented in the order of running them at various points of time. The latest should be used to get final insights and the rest should be treated as a history.

1. [80 millions of pairs](#80-millions-of-pairs), written in batches of 10000, which is meant to similuate the block
1. [160 millions of pairs](#160-millions-of-pairs), written in batches of 10000, which is meant to similuate the block

In each case the key is 32 bytes long. The value is 32 bytes long as well.

### General considerations

#### Memory Profiling

Almost no managed memory used

![image](https://user-images.githubusercontent.com/519707/204166299-81c05582-7e0d-4401-b2cf-91a3c1b7153b.png)

#### Performance profiling

Some potential for extracting scope when writing items down

![image](https://user-images.githubusercontent.com/519707/204166363-afe54fec-d772-49ff-9d63-0bf7571b4294.png)


### 80 millions of pairs

The latest run with simplified and upgraded batch handling. Now every chunk of memory that is written to the database within a batch is updatable. This benefits scenario when a leaf node is promoted to a branch. Leaf has a lot of space and can provide for branches. Additionally a new `NibblePath` used with `ref byte` field to make the paths analysis even faster from `Span<>`. The introduction of the `NibblePath` will help with extensions a lot.

- **70% of the disk size reduction** to the previous benchmarked version of Paprika
- **writing 1,500,000 pairs per second**
- **reading 1,700,000 pairs per second**

Writing of 80,000,000.00 items with batch of 10000 took 00:00:50.6045946 giving a throughput 1,580,884.00 items/s
Reading of 80,000,000.00 items with batch of 10000 took 00:00:44.9487146 giving a throughput 1,779,806.00 items/s

```
File 00000 is used by the current root at 0%
File 00001 is used by the current root at 0%
File 00002 is used by the current root at 70%
File 00003 is used by the current root at 76%
File 00004 is used by the current root at 70%
File 00005 is used by the current root at 61%
File 00006 is used by the current root at 57%
File 00007 is used by the current root at 71%
File 00008 is used by the current root at 86%
File 00009 is used by the current root at 99%
```

### 160 millions of pairs

Clearly, memmapping is beyond memory size and requires some reads that impacts the speed

- **writing 230,000 pairs per second**
- **reading 300,000 pairs per second**
- 14GB of data
