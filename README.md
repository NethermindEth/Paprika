# :hot_pepper: Paprika

A low level Patricia tree, with no indirect db abstraction. The implementation should be suitable to replace a state tree of Ethereum blockchain.

This project removes the abstraction of the underlying storage and make Patricia tree work directly with data. This means, that all node types: branches, extensions (yet to be implemented) and leafs use internal addressing, requiring no additional database to get the data or write them.

Additionally, this means that Paprika uses **almost no managed memory**, delegating everything to the underlying store, based on memory mapped files. There are no managed nodes, no cache and no encoding/decoding. Just key-value pairs stored in a structure directly mapped to disk. 

The database assumes no deletes and keeping the last root in the memory. If deletes or pruning is required, it can be added later on. For example, by copying only values that are alive. The roots can be captured and stored elsewhere. They map to a single long and one can easily amend paprika to get old data by its root.

The current implementation **does not include**:

- extension nodes
- prunning
- Keccak compute and storying it
- RLP encoding

## Benchmarks

The following scenarios were used for benchmarking and are presented in the order of running them at various points of time. The latest should be used to get final insights and the rest should be treated as a history.

1. [15 millions of pairs](#15-millions-of-pairs), written one by one, each commiting a new root
1. [50 millions of pairs](#50-millions-of-pairs), written one by one, each commiting a new root
1. [80 millions of pairs](#80-millions-of-pairs), written in batches of 10000, which is meant to similuate the block
1. [80 millions of pairs but BETTER](#80-millions-of-pairs-updated), written in batches of 10000, which is meant to similuate the block

In each case the key is 32 bytes long. The value is 32 bytes long as well.

### General considerations

#### Memory Profiling

Almost no managed memory used

![image](https://user-images.githubusercontent.com/519707/204166299-81c05582-7e0d-4401-b2cf-91a3c1b7153b.png)

#### Performance profiling

Some potential for extracting scope when writing items down

![image](https://user-images.githubusercontent.com/519707/204166363-afe54fec-d772-49ff-9d63-0bf7571b4294.png)

### 15 millions of pairs

- Writing of 15 millions of items took 00:00:25.1832635
- Reading of 15 millions of items took 00:00:23.9572717

### 50 millions of pairs

50 millions of pairs 32b key + 32b value

- disk size (no prunning, every single root of tree accesible): 40GB
- Writing of 50000000 items took 00:06:18.9389653
- Reading of 50000000 items took 00:08:58.9064186

Which gives ~83_000 read/writes per second

Distribution of space in files on disk shows some potential on copy on write, while undersaturated files would be copied forward.

```
File 00000 is used by the current root at 0%
File 00001 is used by the current root at 0%
File 00002 is used by the current root at 0%
File 00003 is used by the current root at 0%
File 00004 is used by the current root at 0%
File 00005 is used by the current root at 0%
File 00006 is used by the current root at 0%
File 00007 is used by the current root at 0%
File 00008 is used by the current root at 0%
File 00009 is used by the current root at 0%
File 00010 is used by the current root at 0%
File 00011 is used by the current root at 0%
File 00012 is used by the current root at 11%
File 00013 is used by the current root at 13%
File 00014 is used by the current root at 13%
File 00015 is used by the current root at 13%
File 00016 is used by the current root at 13%
File 00017 is used by the current root at 13%
File 00018 is used by the current root at 13%
File 00019 is used by the current root at 13%
File 00020 is used by the current root at 13%
File 00021 is used by the current root at 13%
File 00022 is used by the current root at 13%
File 00023 is used by the current root at 13%
File 00024 is used by the current root at 13%
File 00025 is used by the current root at 13%
File 00026 is used by the current root at 12%
File 00027 is used by the current root at 10%
File 00028 is used by the current root at 10%
File 00029 is used by the current root at 10%
File 00030 is used by the current root at 10%
File 00031 is used by the current root at 10%
File 00032 is used by the current root at 10%
File 00033 is used by the current root at 10%
File 00034 is used by the current root at 10%
File 00035 is used by the current root at 10%
File 00036 is used by the current root at 10%
File 00037 is used by the current root at 10%
File 00038 is used by the current root at 13%
File 00039 is used by the current root at 55%
```

### 80 millions of pairs

80 millions of pairs, written in batches of 10000, which is meant to similuate the block. Writing in bulks uses top upgradable nodes. Upgradable nodes are overwritten so that no new nodes are allocated for branches with child count 16 (top ones). This greatly reduces the amount of litter and is more aligned with a nature of the blockchain.

- Writing of 80,000,000.00 items with batch of 10000 took 00:04:36.7785626 giving a throughput 289,039.00 items/s
- Reading of 80,000,000.00 items with batch of 10000 took 00:05:35.1633116 giving a throughput 238,689.00 items/s

The number of files is much lower! This is due to committing only at the batch end.


```
File 00000 is used by the current root at 0%
File 00001 is used by the current root at 0%
File 00002 is used by the current root at 0%
File 00003 is used by the current root at 0%
File 00004 is used by the current root at 1%
File 00005 is used by the current root at 27%
File 00006 is used by the current root at 27%
File 00007 is used by the current root at 27%
File 00008 is used by the current root at 27%
File 00009 is used by the current root at 27%
File 00010 is used by the current root at 27%
File 00011 is used by the current root at 27%
File 00012 is used by the current root at 16%
File 00013 is used by the current root at 15%
File 00014 is used by the current root at 15%
File 00015 is used by the current root at 15%
File 00016 is used by the current root at 15%
File 00017 is used by the current root at 15%
File 00018 is used by the current root at 15%
File 00019 is used by the current root at 15%
File 00020 is used by the current root at 15%
File 00021 is used by the current root at 15%
File 00022 is used by the current root at 15%
File 00023 is used by the current root at 21%
File 00024 is used by the current root at 23%
File 00025 is used by the current root at 24%
File 00026 is used by the current root at 24%
File 00027 is used by the current root at 24%
File 00028 is used by the current root at 24%
File 00029 is used by the current root at 58%
```

### 80 millions of pairs updated

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
