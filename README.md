# :hot_pepper: Paprika

Paprika provides a custom implementation of the Patricia tree used in Ethereum. It aims at delivering the following:

1. no external KV storage
1. no managed memory overhead - Paprika uses almost no managed memory as only `Span<byte>`, stackallocated variables and uses no managed representation of the tree
1. atomic commit
1. the history of N-th last versions of the db

**No external KV storage** means that Paprika is self-sufficient in regards to writing and reading data from disk and requires no third-party databases like `RocksDB` or others.

**No managed memory overhead** means that Paprika uses almost no managed memory. Only `Span<byte>` and stackallocated variables are used. There's no representation of the tree stored in the managed memory. Also, it uses `[module: SkipLocalsInit]` to not clean any memory before accessing it.

**Atomic commit** means atomic in the meaning of ACID. If there's a failure during the commit the database shall not be corrupted and shall represent the previous state.

**History** is preserved using additional metadata pages, that keep the root and the pages that were marked as abandoned.

## Design

Visit [docs](/docs) for further information.

## Benchmarks

After the redesign to be page-oriented, the benchmarks use bigger numbers

1. [160 million pairs](#160-millions-of-pairs), written in a single transaction, which shall simulate the initial population/sync

In each case, the key is 32 bytes long. The value is 32 bytes long as well.

### General considerations

#### Memory Profiling

Almost no managed memory is used

![image](https://user-images.githubusercontent.com/519707/204166299-81c05582-7e0d-4401-b2cf-91a3c1b7153b.png)

### 160 million pairs

Writing 160 million 32bytes -> 32bytes mappings

```
Wrote 10,000,000 items, DB usage is at 5.01% which gives 1.00GB out of allocated 20GB
Wrote 20,000,000 items, DB usage is at 9.85% which gives 1.97GB out of allocated 20GB
Wrote 30,000,000 items, DB usage is at 11.41% which gives 2.28GB out of allocated 20GB
Wrote 40,000,000 items, DB usage is at 15.05% which gives 3.01GB out of allocated 20GB
Wrote 50,000,000 items, DB usage is at 18.85% which gives 3.77GB out of allocated 20GB
Wrote 60,000,000 items, DB usage is at 21.10% which gives 4.22GB out of allocated 20GB
Wrote 70,000,000 items, DB usage is at 24.86% which gives 4.97GB out of allocated 20GB
Wrote 80,000,000 items, DB usage is at 28.02% which gives 5.60GB out of allocated 20GB
Wrote 90,000,000 items, DB usage is at 30.84% which gives 6.17GB out of allocated 20GB
Wrote 100,000,000 items, DB usage is at 34.42% which gives 6.88GB out of allocated 20GB
Wrote 110,000,000 items, DB usage is at 37.41% which gives 7.48GB out of allocated 20GB
Wrote 120,000,000 items, DB usage is at 40.52% which gives 8.10GB out of allocated 20GB
Wrote 130,000,000 items, DB usage is at 43.89% which gives 8.78GB out of allocated 20GB
Wrote 140,000,000 items, DB usage is at 46.91% which gives 9.38GB out of allocated 20GB
Wrote 150,000,000 items, DB usage is at 50.14% which gives 10.03GB out of allocated 20GB
Writing of 160,000,000.00 items took 00:08:19.8204173 giving a throughput 320,114.00 items/s

Committing to disk took 00:00:10.4085680

Reading of 160,000,000.00 items took 00:08:24.8362414 giving a throughput 316,934.00 items/s
```
