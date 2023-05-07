# Benchmarks

The benchmarks below are a result of running `Paprika.Runner`. The runner is a simple console application that tries its best to provide meaningful, reasonably good scenarios for testing, both in-memory (to check the size and speed with no IO overhead) and with a regular disk-based persistence.

## Benchmark cases

### 50 million accounts

The test writes the following:

- 1000 accounts per block
- 1000 storage slots (one for each created account)
- 50.000 blocks

This gives 50 million accounts in total. Each with one storage slot written.

```
Using in-memory DB for greater speed.
Initializing db of size 18GB
Starting benchmark with commit level FlushDataOnly
Preparing random accounts addresses...
Accounts prepared

(P) - 90th percentile of the value

At Block        | Avg. speed      | Space used      | New pages(P)    | Pages reused(P) | Total pages(P)
           5000 |  771.9 blocks/s |          2.30GB |            1593 |             185 |            1720
          10000 |  672.3 blocks/s |          3.55GB |            1909 |             169 |            1954
          15000 |  634.6 blocks/s |          4.03GB |            2036 |             154 |            2057
          20000 |  581.8 blocks/s |          4.28GB |            2103 |             141 |            2115
          25000 |  610.8 blocks/s |          4.51GB |            2143 |             128 |            2155
          30000 |  595.9 blocks/s |          5.01GB |            2175 |             115 |            2199
          35000 |  535.0 blocks/s |          6.42GB |            2195 |             110 |            2257
          40000 |  503.5 blocks/s |          8.98GB |            2223 |             140 |            2341
          45000 |  471.7 blocks/s |         12.18GB |            2273 |             165 |            2433
          49999 |  464.2 blocks/s |         15.48GB |            2337 |             175 |            2509

Writing state of 1000 accounts per block, each with 1 storage, through 50000 blocks, generated 50000000 accounts, used 15.48GB

Reading and asserting values...
Reading state of all of 50000000 accounts from the last block took 00:00:33.6083939
90th percentiles:
   - new pages allocated per block: 0
   - pages reused allocated per block: 1239
   - total pages written per block: 1244
```
