# :hot_pepper: Paprika

Paprika is a custom implementation for `State` and `Storage` trees of Ethereum. It provides a persistent store solution that is aligned with the Execution Engine API. This means that it's aware of the concepts like blocks, finality, and others, and it leverages them to provide a performant implementation This document covers the main design ideas, inspirations, and most important implementation details.

## Design

Paprika is split into two major components:

1. `Blockchain`
1. `PagedDb`

Additionally, some other components are extracted into as reusable parts:

1. `NibblePath`
1. `SlottedArray`

### Blockchain

`Blockchain` is responsible for handling the new state that is subject to change. The blocks after the merge can be labeled as `latest`, `safe`, and `finalized`. Paprika uses the `finalized` as the cut-off point between the blockchain component and `PagedDb`. The `Blockchain` allows handling the execution API requests such as `NewPayload` and `FCU`. The new payload request is handled by creating a new block on top of the previously existing one. Paprika fully supports handling `NewPayload` in parallel as each block just points to its parent. The following example of creating two blocks that have the same parent shows the possibilities of such API:

```csharp
await using var blockchain = new Blockchain(db);

// create two blocks on top of a shared parent
using var block1A = blockchain.StartNew(parentHash, Block1A, 1);
using var block1B = blockchain.StartNew(parentHash, Block1B, 1);

var account1A = new Account(1, 1);
var account1B = new Account(2, 2);

// set values to the account different for each block
block1A.SetAccount(Key0, account1A);
block1B.SetAccount(Key0, account1B);

// blocks preserve the values set
block1A.GetAccount(Key0).Should().Be(account1A);
block1B.GetAccount(Key0).Should().Be(account1B);
```

It also handles `FCU` in a straight-forward way

```csharp
// finalize the blocks with the given hash that was previously committed
blockchain.Finalize(Block2A);
```

### PagedDb

The `PagedDb` component is responsible for storing the left-fold of the blocks that are beyond the cut-off point. This database uses [memory-mapped files](https://en.wikipedia.org/wiki/Memory-mapped_file) to provide storing capabilities. To handle concurrency, [Copy on Write](https://en.wikipedia.org/wiki/Copy-on-write) is used. This allows multiple concurrent readers to cooperate in a full lock-free manner and a single writer that runs the current transaction. In that manner, it's heavily inspired by [LMBD](https://github.com/LMDB/lmdb).

It's worth to mention that due to the design of the `Blockchain` component, having a single writer available is sufficient. At the same time, having multiple readers allow to create readonly transactions that are later used by blocks from `Blockchain`.

The `PagedDb` component is capable of preserving an arbitrary number of the versions, which makes it different from `LMDB`, `BoltDB` et al. This feature was heavily used before, when all the blocks were immediately added to it. Right now, with readonly transactions and the last blocks handled by the `Blockchain` component, it is not important that much. It might be a subject to change when `Archive` mode is considered.

#### ACID

The database allows 2 modes of commits:

1. `FlushDataOnly`
1. `FlushDataAndRoot`

`FlushDataOnly` allows flushing the data on disk but keeps the root page in memory only. The root page pointing to the recent changes will be flushed the next time. This effectively means that the database preserves the semantics of **Atomic** but is not durable as there's always one write hanging in the air. This mode should be used for greater throughput as it requires only one flushing of the underlying file (`MSYNC` + `FSYNC`).

`FlushDataAndRoot` flushes both, all the data pages and the root page. This mode is not only **Atomic** but also **Durable** as after the commit, the database is fully stored on the disk. This requires two calls to `MSYNC` and two calls to `FSYNC` though, which is a lot heavier than the previous mode. `FlushDataOnly` should be the default one that is used and `FlushDataAndRoot` should be used mostly when closing the database.

#### Memory-mapped caveats

It's worth mentioning that memory-mapped files were lately critiqued by [Andy Pavlo and the team](https://db.cs.cmu.edu/mmap-cidr2022/). The paper's outcome is that any significant DBMS system will need to provide buffer pooling management and `mmap` is not the right tool to build a database. At the moment of writing the decision is to keep the codebase small and use `mmap` and later, if performance is shown to be degrading, migrate.

#### Implementation

The following part provides implementation-related details, that might be helpful when working on or amending the Paprika ~sauce~ source code.

##### Allocations, classes, and objects

Whenever possible initialization should be skipped using `[SkipLocalsInit]` or `Unsafe.` methods.

If a `class` is declared instead of a `struct`, it should be allocated very infrequently. A good example is a transaction or a database that is allocated not that often. When designing constructs created often, like `Keccak` or a `Page`, using the class and allocating an object should be the last resort.

##### Keccak and RLP encoding

Paprika provides custom implementations for some of the operations involving `Keccak` and `RLP` encoding. As the Merkle construct is based on `Keccak` values calculated for Trie nodes that are RLP encoded, Paprika provides combined methods, that first perform the RLP encoding and then calculate the Keccak. This allows an efficient, allocation-free implementation. No `RLP` is used for storing or retrieving data. `RLP` is only used to match the requirements of the Merkle construct.

##### Const constructs and \[StructLayout\]

Whenever a value type needs to be preserved, it's worth considering the usage of `[StructLayout]`, which specifies the placement of the fields. Additionally, the usage of a `Size` const can be of tremendous help. It allows having all the sizes calculated on the step of compilation. It also allows skipping to copy lengths and other unneeded information and replace it with information known upfront.

```csharp
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public struct PageHeader
{
    public const int Size = sizeof(long);
}
```

##### Pages

Paprika uses paged-based addressing. Every page is 4kb. The size of the page is constant and cannot be changed. This makes pages lighter as they do not need to pass the information about their size. The page address, called `DbAddress`, can be encoded within the first 3 bytes of an `uint` if the database size is smaller than 64GB. This leaves one byte for addressing within the page without blowing the address beyond 4 bytes.

There are different types of pages:

1. `RootPage`
1. `AbandonedPage`
1. `DataPage`

###### Root page

The `RootPage` is a page responsible for holding all the metadata information needed to query and amend data. It consists of:

1. batch id - a monotonically increasing number, identifying each batch write to the database that happened
1. block information including its number and the hash
1. an initial fanout of pages (currently set to `256`) - this allows to remove N nibbles from the path (currently: `2`)
1. abandoned pages

The last one is a collection of `DbAddress` pointing to the `Abandoned Pages`. As the amount of metadata is not big, one root page can store over 1 thousand addresses of the abandoned pages.

###### Abandoned Page

An abandoned page is a page storing information about pages that were abandoned during a given batch. Let's describe what abandonment means. When a page is COWed, the original copy should be maintained for the readers. After a given period, defined by the reorganization max depth, the page should be reused to not blow up the database. That is why `AbandonedPage` memoizes the batch at which it was created. Whenever a new page is requested, the allocator checks the list of unused pages (pages that were abandoned that passed the threshold of max reorg depth. If there are some, the page can be reused.

As each `AbandonedPage` can store ~1,000 pages, in cases of big updates, several pages are required to store the addresses of all the abandoned pages. As they share the batch number in which they are abandoned, a linked list is used to occupy only a single slot in the `AbandonedPages` of the `RootPage`. The unlinking and proper management of the list is left up to the allocator that updates slots in the `RootPage` accordingly.

The biggest caveat is where to store the `AbandonedPage`. The same mechanism is used for them as for other pages. This means, that when a block is committed, to store an `AbandonedPage`, the batch needs to allocate (which may get it from the pool) a page and copy to it.

###### Data Page

A data page is responsible for both storing data in-page and providing a fanout for lower levels of the tree. The data page tries to store as much data as possible inline using the [SlottedArray component](#SlottedArray). If there's no more space, left, it selects a bucket, defined by a nibble. The one with the highest count of items is flushed as a separate page and a pointer to that page is stored in the bucket of the original `DataPage`. This is a bit different approach from using page splits. Instead of splitting the page and updating the parent, the page can flush down some of its data, leaving more space for the new. A single `PageData` can hold roughly 50-100 entries. An entry, again, is described in a `SlottedArray`.

##### Page design in C\#

Pages are designed as value types that provide a wrapper around a raw memory pointer. The underlying pointer does not change, so pages can be implemented as `readonly unsafe struct` like in the following example.

```csharp
public readonly unsafe struct Page
{
    private readonly byte* _ptr;
    public Page(byte* ptr) => _ptr = ptr;
}
```

The differentiation of the pages and accessible methods is provided by poor man's derivation - composition. The following snippet presents a data page, that wraps around the generic `Page`.

```csharp
public readonly unsafe struct DataPage
{
    private readonly Page _page;
    public DataPage(Page root) => _page = root;
}
```

The following ASCII Art should provide a better picture of the composition approach

```bash
                Page Header Size, the same for all pages
  start, 0         │
         |         │
         ▼         ▼
         ┌─────────┬────────────────────────────────────────────────────────────────────────────┐
         │ Page    │                                                                            │
Page 4kb │ Header  │                                                                            │
         │         │                                                                            │
         ├─────────┼────────────────────────────────────────────────────────────────────────────┤
         │ Page    │                                                                            │
DataPage │ Header  │                 Payload of the DataPage                                    │
         │         │                                                                            │
         └─────────┴────────────────────────────────────────────────────────────────────────────┘
         │ Page    │                                                                            │
Abandoned│ Header  │                 Payload of the AbandonedPage                               │
         │         │                                                                            │
         └─────────┴────────────────────────────────────────────────────────────────────────────┘
              ▲
              │
              │
              │
          Page Header
          is shared by
          all the pages
```

As fields are located in the same place (`DataPage` wraps `Page` that wraps `byte*`) and all the pages are a size of a `byte*`. To implement the shared functionality, a markup interface `IPage` is used with some extension methods. Again, as pages have the data in the same place they can be cast with the help of `Unsafe`.

```csharp
// The markup interface IPage implemented
public readonly unsafe struct DataPage : IPage
{
    private readonly Page _page;
}

public static class PageExtensions
{
    // The ref to the page is cast to Page, as they underneath are nothing more than a byte* wrappers
    public static void CopyTo<TPage>(this TPage page, TPage destination) where TPage : unmanaged, IPage =>
        Unsafe.As<TPage, Page>(ref page).Span.CopyTo(Unsafe.As<TPage, Page>(ref destination).Span);
}
```

###### Page number

As each page is a wrapper for a pointer. It contains no information about the page number. The page number can be retrieved from the database though, that provides it with the following calculation:

```csharp
private DbAddress GetAddress(in Page page)
{
    return DbAddress.Page((uint)(Unsafe
        .ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
        .ToInt64() / Page.PageSize));
}
```

###### Page headers

As pages may differ by how they use 4kb of provided memory, they need to share some amount of data that can be used to:

1. differentiate the type of the page
2. memoize the last time when the page was written to

The 1st point, the type differentiation, can be addressed either by storying the page type or reasoning about the page place where the page is used. For example, if a page is one of the N pages that support reorganizations, it must be a `RootPage`. Whenever the information can be reasoned out of the context, the type won't be stored to save some memory.

The 2nd point that covers storing important information is stored at the shared page header. The shared `PageHeader` is an amount of memory that is coherent across all the pages. Again, the memory size is const and C\# `const` constructs are leveraged to have it calculated by the compiler and not to deal with them in the runtime.

```csharp
/// <summary>
/// The header shared across all the pages.
/// </summary>
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public struct PageHeader
{
    public const int Size = sizeof(ulong);

    /// <summary>
    /// The id of the last batch that wrote to this page.
    /// </summary>
    [FieldOffset(0)]
    public uint BatchId;

    [FieldOffset(4)]
    public uint Reserved; // for not it's just alignment
}
```

### NibblePath

`NibblePath` is a custom implementation of the path of nibbles, needed to traverse the Trie of Ethereum. The structure allocates no memory and uses `ref` semantics to effectively traverse the path. It also allows for efficient comparisons and slicing. As it's `ref` based, it can be built on top of `Span<byte>`.

### SlottedArray

The `SlottedArray` component is responsible for storing data in-page. It does it by using path-based addressing based on the functionality provided by `NibblePath`. The path is not the only discriminator for the values though. The other part required to create a `Key` is the `type` of entry. Currently, there are the following types of entries:

1. `Account` - identifies `(balance, nonce, codeHash, storageRootHash)` of a given account. The entry type is aligned with `Account` of Ethereum. It uses a more efficient encoding though that requires to be translated to RLP later.
1. `StorageCell` - identifies that the given entry is a storage cell that is kept in-page, alongside other account attributes. To make it unique, the storage cell address is stored as the first 32 bytes of its raw data. It is used for comparisons whenever needed.
1. `StorageTreeRootPageAddress` - the address of the page that holds the root page of the storage tree. It's optional as for accounts with a small number of storage slots occupied, they will be held in-page. Only if an account's storage becomes really large, it is flushed as a separate subtree.
1. `StorageTreeStorageCell` - the storage cell that is kept under a tree with the root of `StorageTreeRootPageAddress`. As the storage cell belongs to a single account (it's in its storage tree), no need to prefix it with the account address. The storage cell address will be used as a nibble path.

For example:

> To store an update of a `nonce` of a given contract, `[address, Type.Account]` pair is used as a `Key` to store the serialized `(balance, nonce, codeHash, storageRootHash)` pair.

and

> To store a storage cell with the given `index` for the given `address`, `[address, Type.StorageCell]` is used as a `Key` with addition of additional bytes of `index`.

The addition of additional bytes breaks the uniform addressing that is based on the path only. It allows at the same time for auto optimization of the tree and much more dense packaging of pages.

#### SlottedArray layout

`SlottedArray` needs to store values with variant lengths over a fixed `Span<byte>` provided by the page. To make it work, Paprika uses a modified pattern of the slot array, used by major players in the world of B+ oriented databases (see: [PostgreSQL page layout](https://www.postgresql.org/docs/current/storage-page-layout.html#STORAGE-PAGE-LAYOUT-FIGURE)). How it works then?

The slot array pattern uses a fixed-size buffer that is provided within the page. It allocates chunks of it from two directions:

1. from `0` forward
2. from the end downward

The first direction, from `0` is used for fixed-size structures that represent slots. Each slot has some metadata, including the most important one, the offset to the start of data. The direction from the end is used to store var length payloads. Paprika diverges from the usual slot array though. The slot array assumes that it's up to the higher level to map the slot identifiers to keys. What the page provides is just a container for tuples that stores them and maps them to the `CTID`s (see: [PostgreSQL system columns](https://www.postgresql.org/docs/current/ddl-system-columns.html)). How Paprika uses this approach

In Paprika, each page level represents a cutoff in the nibble path to make it aligned to the Merkle construct. The key management could be extracted out of the `SlottedArray` component, but it would make it less self-contained. `SlottedArray` then provides `TrySet` and `TryGet` methods that accept nibble paths. This impacts the design of the slot, which is as follows:

```csharp
[StructLayout(LayoutKind.Explicit, Size = Size)]
private struct Slot
{
    public const int Size = 4;

    // The address of this item.
    public ushort ItemAddress { /* bitwise magic */ }

    // Whether the slot is deleted
    public bool IsDeleted { /* bitwise magic */ }

    // the type of the entry
    public DataType Type { /* bitwise magic */ }

    [FieldOffset(0)] private ushort Raw;

    // First 4 nibbles extracted as ushort.
    [FieldOffset(2)] public ushort Prefix;
}
```

The slot is 4 bytes long. It extracts 4 first nibbles as a prefix for fast comparisons. It has a pointer to the item. The length of the item is calculated by subtracting the address from the previous slot address. The drawback of this design is a linear search across all the slots when an item must be found. With the expected number of items per page, which should be no bigger than 100, it gives 400 bytes of slots to search through. This should be ok-ish with modern processors. The code is marked with an optimization opportunity.

With this, the `SlottedArray` memory representation looks like the following.

```bash
┌───────────────┬───────┬───────┬───────────────────────────────┐
│HEADER         │Slot 0 │Slot 1 │                               │
│               │       │       │                               │
│High           │Prefix │Prefix │                               │
│Low            │Addr   │Addr   │ ► ► ►                         │
│Deleted        │   │   │   │   │                               │
│               │   │   │   │   │                               │
├───────────────┴───┼───┴───┼───┘                               │
│                   │       │                                   │
│                ┌──┼───────┘                                   │
│                │  │                                           │
│                │  │                                           │
│                │  └──────────┐                                │
│                │             │                                │
│                ▼             ▼                                │
│                ┌─────────────┬────────────────────────────────┤
│                │             │                                │
│                │             │                                │
│          ◄ ◄ ◄ │    DATA     │             DATA               │
│                │ for slot1   │          for slot 0            │
│                │             │                                │
└────────────────┴─────────────┴────────────────────────────────┘
```

The `SlottedArray` can wrap an arbitrary span of memory so it can be used for any page that wants to store data by key.

### Merkle construct

From Ethereum's point of view, any storage mechanism needs to be able to calculate the `StateRootHash`. This hash allows us to verify whether the block state is valid. How it is done and what is used underneath is not important as long as the store mechanism can provide the answer to the ultimate question: _what is the StateRootHash of the given block?_

The Merkle construct is in the making and it will be updated later.

## Examples

### A small contract

Let's consider a contract `0xABCD` deployed and have some of its storage cells set after performing some of the operations:

1. address: `0xABCD`
1. balance: `0123`
1. nonce: `02`
1. code hash: `0xFEDCBA`
1. storage cells:
   1. `keccak0` -> `0x0A`
   1. `keccak1` -> `0x0B`

As you can see there's no `storageRootHash` as it is calculated from the storage itself. The listing above just gets the data that are set to Paprika. Internally, it will be mapped to a list of entries. Each entry is a mapping between a `key` and an encoded value. The key, besides the `path`, contains the data `type` and an `additional` key if needed. Also, if the number of storage cells is below ~50 (a rough estimate), it will be stored on the same page, increasing the locality. Let's see to which entries the listing above will be mapped. Let's assume that it's the only contract in the state and there are no other pages above this one. If they were, the path would be truncated. Let's encode it!

| Key: Path | Key: Type     | Key: Storage Path | Byte encoded value                                                 |
| --------- | ------------- | ----------------- | ------------------------------------------------------------------ |
| `0xABCD`  | `Account`     | `_`               | (`balance` and `nonce` and `codeHash` and `storage root`)          |
| `0xABCD`  | `StorageCell` | `keccak0`         | `01 0A` (var length, big-endian, number of bytes as prefix)        |
| `0xABCD`  | `StorageCell` | `keccak1`         | `01 0B` (var length, big-endian, number of bytes as prefix)        |
| `0xABCD`  | `Merkle`      | ``                | the root of the Merkle tree for this account (usually, the branch) |
| `0xABCD`  | `Merkle`      | `0`               | the child with nibble `0`                                          |
| `0xABCD`  | `Merkle`      | `1`               | the child with nibble `1`                                          |

A few remarks:

1. `UInt256` is always encoded with `BigEndian`, followed by the truncation of leading zeroes and prefixed with the number of bytes used (the prefix is 1 byte)
1. The navigation is always path-based! Keccaks are stored as described above.
1. For contracts with a small number of storage cells, no separate tree is created, and storage cell values are stored alongside the other data.

### A contract with a huge storage

Let's consider another contract `0xCD`, deployed but wit ha lot of storage cells (thousands to millions)

1. address: `0xCD`
1. balance: `0123`
1. nonce: `02`
1. code hash: `0xFEDCBA`
1. storage cells:
   1. `keccak1` -> `0x00000001`
   1. `keccak2` -> `0x00000002`
   1. more...
   1. `keccakN` -> `0x000FFFFF` (one million)

Which would be stored on one page as:

| Key: Path | Key: Type                    | Key: Additional Key | Byte encoded value                                          |
| --------- | ---------------------------- | ------------------- | ----------------------------------------------------------- |
| `0xCD`    | `Account`                    | `_`                 | (`balance` and `nonce` and `codeHash` and `storage root`)   |
| `0xCD`    | `StorageTreeRootPageAddress` | `_`                 | `02 12 34` (var length big-endian encoding of page `@1234`) |

And on the page under address `@1234`, which is a separate storage trie created for this huge account

| Key: Path | Key: Type                | Key: Additional Key | Byte encoded value             |
| --------- | ------------------------ | ------------------- | ------------------------------ |
| `keccak1` | `StorageTreeStorageCell` | `_`                 | `01 01` (`0x00000001` encoded) |
| `keccak2` | `StorageTreeStorageCell` | `_`                 | `01 02` (`0x00000002` encoded) |
| `keccak3` | `StorageTreeStorageCell` | `_`                 | `01 03` (`0x00000003` encoded) |
| ...       | `StorageTreeStorageCell` | `_`                 | `...`                          |

A few remarks:

1. For contracts with a large number of `StorageCells` a separate tree is created, so that the shared prefix (account address) is stored only in the entry with `StorageTreeRootPageAddress`
1. A separate type `StorageTreeStorageCell` is used to represent an entry in this massive storage trie.
1. An entry of type `StorageTreeRootPageAddress` provides a page address where to jump to find the storage cells of the given account
1. `StorageTreeStorageCells` create a usual Paprika trie, with the extraction of the nibble with the biggest count if needed as a separate page
1. All the entries are stored in [SlottedArray](#SlottedArray), where a small hash `ushort` is used to represent the slot

## Learning materials

1. PostgreSQL
   1. [page layout docs](https://www.postgresql.org/docs/current/storage-page-layout.html)
   1. [bufapge.c implementation](https://github.com/postgres/postgres/blob/master/src/backend/storage/page/bufpage.c)
   1. [hio.c and bufpage usage](https://github.com/postgres/postgres/blob/master/src/backend/access/heap/hio.c)
1. Database Storage lectures by Andy Pavlo from CMU Intro to Database Systems / Fall 2022:
   1. Database Storage, pt. 1 https://www.youtube.com/watch?v=df-l2PxUidI
   1. Database Storage, pt. 2 https://www.youtube.com/watch?v=2HtfGdsrwqA
1. LMBD
   1. Howard Chu - LMDB [The Databaseology Lectures - CMU Fall 2015](https://www.youtube.com/watch?v=tEa5sAh-kVk)
   1. The main file of LMDB [mdb.c](https://github.com/LMDB/lmdb/blob/mdb.master/libraries/liblmdb/mdb.c)
