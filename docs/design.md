# :hot_pepper: Paprika

Paprika is a custom database implementation for `State` and `Storage` trees of Ethereum. This document covers the main design ideas, inspirations and most important implementation details.

## Design

Paprika is a database that uses [memory-mapped files](https://en.wikipedia.org/wiki/Memory-mapped_file). To handle concurrency, [Copy on Write](https://en.wikipedia.org/wiki/Copy-on-write) is used. This allows multiple concurrent readers to cooperate in a full lock-free manner and a single writer that runs the current transaction. In that manner, it's heavily inspired by [LMBD](https://github.com/LMDB/lmdb). Paprika uses 4kb pages.

### Reorganizations handling

The foundation of any blockchain is a single list of blocks, a chain. The _canonical chain_ is the chain that is perceived to be the main chain. Due to the nature of the network, the canonical chain changes in time. If a block at a given position in the list is replaced by another, the effect is called a _reorganization_. The usual process of handling a reorganization is undoing recent N operations, until the youngest block was changed, and applying new blocks. From the database perspective, it means that it needs to be able to undo an arbitrary number of committed blocks. 

In Paprika, it's handled by specifying the _history depth_ that keeps the block information for at least _history depth_. This allows undoing blocks till the reorganization boundary and applying blocks from the canonical chain. Paprika internally keeps the _history depth_ of last root pages. If a reorganization is detected and the state needs to be reverted to any of the last _history depth_, it copies the root page with all the metadata as current and allows to re-build the state on top of it. Due to its internal page reuse, as the snapshot of the past restored, it also logically undoes all the page writes etc. It works as a clock that is turned back by a given amount of time (blocks).

### Merkle construct

Paprika focuses on delivering fast reads and writes, keeping the information about `Merkle` construct in separate pages. This allows to load and access of pages with data in a fast manner, leaving the update of the root hash (and other nodes) for the transaction commit. It also allows choosing which Keccaks are memoized. For example, the implementor may choose to store every other level of Keccaks.

### ACID

Paprika allows 2 modes of commits:

1. `FlushDataOnly`
1. `FlushDataAndRoot`

`FlushDataOnly` allows flushing the data on disk but keeps the root page in memory only. The root page pointing to the recent changes will be flushed the next time. This effectively means that the database preserves the semantics of **Atomic** but is not durable as there's always one write hanging in the air. This mode should be used for greater throughput as it requires only one flushing of the underlying file (`MSYNC` + `FSYNC`).

`FlushDataAndRoot` flushes both, all the data pages and the root page. This mode is not only **Atomic** but also **Durable** as after the commit, the database is fully stored on the disk. This requires two calls to `MSYNC` and two calls to `FSYNC` though, which is a lot heavier than the previous mode. `FlushDataOnly` should be the default one that is used and `FlushDataAndRoot` should be used mostly when closing the database.

### Memory-mapped caveats

It's worth mentioning that memory-mapped files were lately critiqued by [Andy Pavlo and the team](https://db.cs.cmu.edu/mmap-cidr2022/). The paper's outcome is that any significant DBMS system will need to provide buffer pooling management and `mmap` is not the right tool to build a database. At the moment of writing the decision is to keep the codebase small and use `mmap` and later, if performance is shown to be degrading, migrate.

## Implementation

The following part provides implementation-related details, that might be helpful when working on or amending the Paprika ~sauce~ source code.

### Allocations, classes and objects

Whenever possible initialization should be skipped using `[SkipLocalsInit]` or `Unsafe.` methods.

If a `class` is declared instead of a `struct`, it should be allocated very infrequently. A good example is a transaction or a database that is allocated not that often. When designing constructs created often, like `Keccak` or a `Page`, using the class and allocating an object should be the last resort.

### NibblePath

`NibblePath` is a custom implementation of the path of nibbles, needed to traverse the Trie of Ethereum. The structure allocates no memory and uses `ref` semantics to effectively traverse the path. It also allows for efficient comparisons and slicing. As it's `ref` based, it can be built on top of `Span<byte>`.

### Keccak and RLP encoding

Paprika provides custom implementations for some of the operations involving `Keccak` and `RLP` encoding. As the Merkle construct is based on `Keccak` values calculated for Trie nodes that are RLP encoded, Paprika provides combined methods, that first perform the RLP encoding and then calculate the Keccak. This allows an efficient, allocation-free implementation. No `RLP` is used for storing or retrieving data. `RLP` is only used to match the requirements of the Merkle construct.

### Const constructs and \[StructLayout\]

Whenever a value type needs to be preserved, it's worth considering the usage of `[StructLayout]`, which specifies the placement of the fields. Additionally, the usage of a `Size` const can be of tremendous help. It allows having all the sizes calculated on the step of compilation. It also allows skipping to copy lengths and other unneeded information and replace it with information known upfront.

```csharp
[StructLayout(LayoutKind.Explicit, Pack = 1, Size = Size)]
public struct PageHeader
{
    public const int Size = sizeof(long);
}
```

### Pages

Paprika uses paged-based addressing. Every page is 4kb. The size of the page is constant and cannot be changed. This makes pages lighter as they do not need to pass the information about their size. The page address, called `DbAddress`, can be encoded within first 3 bytes of an `uint` if the database size is smaller than 64GB. This leaves one byte for addressing within the page without blowing the address beyond 4 bytes.

There are different types of pages:

1. `RootPage`
1. `AbandonedPage`
1. `DataPage`

#### Root page

The `RootPage` is a page responsible for holding all the metadata information needed to query and amend data. It consists of:

1. batch id - a monotonically increasing number, identifying each batch write to the database that happened
1. block information including its number and the hash
1. abandoned pages

The last one is a collection of `DbAddress` pointing to the `Abandoned Pages`. As the amount of metadata is not big, one root page can store over 1 thousand addresses of the abandoned pages.

#### Abandoned Page

An abandoned page is a page storing information about pages that were abandoned during a given batch. Let's describe what abandonment means. When a page is COWed, the original copy should be maintain for the readers. After a given period of time, defined by the reorganization max depth, the page should be reused to not blow up the database. That is why `AbandonedPage` memoizes the batch which it was created at. Whenever a new page is requested, the allocator checks the list of unused pages (pages that were abandoned that passed the threshold of max reorg depth. If there's some, the page can be reused.

As each `AbandonedPage` can store ~1,000 of pages, in cases of big updates, several pages are required to store addresses of all the abandoned pages. As they share the batch number in which they are abandoned, a linked list is used to occupy only a single slot in the `AbandonedPages` of the `RootPage`. The unlinking and proper management of the list is left up to the allocator that updates slots in the `RootPage` accordingly.

The biggest caveat is where to store the `AbandonedPage`. The same mechanism is used for them as for other pages. This means, that when a block is committed, to store an `AbandonedPage`, the batch needs to allocate (which may get it from the pool) a page and copy to it.

#### Data Page

A data page is responsible for storying data, meaning a map from the `Keccak`->`Account`. The data page tries to store as much data as possible inline. If there's no more space, left, it selects a bucket, defined by a nibble. The one with the highest count of items is flushed as a separate page and a pointer to that page is stored in the bucket of the original `DataPage`. This is a bit different approach from using page splits. Instead of splitting the page and updating the parent, the page can flush down some of its data, leaving more space for the new. A single `PageData` can hold roughly 31-60 accounts. This divided by the count of nibbles 16 gives a rough minimal estimate of how much flushing down can save memory (at least 2 frames).

### Page design in C\#

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

The following ASCII Art should provide a better picture for the composition approach

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

#### Page number

As each page is a wrapper for a pointer. It contains no information about the page number. The page number can be retrieved from the database though, that provides it with the following calculation:

```csharp
private DbAddress GetAddress(in Page page)
{
    return DbAddress.Page((uint)(Unsafe
        .ByteOffset(ref Unsafe.AsRef<byte>(Ptr), ref Unsafe.AsRef<byte>(page.Raw.ToPointer()))
        .ToInt64() / Page.PageSize));
}
```

#### Page headers

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
    public const int Size = sizeof(long);

    /// <summary>
    /// The id of the last transaction that wrote to this page.
    /// </summary>
    [FieldOffset(0)]
    public long TransactionId;
}
```

#### Frames

As stored values fall into one of several categories, like EOA or contract data, const length frames are used to store the data. This allows for fast bit-wise-based page memory management and removes the cost of var-length serialization and page buffer management.
