# :hot_pepper: Paprika

Paprika is a custom database implementation for `State` and `Storage` trees of Ethereum. This document covers the main design ideas, inspirations and most important implementation details.

## Design

Paprika is a database that uses [memory-mapped files](https://en.wikipedia.org/wiki/Memory-mapped_file). To handle concurrency, [Copy on Write](https://en.wikipedia.org/wiki/Copy-on-write) is used. This allows multiple concurrent readers to cooperate in a full lock-free manner and a single writer that runs the current transaction. In that manner, it's heavily inspired by [LMBD](https://github.com/LMDB/lmdb). Paprika uses 4kb pages.

### Reorganizations handling

The foundation of any blockchain is a single list of blocks, a chain. The _canonical chain_ is the chain that is perceived to be the main chain. Due to the nature of the network, the canonical chain changes in time. If a block at a given position in the list is replaced by another, the effect is called a _reorganization_. The usual process of handling a reorganization is undoing recent N operations, until the youngest block was changed, and applying new blocks. From the database perspective, it means that it needs to be able to undo an arbitrary number of committed blocks. In Paprika, it's handled by specifying the _history depth_ that keeps the block information for at least _history depth_. This allows undoing blocks till the reorganization boundary and applying blocks from the canonical chain.

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

Pages are designed as value types that provide a wrapper around a raw memory pointer. The underlying pointer does not change, so that pages can be implemented as `readonly unsafe struct` like in the following example.

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
         ├─────────┼─────────────┬──────────────────────────────────────────────────────────────┤
         │ Page    │ Additional  │                                                              │
DataPage │ Header  │ DataPage    │   Payload of the page                                        │
         │         │ Header      │                                                              │
         └─────────┴─────────────┴──────────────────────────────────────────────────────────────┘
              ▲                  ▲
              │                  │
              │                  │  
              │                  │                         
          Page Header      DataPage Header, the same for all the DataPages
          is shared by
          all the pages
```

As fields are located in the same place (`DataPage` wraps `Page` that wraps `byte*`) and all the pages are a size of a `byte*`. To implemented the shared functionality, a markup interface `IPage` is used with some extension methods. Again, as pages have the data in the same place they can be cast with the help of `Unsafe`.

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

#### Page size

Every page is 4kb. The size of the page is constant and cannot be changed. This makes pages lighter as they do not need to pass the information about their size.

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

The 1st point, the type differentiation, can be addressed either by storying the page type or reasoning about the page place where the page is used. For example, if a page is one of the N pages that support reorganizations, it must be a `RootPage`.

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
