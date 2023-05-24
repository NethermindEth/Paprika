using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading.Channels;
using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Chain;

/// <summary>
/// 
/// </summary>
/// <remarks>
/// The current implementation assumes a single threaded access. For multi-threaded, some adjustments will be required.
/// The following should be covered:
/// 1. reading a state at a given time based on the root. Should never fail.
/// 2. TBD
/// </remarks>
public class Blockchain
{
    // allocate 1024 pages (4MB) at once
    private readonly PagePool _pool = new(1024);

    // TODO: potentially optimize if many blocks per one number occur
    private readonly ConcurrentDictionary<uint, Block[]> _blocksByNumber = new();
    private readonly ConcurrentDictionary<Keccak, Block> _blocksByHash = new();
    private readonly Channel<Block> _finalizedChannel;
    private readonly ConcurrentQueue<(IReadOnlyBatch reader, uint blockNumber)> _alreadyFlushedTo;

    private readonly PagedDb _db;
    private uint _lastFinalized;
    private IReadOnlyBatch _dbReader;

    public Blockchain(PagedDb db)
    {
        _db = db;
        _finalizedChannel = Channel.CreateUnbounded<Block>(new UnboundedChannelOptions
        {
            AllowSynchronousContinuations = true,
            SingleReader = true,
            SingleWriter = true
        });
        _alreadyFlushedTo = new ConcurrentQueue<(IReadOnlyBatch reader, uint blockNumber)>();
        _dbReader = db.BeginReadOnlyBatch();
    }

    public IWorldState StartNew(Keccak parentKeccak, Keccak blockKeccak, uint blockNumber)
    {
        return new Block(parentKeccak, blockKeccak, blockNumber, this);
    }

    private UInt256 GetStorage(in Keccak account, in Keccak address, Block start)
    {
        var bloom = Block.BloomForStorageOperation(account, address);
        var key = Key.StorageCell(NibblePath.FromKey(account), address);

        if (TryGetInBlockchain(start, bloom, key, out var result))
        {
            Serializer.ReadStorageValue(result, out var value);
            return value;
        }

        return default;
    }

    private Account GetAccount(in Keccak account, Block start)
    {
        var bloom = Block.BloomForAccountOperation(account);
        var key = Key.Account(NibblePath.FromKey(account));

        if (TryGetInBlockchain(start, bloom, key, out var result))
        {
            Serializer.ReadAccount(result, out var balance, out var nonce);
            return new Account(balance, nonce);
        }

        return default;
    }

    /// <summary>
    /// Finds the given key in the blockchain.
    /// </summary>
    private bool TryGetInBlockchain(Block start, int bloom, in Key key, out ReadOnlySpan<byte> result)
    {
        var block = start;

        // walk through the blocks
        do
        {
            if (block.TryGet(bloom, key, out result))
            {
                return true;
            }
        } while (_blocksByHash.TryGetValue(block.ParentHash, out block));

        // default to the reader
        return _dbReader.TryGet(key, out result);
    }

    public void Finalize(Keccak keccak)
    {
        ReuseAlreadyFlushed();

        // find the block to finalize
        if (_blocksByHash.TryGetValue(keccak, out var block) == false)
        {
            throw new Exception("Block that is marked as finalized is not present");
        }

        Debug.Assert(block.BlockNumber > _lastFinalized,
            "Block that is finalized should have a higher number than the last finalized");

        // gather all the blocks between last finalized and this.
        var count = block.BlockNumber - _lastFinalized;
        Stack<Block> finalized = new((int)count);
        for (var blockNumber = block.BlockNumber; blockNumber > _lastFinalized; blockNumber--)
        {
            // to finalize
            finalized.Push(block);

            // move to next
            block = _blocksByHash[block.ParentHash];
        }

        while (finalized.TryPop(out block))
        {
            // publish for the PagedDb
            _finalizedChannel.Writer.TryWrite(block);
        }

        _lastFinalized += count;
    }

    private void ReuseAlreadyFlushed()
    {
        while (_alreadyFlushedTo.TryDequeue(out var item))
        {
            // set the last reader
            var previous = _dbReader;

            _dbReader = item.reader;

            previous.Dispose();

            _lastFinalized = item.blockNumber;

            // clean blocks with a given number
            if (_blocksByNumber.Remove(item.blockNumber, out var blocks))
            {
                foreach (var block in blocks)
                {
                    block.Dispose();
                }
            }
        }
    }

    /// <summary>
    /// Represents a block that is a result of ExecutionPayload, storing it in a in-memory trie
    /// </summary>
    private class Block : IBatchContext, IWorldState
    {
        public Keccak Hash { get; }
        public Keccak ParentHash { get; }
        public uint BlockNumber { get; }

        private readonly DataPage _root;
        private readonly BitPage _bloom;
        private readonly Blockchain _blockchain;

        private readonly List<Page> _pages = new();

        public Block(Keccak parentHash, Keccak hash, uint blockNumber, Blockchain blockchain)
        {
            _blockchain = blockchain;
            Hash = hash;
            ParentHash = parentHash;
            BlockNumber = blockNumber;

            // rent one page for the bloom
            _bloom = new BitPage(GetNewPage(out _, true));

            // rent one page for the root of the data
            _root = new DataPage(GetNewPage(out _, true));
        }

        /// <summary>
        /// Commits the block to the block chain.
        /// </summary>
        public void Commit()
        {
            // set to blocks in number and in blocks by hash
            _blockchain._blocksByNumber.AddOrUpdate(BlockNumber,
                static (_, block) => new[] { block },
                static (_, existing, block) =>
                {
                    var array = existing;
                    Array.Resize(ref array, array.Length + 1);
                    array[^1] = block;
                    return array;
                }, this);

            _blockchain._blocksByHash.TryAdd(Hash, this);
        }

        private PagePool Pool => _blockchain._pool;


        public UInt256 GetStorage(in Keccak key, in Keccak address) => _blockchain.GetStorage(in key, in address, this);

        public Account GetAccount(in Keccak key) => _blockchain.GetAccount(in key, this);

        public static int BloomForStorageOperation(in Keccak key, in Keccak address) =>
            key.GetHashCode() ^ address.GetHashCode();

        public static int BloomForAccountOperation(in Keccak key) => key.GetHashCode();

        public void SetAccount(in Keccak key, in Account account)
        {
            throw new NotImplementedException();
        }

        public void SetStorage(in Keccak key, in Keccak address, UInt256 value)
        {
            throw new NotImplementedException();
        }

        Page IPageResolver.GetAt(DbAddress address) => Pool.GetAt(address);

        uint IReadOnlyBatchContext.BatchId => 0;

        DbAddress IBatchContext.GetAddress(Page page) => Pool.GetAddress(page);

        public Page GetNewPage(out DbAddress addr, bool clear)
        {
            var page = Pool.Get();

            page.Clear(); // always clear

            _pages.Add(page);

            addr = Pool.GetAddress(page);
            return page;
        }

        Page IBatchContext.GetWritableCopy(Page page) =>
            throw new Exception("The COW should never happen in block. It should always use only writable pages");

        /// <summary>
        /// The implementation assumes that all the pages are writable.
        /// </summary>
        bool IBatchContext.WasWritten(DbAddress addr) => true;

        public void Dispose()
        {
            // return all the pages
            foreach (var page in _pages)
            {
                Pool.Return(page);
            }
        }

        public bool TryGet(int bloom, Key key, out ReadOnlySpan<byte> result)
        {
            if (_bloom.IsSet(bloom) == false)
            {
                result = default;
                return false;
            }

            return _root.TryGet(key, this, out result);
        }
    }
}