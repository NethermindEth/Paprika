using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.Contracts;
using System.Diagnostics.Metrics;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Paprika.Chain;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Store;
using Paprika.Utils;
using Spreads.Buffers;
using Spreads.LMDB;

namespace Paprika.LMDB;

public class Db : IDb, IDisposable
{
    private readonly int _historyDepth;
    private readonly LMDBEnvironment _env;
    private readonly Database _db;
    private readonly object _batchLock = new();
    private readonly Queue<ReadOnlyBatchCountingRefs> _readers = new();

    private readonly Meter _meter;
    private readonly MetricsExtensions.IAtomicIntGauge _depth;
    private readonly MetricsExtensions.IAtomicIntGauge _compressedLength;

    /// <summary>
    /// Used for storing metadata and mapping between accounts and their counters.
    /// </summary>
    /// <remarks>
    /// 0xFF, 1 - metadata
    /// 0xFF, 2 - account counter
    /// 0xFF + 32bytes -> id of an account
    /// 0x80 + 32bytes -> account
    /// </remarks>
    private const byte PrefixMeta = 0xFF;
    private const byte PrefixAccount = 0x80;

    private const int MetaPrefixLength = 1;

    /// <summary>
    /// For compressed keys <see cref="DataType.CompressedAccount"/>, they will always start with 1
    /// and compress much better than 64 bytes. For non compressed, this should be much smaller than that.
    /// </summary>
    private const int MaxMappedKeyLength = 64;

    private const int MaxAccountIdLength = 8;
    private const int AccountMapKeyLength = MetaPrefixLength + Keccak.Size;
    private const int TypeShift = 4;
    private const int IdLengthShift = 1;

    private static ReadOnlySpan<byte> MetadataKey => new byte[] { PrefixMeta, 1 };
    private static ReadOnlySpan<byte> AccountCounter => new byte[] { PrefixMeta, 2 };

    public Db(string path, int historyDepth, long maxMapSize, bool sync = true)
    {
        _historyDepth = historyDepth;

        const LMDBEnvironmentFlags noSyncFlags = LMDBEnvironmentFlags.NoSync | LMDBEnvironmentFlags.WriteMap;

        _env = LMDBEnvironment.Create(path, sync ? LMDBEnvironmentFlags.NoMetaSync : noSyncFlags);
        _env.MapSize = maxMapSize;
        _env.Open();

        _db = _env.OpenDatabase(null, new DatabaseConfig(DbFlags.Create));

        _meter = new Meter("Paprika.Store.LMDB");
        _depth = _meter.CreateAtomicObservableGauge("BTree depth", "Depth", "The number of levels in BTree");
        _compressedLength = _meter.CreateAtomicObservableGauge("Max length compressed prefix of account", "Bytes", "The number of bytes");

        PushLatestReader();
    }

    public IBatch BeginNextBatch() => Batch.ReadWrite(this, _env.BeginTransaction());

    public IReadOnlyBatch BeginReadOnlyBatch(string name = "")
    {
        lock (_batchLock)
        {
            var last = _readers.Last();
            last.AcquireLease();
            return last;
        }
    }

    public void Flush()
    {
    }

    public IReadOnlyBatch BeginReadOnlyBatchOrLatest(in Keccak stateHash, string name = "")
    {
        lock (_batchLock)
        {
            ReadOnlyBatchCountingRefs last = null!;
            foreach (var reader in _readers)
            {
                last = reader;
                if (reader.Metadata.StateHash == stateHash)
                {
                    break;
                }
            }

            last.AcquireLease();
            return last;
        }
    }

    public bool HasState(in Keccak keccak)
    {
        lock (_batchLock)
        {
            foreach (var reader in _readers)
            {
                if (reader.Metadata.StateHash == keccak)
                {
                    return true;
                }
            }

            return false;
        }
    }

    public Stats GatherStats()
    {
        using var read = _env.BeginReadOnlyTransaction();
        using var cursor = _db.OpenReadOnlyCursor(read);

        Stats stats = new Stats();

        DirectBuffer key = default, value = default;
        if (cursor.TryGet(ref key, ref value, CursorGetOption.First))
        {
            stats.Record(key, value);

            while (cursor.TryGet(ref key, ref value, CursorGetOption.Next))
            {
                stats.Record(key, value);
            }
        }

        return stats;

    }

    private bool TryMapKey<TAccountContext>(in TAccountContext ctx, in Key key, Span<byte> destination,
        out Span<byte> mapped)
        where TAccountContext : struct, IAccountContext
    {
        if (key.Path.Length == NibblePath.KeccakNibbleCount)
        {
            // It can be either Account, Storage, or Merkle for Storage
            if (key.Type == DataType.Account)
            {
                // Key length optimization.
                //
                // The majority of the accounts will have no storage.
                // It would be wasteful to use additional mapping for them.
                // Create key directly and skip creating additional mapping for the account.
                destination[0] = PrefixAccount;
                key.Path.UnsafeAsKeccak.Span.CopyTo(destination[1..]);
                mapped = destination[..(1 + Keccak.Size)];
                return true;
            }

            // It can be Storage, or Merkle for the Storage
            // First try to get its account id.
            if (TryGetAccountId(ctx.Tx, key.Path.UnsafeAsKeccak, out var id))
            {
                mapped = CompressKey(key, id, destination);
                return true;
            }

            if (ctx.CanGenerate)
            {
                id = ctx.Generate(key.Path.UnsafeAsKeccak);
                mapped = CompressKey(key, id, destination);

                _compressedLength.Set(id.Length);

                return true;
            }

            // The account that should be mapped does not exist
            mapped = default;
            return false;
        }

        Debug.Assert(key.Type == DataType.Merkle &&
                     key.StoragePath.Length == 0 &&
                     key.Path.Length < NibblePath.KeccakNibbleCount);


        // shift left key by 4, so that move k up, so that the first bit is left unset
        // store oddity at the end
        const byte firstByte = (int)DataType.Merkle << TypeShift;
        const int headerLength = 1;

        if (key.Path.IsEmpty)
        {
            destination[0] = firstByte;
            mapped = destination[..headerLength];
            return true;
        }

        destination[0] = firstByte;
        var written = key.Path.WriteTo(destination[headerLength..]);
        mapped = destination[..(written.Length + headerLength)];
        return true;
    }

    private static Span<byte> CompressKey(in Key key, ReadOnlySpan<byte> id, Span<byte> destination)
    {
        Debug.Assert(key.Path.Length == NibblePath.KeccakNibbleCount);

        var k = key.Type | DataType.CompressedAccount;

        // shift left key by 4, so that move k up, so that the first bit is left unset
        var firstByte = ((int)k << TypeShift) + (id.Length << IdLengthShift);

        destination[0] = (byte)firstByte;
        id.CopyTo(destination[1..]);

        var slice = 1 + id.Length;
        if (key.StoragePath.Length == 0)
        {
            return destination.Slice(0, slice);
        }

        int storagePathLength;
        if (key.Type == DataType.StorageCell)
        {
            // Key length optimization.
            //
            // The storage will always have the length of the StoragePath equal to 32. 
            // This allows to write raw Keccak and skip the byte of preamble of the path for the storage. 
            key.StoragePath.UnsafeAsKeccak.Span.CopyTo(destination[slice..]);
            storagePathLength = 32;
        }
        else
        {
            storagePathLength = key.StoragePath.WriteTo(destination[slice..]).Length;
        }

        var totalLength = slice + storagePathLength;
        return destination[..totalLength];
    }

    [SkipLocalsInit]
    private bool TryGetAccountId(ReadOnlyTransaction tx, in Keccak keccak, out ReadOnlySpan<byte> id)
    {
        Span<byte> span = stackalloc byte[AccountMapKeyLength];
        return TryGet(tx, BuildAccountMapKey(keccak, span), out id);
    }

    private static Span<byte> BuildAccountMapKey(in Keccak keccak, Span<byte> span)
    {
        span[0] = PrefixMeta;
        keccak.Span.CopyTo(span[1..]);
        return span.Slice(0, AccountMapKeyLength);
    }

    private bool TryGet(ReadOnlyTransaction tx, scoped ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        var k = ToBuffer(key);

        if (_db.TryGet(tx, ref k, out var v))
        {
            value = v.Span;
            return true;
        }

        value = default;
        return false;
    }

    private void Put(Transaction tx, scoped ReadOnlySpan<byte> key, scoped ReadOnlySpan<byte> value)
    {
        var k = ToBuffer(key);
        var v = ToBuffer(value);

        _db.Put(tx, ref k, ref v);
    }

    private void Delete(Transaction tx, scoped ReadOnlySpan<byte> key)
    {
        var k = ToBuffer(key);
        _db.Delete(tx, ref k);
    }

    private static DirectBuffer ToBuffer(ReadOnlySpan<byte> key)
    {
        return new DirectBuffer(MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(key), key.Length));
    }

    public void Dispose()
    {
        lock (_batchLock)
        {
            while (_readers.TryDequeue(out var reader))
            {
                reader.Dispose();
            }
        }

        _db.Dispose();
        _env.Dispose();
        _meter.Dispose();
    }

    class Batch : IBatch
    {
        private readonly Db _db;
        private readonly Transaction _tx;
        private readonly ReadOnlyTransaction _read;

        public static Batch ReadWrite(Db db, Transaction tx) => new(db, tx, tx);
        public static Batch ReadOnly(Db db, ReadOnlyTransaction tx) => new(db, default, tx);

        private Batch(Db db, Transaction tx, ReadOnlyTransaction read)
        {
            _db = db;
            _tx = tx;
            _read = read;

            Metadata = _db.TryGet(_read, MetadataKey, out var value)
                ? Metadata.ReadFrom(value)
                : default;
        }

        public Metadata Metadata { get; private set; }

        [SkipLocalsInit]
        public bool TryGet(scoped in Key key, out ReadOnlySpan<byte> result)
        {
            var ctx = new ReadonlyAccountContext(_read);
            if (_db.TryMapKey(ctx, key, stackalloc byte[MaxMappedKeyLength], out var mapped))
            {
                return _db.TryGet(_read, mapped, out result);
            }

            result = default;
            return false;
        }

        public void SetMetadata(uint blockNumber, in Keccak blockHash)
        {
            Span<byte> destination = stackalloc byte[Metadata.Size];

            Metadata = new Metadata(blockNumber, blockHash);
            Metadata.WriteTo(destination);

            _db.Put(_tx, MetadataKey, destination);
        }

        public void SetRaw(in Key key, ReadOnlySpan<byte> rawData)
        {
            var ctx = new ReadWriteAccountContext(_tx, _db);
            if (!_db.TryMapKey(ctx, key, stackalloc byte[MaxMappedKeyLength], out var mapped))
            {
                throw new Exception("Should id map properly");
            }

            if (rawData.IsEmpty)
            {
                _db.Delete(_tx, mapped);
            }
            else
            {
                _db.Put(_tx, mapped, rawData);
            }
        }

        [SkipLocalsInit]
        public void Destroy(in NibblePath account)
        {
            Debug.Assert(account.Length == NibblePath.KeccakNibbleCount);

            // TODO: apply proper delete, otherwise db will grow
            _db.Delete(_tx, BuildAccountMapKey(account.UnsafeAsKeccak, stackalloc byte[AccountMapKeyLength]));
        }

        public ValueTask Commit(CommitOptions options)
        {
            _tx.Commit();
            _db.Committed(this);

            return ValueTask.CompletedTask;
        }

        public void Dispose()
        {
            _read.Dispose();
            // _tx doesn't have to be disposed. It should be the same as read.
        }
    }

    private void Committed(Batch batch)
    {
        PushLatestReader();

        var stats = _env.GetStat();
        _depth.Set((int)stats.ms_depth);
    }

    private void PushLatestReader()
    {
        lock (_batchLock)
        {
            var reader = Batch.ReadOnly(this, _env.BeginReadOnlyTransaction());
            _readers.Enqueue(new ReadOnlyBatchCountingRefs(reader));

            // ensure memory not breached
            if (_readers.Count > _historyDepth)
            {
                _readers.Dequeue().Dispose();
            }
        }
    }

    private interface IAccountContext
    {
        ReadOnlyTransaction Tx { get; }

        bool CanGenerate { get; }

        [Pure]
        Span<byte> Generate(in Keccak keccak);
    }

    private readonly struct ReadonlyAccountContext(ReadOnlyTransaction tx) : IAccountContext
    {
        public ReadOnlyTransaction Tx { get; } = tx;

        public bool CanGenerate => false;

        public Span<byte> Generate(in Keccak keccak)
        {
            throw new Exception("Readonly");
        }
    }

    private readonly struct ReadWriteAccountContext(Transaction tx, Db db) : IAccountContext
    {
        public ReadOnlyTransaction Tx => tx;

        public bool CanGenerate => true;

        public Span<byte> Generate(in Keccak keccak)
        {
            if (db.TryGet(tx, AccountCounter, out var value) == false)
            {
                value = new byte[] { 0 };
            }

            Span<byte> span = stackalloc byte[MaxAccountIdLength];
            value.CopyTo(span);

            var id = BinaryPrimitives.ReadUInt64LittleEndian(span) + 1;
            BinaryPrimitives.WriteUInt64LittleEndian(span, id);

            // align length to full bytes
            var length = (64 + 7 - BitOperations.LeadingZeroCount(id)) / 8;

            var sliced = span.Slice(0, length);

            db.Put(tx, AccountCounter, sliced);
            db.Put(tx, BuildAccountMapKey(keccak, stackalloc byte[AccountMapKeyLength]), sliced);

            // TODO: later try to clean this up
            return sliced.ToArray();
        }
    }

    public void ForceSync()
    {
        _env.Sync(true);
    }

    public class Stats
    {
        public int Accounts { get; private set; }
        public long TotalSizeAccounts { get; private set; }
        public int IdMappings { get; private set; }
        public long TotalSizeIdMappings { get; private set; }

        public long TotalSizeStorage { get; private set; }
        public long TotalSizeMerkle { get; private set; }

        public long TotalSizeEmbeddedMerkleLeafs { get; private set; }

        public void Record(in DirectBuffer key, in DirectBuffer value)
        {
            var totalLength = key.Length + value.Length + 8;

            var first = key.Span[0];
            switch (first)
            {
                case PrefixAccount:
                    Accounts++;
                    TotalSizeAccounts += totalLength;
                    break;
                case PrefixMeta:
                    {
                        switch (key.Span.Length)
                        {
                            case 1 + Keccak.Size:
                                IdMappings++;
                                TotalSizeIdMappings += totalLength;
                                break;
                            case > 2:
                                throw new Exception($"{((ReadOnlySpan<byte>)key.Span).ToHexString(true)} should not appear here");
                        }

                        break;
                    }
                default:
                    {
                        // complex key
                        var type = (DataType)(first >> TypeShift) & ~DataType.CompressedAccount;

                        switch (type)
                        {
                            case DataType.StorageCell:
                                TotalSizeStorage += totalLength;
                                break;
                            case DataType.Merkle:
                                TotalSizeMerkle += totalLength;

                                Node.ReadFrom(value.Span, out var nodeType, out _, out _, out var branch);
                                if (nodeType == Node.Type.Branch)
                                {
                                    TotalSizeEmbeddedMerkleLeafs += branch.Leafs.MaxByteSize;
                                }

                                break;
                            default:
                                throw new ArgumentOutOfRangeException();
                        }
                        break;
                    }
            }
        }

        public override string ToString()
        {
            var sb = new StringBuilder();

            var props = this.GetType().GetProperties();

            foreach (var prop in props)
            {
                var value = prop.GetValue(this);

                if (value == null)
                {
                    sb.AppendLine($"{prop.Name}: null");
                }
                else if (prop.Name.StartsWith("total", StringComparison.OrdinalIgnoreCase))
                {
                    var size = ((double)((long)value!)) / (1024 * 1024 * 1024);
                    sb.AppendLine($"{prop.Name}: {size:F2}");
                }
                else
                {
                    sb.AppendLine($"{prop.Name}: {value.ToString()}");
                }
            }

            return sb.ToString();
        }
    }
}

