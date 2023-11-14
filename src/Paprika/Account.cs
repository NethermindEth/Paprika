using Nethermind.Int256;
using Paprika.Crypto;
using Paprika.Data;

namespace Paprika;

/// <summary>
/// A presentation of an account.
/// </summary>
public readonly struct Account : IEquatable<Account>
{
    public static readonly Account Empty = default;

    public readonly UInt256 Balance;
    public readonly UInt256 Nonce;
    public readonly Keccak CodeHash;
    public readonly Keccak StorageRootHash;

    public bool IsEmpty => Balance.IsZero
                           && Nonce.IsZero
                           && CodeHash == EmptyCodeHash
                           && StorageRootHash == EmptyStorageRoot;

    public Account(UInt256 balance, UInt256 nonce)
    {
        Balance = balance;
        Nonce = nonce;
        CodeHash = EmptyCodeHash;
        StorageRootHash = EmptyStorageRoot;
    }

    public Account(UInt256 balance, UInt256 nonce, Keccak codeHash, Keccak storageRootHash)
    {
        Balance = balance;
        Nonce = nonce;
        CodeHash = codeHash;
        StorageRootHash = storageRootHash;
    }

    public Account WithChangedStorageRoot(Keccak newStorageRoot) => new(Balance, Nonce, CodeHash, newStorageRoot);

    public bool Equals(Account other) => Balance.Equals(other.Balance) &&
                                         Nonce == other.Nonce &&
                                         CodeHash == other.CodeHash &&
                                         StorageRootHash == other.StorageRootHash;

    public override bool Equals(object? obj) => obj is Account other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Balance, Nonce, CodeHash, StorageRootHash);

    public static bool operator ==(Account left, Account right) => left.Equals(right);

    public static bool operator !=(Account left, Account right) => !left.Equals(right);

    public override string ToString() =>
        $"{nameof(Nonce)}: {Nonce}, " +
        $"{nameof(Balance)}: {Balance}, " +
        $"{nameof(CodeHash)}: {CodeHash}" +
        $"{nameof(StorageRootHash)}: {StorageRootHash}";

    public const int MaxByteCount = BigPreambleLength + // preamble 
                                    Serializer.Uint256Size + // balance
                                    Serializer.Uint256Size + // nonce
                                    Keccak.Size + // CodeHash
                                    Keccak.Size; // StorageRootHash


    private const ulong SevenBytesULong = 0x00_FF_FF_FF_FF_FF_FF_FF;
    private const ulong ThreeBytesULong = 0x00_00_00_00_00_FF_FF_FF;

    /// <summary>
    /// Seven bytes max for the nonce.
    /// </summary>
    private static readonly UInt256 MaxDenseNonce = new(ThreeBytesULong);

    /// <summary>
    /// 15 bytes max for the balance.
    /// </summary>
    private static readonly UInt256 MaxDenseBalance = new(ulong.MaxValue, SevenBytesULong);

    private static readonly Keccak EmptyCodeHash = Keccak.OfAnEmptyString;
    private static readonly Keccak EmptyStorageRoot = Keccak.EmptyTreeHash;

    private const byte DensePreambleLength = 1;
    private const byte DenseMask = 0b1000_0000;
    private const byte DenseNonceLengthShift = 4;
    private const byte DenseNonceLengthMask = 0b0011_0000;
    private const byte DenseBalanceMask = 0b0000_1111;
    private const byte DenseCodeHashAndStorageRootExistMask = 0b0100_0000;
    private const byte DenseCodeHashAndStorageRootExistShift = 6;

    private const byte BigPreambleLength = 2;
    private const byte BigPreambleBalanceIndex = 0;
    private const byte BigPreambleNonceIndex = 1;

    /// <summary>
    /// Serializes the account balance and nonce.
    /// </summary>
    /// <returns>The actual payload written.</returns>
    public Span<byte> WriteTo(Span<byte> destination)
    {
        if (Balance <= MaxDenseBalance && Nonce <= MaxDenseNonce)
        {
            // special case, we can encode it a dense way
            var span = destination.Slice(DensePreambleLength);
            span = Balance.WriteWithLeftover(span, out var balanceLength);
            Nonce.WriteWithLeftover(span, out var nonceLength);

            var codeHashStorageRootExist = CodeHashOrStorageRootExist;

            destination[0] = (byte)(DenseMask |
                                    balanceLength |
                                    (nonceLength << DenseNonceLengthShift) |
                                    ((codeHashStorageRootExist ? 1 : 0) << DenseCodeHashAndStorageRootExistShift));

            var balanceAndNonceLength = DensePreambleLength + balanceLength + nonceLength;

            // CodeHash & StorageRootHash flags
            if (!codeHashStorageRootExist)
            {
                return destination.Slice(0, balanceAndNonceLength);
            }

            CodeHash.BytesAsSpan.CopyTo(destination.Slice(balanceAndNonceLength));
            StorageRootHash.BytesAsSpan.CopyTo(destination.Slice(balanceAndNonceLength + Keccak.Size));

            return destination.Slice(0, balanceAndNonceLength + Keccak.Size + Keccak.Size);
        }

        {
            // really big numbers
            var span = destination.Slice(BigPreambleLength);
            span = Balance.WriteWithLeftover(span, out var balanceLength);
            span = Nonce.WriteWithLeftover(span, out var nonceLength);

            destination[BigPreambleBalanceIndex] = (byte)balanceLength;
            destination[BigPreambleNonceIndex] = (byte)nonceLength;

            CodeHash.BytesAsSpan.CopyTo(span);
            StorageRootHash.BytesAsSpan.CopyTo(span.Slice(Keccak.Size));

            return destination.Slice(0, BigPreambleLength + balanceLength + nonceLength + Keccak.Size + Keccak.Size);
        }
    }

    private bool CodeHashOrStorageRootExist => CodeHash != EmptyCodeHash || StorageRootHash != EmptyStorageRoot;

    /// <summary>
    /// Reads the account balance and nonce.
    /// </summary>
    public static void ReadFrom(ReadOnlySpan<byte> source, out Account account)
    {
        var first = source[0];
        if ((first & DenseMask) == DenseMask)
        {
            // special case, decode the dense
            var nonceLength = (first & DenseNonceLengthMask) >> DenseNonceLengthShift;
            var balanceLength = first & DenseBalanceMask;

            Serializer.ReadFrom(source.Slice(DensePreambleLength, balanceLength), out var balance);
            Serializer.ReadFrom(source.Slice(DensePreambleLength + balanceLength, nonceLength), out var nonce);

            var codeHashStorageRootExist =
                (first & DenseCodeHashAndStorageRootExistMask) == DenseCodeHashAndStorageRootExistMask;

            if (codeHashStorageRootExist == false)
            {
                account = new Account(balance, nonce);
                return;
            }

            account = new Account(balance, nonce,
                new Keccak(source.Slice(DensePreambleLength + balanceLength + nonceLength, Keccak.Size)),
                new Keccak(source.Slice(DensePreambleLength + balanceLength + nonceLength + Keccak.Size, Keccak.Size)));

            return;
        }

        {
            var balanceLength = source[BigPreambleBalanceIndex];
            var nonceLength = source[BigPreambleNonceIndex];

            Serializer.ReadFrom(source.Slice(BigPreambleLength, balanceLength), out var balance);
            Serializer.ReadFrom(source.Slice(BigPreambleLength + balanceLength, nonceLength), out var nonce);

            account = new Account(balance, nonce,
                new Keccak(source.Slice(BigPreambleLength + balanceLength + nonceLength, Keccak.Size)),
                new Keccak(source.Slice(BigPreambleLength + balanceLength + nonceLength + Keccak.Size, Keccak.Size))
            );
        }
    }
}