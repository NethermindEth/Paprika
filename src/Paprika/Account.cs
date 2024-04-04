using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
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

    public Account(in UInt256 balance, in UInt256 nonce)
    {
        Balance = balance;
        Nonce = nonce;
        CodeHash = EmptyCodeHash;
        StorageRootHash = EmptyStorageRoot;
    }

    public Account(in UInt256 balance, in UInt256 nonce, in Keccak codeHash, in Keccak storageRootHash)
    {
        Balance = balance;
        Nonce = nonce;
        CodeHash = codeHash;
        StorageRootHash = storageRootHash;
    }

    public void WithChangedStorageRoot(in Keccak newStorageRoot, out Account account)
    {
        // Fast copy to return buffer
        account = this;
        // Overwrite the storage root hash
        Unsafe.AsRef(in account.StorageRootHash) = newStorageRoot;
    }

    public bool Equals(Account other) => EqualsByRef(other);

    public bool EqualsByRef(in Account other) => Balance.Equals(other.Balance) &&
                                         Nonce == other.Nonce &&
                                         CodeHash == other.CodeHash &&
                                         StorageRootHash == other.StorageRootHash;

    public override bool Equals(object? obj) => obj is Account other && EqualsByRef(other);

    public override int GetHashCode() => HashCode.Combine(Balance, Nonce, CodeHash, StorageRootHash);

    public static bool operator ==(in Account left, in Account right) => left.EqualsByRef(right);

    public static bool operator !=(in Account left, in Account right) => !left.EqualsByRef(right);

    public override string ToString() =>
        $"{nameof(Nonce)}: {Nonce}, " +
        $"{nameof(Balance)}: {Balance}, " +
        $"{nameof(CodeHash)}: {CodeHash}, " +
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
    [SkipLocalsInit]
    public Span<byte> WriteTo(Span<byte> destination)
    {
        int balanceAndNonceLength;
        ref var dest = ref MemoryMarshal.GetReference(destination);

        if (Balance <= MaxDenseBalance && Nonce <= MaxDenseNonce)
        {
            // special case, we can encode it a dense way
            var balanceLength = Serializer.WriteWithLeftover(Balance, destination[DensePreambleLength..]);
            var nonceLength = Serializer.WriteWithLeftover(Nonce, destination[(DensePreambleLength + balanceLength)..]);

            var codeHashStorageRootExist = CodeHashOrStorageRootExist;

            destination[0] = (byte)(DenseMask |
                                    balanceLength |
                                    (nonceLength << DenseNonceLengthShift) |
                                    ((codeHashStorageRootExist ? 1 : 0) << DenseCodeHashAndStorageRootExistShift));

            balanceAndNonceLength = DensePreambleLength + balanceLength + nonceLength;

            // CodeHash & StorageRootHash flags
            if (!CodeHashOrStorageRootExist)
            {
                return destination[..balanceAndNonceLength];
            }
        }
        else
        {
            // Massive numbers
            var balanceLength = Serializer.WriteWithLeftover(Balance, destination[BigPreambleLength..]);
            var nonceLength = Serializer.WriteWithLeftover(Nonce, destination[(BigPreambleLength + balanceLength)..]);

            // Write lengths
            Unsafe.Add(ref dest, BigPreambleBalanceIndex) = (byte)balanceLength;
            Unsafe.Add(ref dest, BigPreambleNonceIndex) = (byte)nonceLength;

            balanceAndNonceLength = BigPreambleLength + balanceLength + nonceLength;
        }

        Unsafe.WriteUnaligned(ref Unsafe.Add(ref dest, balanceAndNonceLength), CodeHash);
        Unsafe.WriteUnaligned(ref Unsafe.Add(ref dest, balanceAndNonceLength + Keccak.Size), StorageRootHash);

        return destination.Slice(0, balanceAndNonceLength + Keccak.Size + Keccak.Size);
    }

    private bool CodeHashOrStorageRootExist
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get
        {
            if (Vector256.IsHardwareAccelerated)
            {
                // The bitwise equivalent of the one below
                return !((CodeHash ^ EmptyCodeHash) | (StorageRootHash ^ EmptyStorageRoot)).Equals(Vector256<byte>.Zero);
            }
            else
            {
                return CodeHash != EmptyCodeHash || StorageRootHash != EmptyStorageRoot;
            }
        }
    }

    /// <summary>
    /// Reads the account balance and nonce.
    /// </summary>
    [SkipLocalsInit]
    public static void ReadFrom(ReadOnlySpan<byte> source, out Account account)
    {
        UInt256 balance;
        UInt256 nonce;

        var first = source[0];
        int consumed;
        if ((first & DenseMask) == DenseMask)
        {
            // special case, decode the dense
            var nonceLength = (first & DenseNonceLengthMask) >> DenseNonceLengthShift;
            var balanceLength = first & DenseBalanceMask;

            Serializer.ReadFrom(source.Slice(DensePreambleLength, balanceLength), out balance);
            Serializer.ReadFrom(source.Slice(DensePreambleLength + balanceLength, nonceLength), out nonce);

            var codeHashStorageRootExist =
                (first & DenseCodeHashAndStorageRootExistMask) == DenseCodeHashAndStorageRootExistMask;

            if (codeHashStorageRootExist == false)
            {
                account = new Account(balance, nonce);
                return;
            }

            consumed = DensePreambleLength + balanceLength + nonceLength;
        }
        else
        {

            var balanceLength = source[BigPreambleBalanceIndex];
            var nonceLength = source[BigPreambleNonceIndex];

            Serializer.ReadFrom(source.Slice(BigPreambleLength, balanceLength), out balance);
            Serializer.ReadFrom(source.Slice(BigPreambleLength + balanceLength, nonceLength), out nonce);

            consumed = BigPreambleLength + balanceLength + nonceLength;
        }

        account = new Account(balance, nonce,
            Unsafe.ReadUnaligned<Keccak>(in source[consumed]),
            Unsafe.ReadUnaligned<Keccak>(in source[consumed + Keccak.Size])
        );
    }
}
