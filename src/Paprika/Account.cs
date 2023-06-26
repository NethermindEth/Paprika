using Nethermind.Int256;
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

    public Account(UInt256 balance, UInt256 nonce)
    {
        Balance = balance;
        Nonce = nonce;
    }

    public bool Equals(Account other) => Balance.Equals(other.Balance) && Nonce == other.Nonce;

    public override bool Equals(object? obj) => obj is Account other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Balance, Nonce);

    public static bool operator ==(Account left, Account right) => left.Equals(right);

    public static bool operator !=(Account left, Account right) => !left.Equals(right);

    public override string ToString() => $"{nameof(Nonce)}: {Nonce}, {nameof(Balance)}: {Balance}";

    public const int MaxByteCount = BigPreambleLength + // preamble 
                                    Serializer.Uint256Size + // balance
                                    Serializer.Uint256Size; // nonce

    private const ulong SevenBytesULong = 0x00_FF_FF_FF_FF_FF_FF_FF;

    /// <summary>
    /// Seven bytes max for the nonce.
    /// </summary>
    private static readonly UInt256 MaxDenseNonce = new(SevenBytesULong);

    /// <summary>
    /// 15 bytes max for the balance.
    /// </summary>
    private static readonly UInt256 MaxDenseBalance = new(ulong.MaxValue, SevenBytesULong);

    private const byte DensePreambleLength = 1;
    private const byte DenseMask = 0b1000_0000;
    private const byte DenseNonceLengthShift = 4;
    private const byte DenseNonceLengthMask = 0b0111_0000;
    private const byte DenseBalanceMask = 0b0000_1111;

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

            destination[0] = (byte)(DenseMask | balanceLength | (nonceLength << DenseNonceLengthShift));
            return destination.Slice(0, DensePreambleLength + balanceLength + nonceLength);
        }

        {
            // really big numbers
            var span = destination.Slice(BigPreambleLength);
            span = Balance.WriteWithLeftover(span, out var balanceLength);
            Nonce.WriteWithLeftover(span, out var nonceLength);

            destination[BigPreambleBalanceIndex] = (byte)balanceLength;
            destination[BigPreambleNonceIndex] = (byte)nonceLength;

            return destination.Slice(0, BigPreambleLength + balanceLength + nonceLength);
        }
    }

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

            account = new Account(balance, nonce);
            return;
        }

        {
            var balanceLength = source[BigPreambleBalanceIndex];
            var nonceLength = source[BigPreambleNonceIndex];

            Serializer.ReadFrom(source.Slice(BigPreambleLength, balanceLength), out var balance);
            Serializer.ReadFrom(source.Slice(BigPreambleLength + balanceLength, nonceLength), out var nonce);

            account = new Account(balance, nonce);
        }
    }
}