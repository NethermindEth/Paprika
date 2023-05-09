﻿using Nethermind.Int256;
using Paprika.Crypto;

namespace Paprika.Data;

public static class Serializer
{
    private const int Uint256Size = 32;
    private const int Uint256PrefixSize = 1;
    private const int MaxUint256SizeWithPrefix = Uint256Size + Uint256PrefixSize;
    private const bool BigEndian = true;
    private const int MaxNibblePathLength = Keccak.Size + 1;

    private static Span<byte> WriteToWithLeftover(Span<byte> destination, in UInt256 value)
    {
        var uint256 = destination.Slice(1, Uint256Size);
        value.ToBigEndian(uint256);
        var firstNonZero = uint256.IndexOfAnyExcept((byte)0);

        if (firstNonZero == -1)
        {
            // only zeros, special case
            destination[0] = 0;
            return destination.Slice(1);
        }

        var nonZeroBytes = (byte)(Uint256Size - firstNonZero);
        destination[0] = nonZeroBytes;

        // this will be usually the case, no need to check with if
        // move to first non zero
        uint256.Slice(firstNonZero).CopyTo(uint256);

        return destination.Slice(Uint256PrefixSize + nonZeroBytes);
    }

    private static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out UInt256 value)
    {
        var nonZeroBytes = source[0];
        Span<byte> uint256 = stackalloc byte[Uint256Size];

        source.Slice(Uint256PrefixSize, nonZeroBytes).CopyTo(uint256.Slice(Uint256Size - nonZeroBytes));
        value = new UInt256(uint256, BigEndian);
        return source.Slice(nonZeroBytes + Uint256PrefixSize);
    }

    public const int StorageValueMaxByteCount = MaxUint256SizeWithPrefix;


    /// <summary>
    /// Writes the storage value.
    /// </summary>
    public static Span<byte> WriteStorageValue(Span<byte> destination, in UInt256 value)
    {
        return destination.Slice(0, destination.Length - WriteToWithLeftover(destination, value).Length);
    }

    /// <summary>
    /// Writes the storage value.
    /// </summary>
    public static void ReadStorageValue(ReadOnlySpan<byte> source, out UInt256 value)
    {
        ReadFrom(source, out value);
    }

    public const int BalanceNonceMaxByteCount = MaxUint256SizeWithPrefix + // balance
                                                MaxUint256SizeWithPrefix; // nonce

    /// <summary>
    /// Serializes the account balance and nonce.
    /// </summary>
    /// <returns>The actual payload written.</returns>
    public static Span<byte> WriteAccount(Span<byte> destination, UInt256 balance, UInt256 nonce)
    {
        var leftover = WriteToWithLeftover(destination, balance);
        leftover = WriteToWithLeftover(leftover, nonce);

        return destination.Slice(0, destination.Length - leftover.Length);
    }

    /// <summary>
    /// Reads the account balance and nonce.
    /// </summary>
    public static void ReadAccount(ReadOnlySpan<byte> source, out UInt256 balance,
        out UInt256 nonce)
    {
        var span = ReadFrom(source, out balance);
        ReadFrom(span, out nonce);
    }
}