using System.Numerics;
using Nethermind.Int256;

namespace Paprika;

/// <summary>
/// A presentation of an account.
/// </summary>
public readonly struct Account : IEquatable<Account>
{
    public static readonly Account Empty = default;

    private readonly UInt256 _balance;
    private readonly UInt256 _nonce;
    public UInt256 Nonce => _nonce;

    public UInt256 Balance => _balance;

    public Account(in UInt128 balance, uint nonce)
    {
        _balance = Convert(balance);
        _nonce = nonce;
    }

    public Account(in UInt256 balance, UInt256 nonce)
    {
        _balance = balance;
        _nonce = nonce;
    }

    private static UInt256 Convert<TFrom>(in TFrom from)
        where TFrom : IBinaryInteger<UInt128>
    {
        Span<byte> span = stackalloc byte[32];
        from.WriteLittleEndian(span);
        return new UInt256(span, false);
    }

    public bool Equals(Account other) => _balance.Equals(other._balance) && _nonce == other._nonce;

    public override bool Equals(object? obj) => obj is Account other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(_balance, _nonce);

    public static bool operator ==(Account left, Account right) => left.Equals(right);

    public static bool operator !=(Account left, Account right) => !left.Equals(right);
}