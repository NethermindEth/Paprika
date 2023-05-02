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


    public Account(UInt256 balance, UInt256 nonce)
    {
        _balance = balance;
        _nonce = nonce;
    }

    public bool Equals(Account other) => _balance.Equals(other._balance) && _nonce == other._nonce;

    public override bool Equals(object? obj) => obj is Account other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(_balance, _nonce);

    public static bool operator ==(Account left, Account right) => left.Equals(right);

    public static bool operator !=(Account left, Account right) => !left.Equals(right);
}