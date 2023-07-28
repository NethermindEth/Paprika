using System.Text;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Data;
using Paprika.Utils;

namespace Paprika.Tests.Merkle;

/// <summary>
/// A commit mock used to provide the data and assert them when needed.
/// </summary>
class Commit : ICommit
{
    private readonly Dictionary<byte[], byte[]> _before = new(new BytesEqualityComparer());
    private readonly Dictionary<byte[], byte[]> _after = new(new BytesEqualityComparer());
    private bool _asserting;

    public void Set(in Key key, ReadOnlySpan<byte> value)
    {
        _before[GetKey(key)] = value.ToArray();
    }

    public void ShouldBeEmpty()
    {
        if (_after.Count == 0)
            return;

        var sb = new StringBuilder();
        sb.AppendLine("The commit should be empty, but it contains the following keys: ");
        foreach (var (key, value) in _after)
        {
            Key.ReadFrom(key, out Key k);
            sb.AppendLine($"- {k.ToString()}");
        }

        Assert.Fail(sb.ToString());
    }

    ReadOnlySpanOwner<byte> ICommit.Get(in Key key)
    {
        var k = GetKey(key);
        if (_before.TryGetValue(k, out var value))
        {
            return new ReadOnlySpanOwner<byte>(value, null);
        }

        if (_after.TryGetValue(k, out value))
        {
            return new ReadOnlySpanOwner<byte>(value, null);
        }

        return default;
    }

    private static byte[] GetKey(in Key key) => key.WriteTo(stackalloc byte[key.MaxByteLength]).ToArray();

    void ICommit.Set(in Key key, in ReadOnlySpan<byte> payload)
    {
        var bytes = GetKey(key);
        if (_asserting == false)
        {
            _after[bytes] = payload.ToArray();
        }
        else
        {
            _after.Remove(bytes, out var existing).Should().BeTrue($"key {key.ToString()} should exist");
            payload.SequenceEqual(existing).Should().BeTrue("The value should be equal");
        }
    }

    void ICommit.Visit(CommitAction action)
    {
        foreach (var (k, v) in _before)
        {
            Key.ReadFrom(k, out var key);
            action(key, this);
        }
    }

    /// <summary>
    /// Sets the commit into an asserting mode, where all the sets will be removing and asserting values from it.
    /// </summary>
    public void StartAssert()
    {
        _asserting = true;
    }
}