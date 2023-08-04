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
    // history <- before <- after
    private readonly Dictionary<byte[], byte[]> _history = new(new BytesEqualityComparer());
    private readonly Dictionary<byte[], byte[]> _before = new(new BytesEqualityComparer());
    private readonly Dictionary<byte[], byte[]> _after = new(new BytesEqualityComparer());
    private bool _asserting;

    public void Set(in Key key, ReadOnlySpan<byte> value)
    {
        _before[GetKey(key)] = value.ToArray();
    }

    public void DeleteKey(in Key key) => Set(key, ReadOnlySpan<byte>.Empty);

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

        if (_after.TryGetValue(k, out var value))
        {
            return new ReadOnlySpanOwner<byte>(value, null);
        }

        if (_before.TryGetValue(k, out value))
        {
            return new ReadOnlySpanOwner<byte>(value, null);
        }

        if (_history.TryGetValue(k, out value))
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
            action(key, v, this);
        }
    }

    /// <summary>
    /// Sets the commit into an asserting mode, where all the sets will be removing and asserting values from it.
    /// </summary>
    public void StartAssert()
    {
        _asserting = true;
    }

    /// <summary>
    /// Generates next commit, folding history into the current state.
    /// </summary>
    public Commit Squash(bool removeEmpty = false)
    {
        var commit = new Commit();

        var dict = commit._history;

        foreach (var kvp in _before)
        {
            dict[kvp.Key] = kvp.Value;
        }

        foreach (var kvp in _after)
        {
            dict[kvp.Key] = kvp.Value;
        }

        if (removeEmpty)
        {
            var emptyKeys = dict
                .Where((kvp) => kvp.Value.Length == 0)
                .Select(kvp => kvp.Key)
                .ToArray();

            foreach (var emptyKey in emptyKeys)
            {
                dict.Remove(emptyKey);
            }
        }

        return commit;
    }
}