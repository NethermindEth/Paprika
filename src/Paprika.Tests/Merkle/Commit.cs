using System.Text;
using FluentAssertions;
using NUnit.Framework;
using Paprika.Chain;
using Paprika.Data;
using Paprika.Merkle;
using Paprika.Utils;

namespace Paprika.Tests.Merkle;

/// <summary>
/// A commit mock used to provide the data and assert them when needed.
/// </summary>
public class Commit : ICommit
{
    // history <- before <- after
    private readonly Dictionary<byte[], byte[]> _history = new(Comparer);
    private readonly Dictionary<byte[], byte[]> _before = new(Comparer);
    private readonly Dictionary<byte[], byte[]> _after = new(Comparer);
    
    private bool _asserting;
    
    private static readonly BytesEqualityComparer Comparer = new();

    public readonly ComputeMerkleBehavior Merkle = new();

    public void Set(in Key key, ReadOnlySpan<byte> value)
    {
        _before[GetKey(key)] = value.ToArray();
    }

    public void DeleteKey(in Key key) => Set(key, ReadOnlySpan<byte>.Empty);

    public void ShouldBeEmpty() => AssertEmpty(_after, "set of operations");

    public void ShouldHaveSquashedStateEmpty() => AssertEmpty(_history, "squashed state");

    private static void AssertEmpty(Dictionary<byte[], byte[]> dict, string txt)
    {
        if (dict.Count == 0)
            return;

        var sb = new StringBuilder();
        sb.AppendLine($"The {txt} should be empty, but it contains the following keys: ");
        foreach (var (key, value) in dict)
        {
            Key.ReadFrom(key, out Key k);

            sb.Append($"- {k.ToString()}");

            if (k.Type == DataType.Merkle)
            {
                Node.ReadFrom(value, out var type, out var leaf, out var ext, out var branch);
                switch (type)
                {
                    case Node.Type.Leaf:
                        sb.Append($" {leaf.ToString()}");
                        break;
                    case Node.Type.Extension:
                        sb.Append($" {ext.ToString()}");
                        break;
                    case Node.Type.Branch:
                        sb.Append($" {branch.ToString()}");
                        break;
                }
            }

            sb.AppendLine();


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