using System.Runtime.InteropServices;
using Paprika.Crypto;

namespace Paprika.Chain;

/// <summary>
/// Commit stats extensions.
/// </summary>
public static class CommitStats
{
    public static void RegisterSetAccount(this Dictionary<Keccak, int> stats, in Keccak account)
    {
        // just register account in the dictionary
        CollectionsMarshal.GetValueRefOrAddDefault(stats, account, out _);
    }

    public static void RegisterSetStorageAccount(this Dictionary<Keccak, int> stats, in Keccak account)
    {
        // add the key and add 1 for storage
        ref var count = ref CollectionsMarshal.GetValueRefOrAddDefault(stats, account, out _);
        count += 1;
    }
}
