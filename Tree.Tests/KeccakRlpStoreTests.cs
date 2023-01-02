using NUnit.Framework;
using Tree.Rlp;

namespace Tree.Tests;

public class KeccakRlpStoreTests
{
    [Test]
    public void T()
    {
        var store = new KeccakRlpStore(2 * 1024 * 1024 * 1024L);
    }
}