using FluentAssertions;
using Paprika.Store;

namespace Paprika.Tests.Store;

public abstract class DbAddressListTests<TList> where TList : struct, DbAddressList.IDbAddressList
{
    [Test]
    public void Test()
    {
        const int seed = 13;

        var list = default(TList);
        var rand = new Random(seed);

        for (var i = 0; i < TList.Length; i++)
        {
            list[i] = NextAddress();
        }

        rand = new Random(seed);
        for (var i = 0; i < TList.Length; i++)
        {
            list[i].Should().Be(NextAddress());
        }

        TList.Length.Should().BeGreaterThan(0);

        return;

        DbAddress NextAddress() => DbAddress.Page((uint)rand.Next(0, (int)DbAddressList.Max.Raw));
    }
}

[TestFixture]
public class DbAddressListTestsOf16 : DbAddressListTests<DbAddressList.Of16>;

[TestFixture]
public class DbAddressListTestsOfOf256 : DbAddressListTests<DbAddressList.Of256>;

[TestFixture]
public class DbAddressListTestsOfOf1024 : DbAddressListTests<DbAddressList.Of1024>;