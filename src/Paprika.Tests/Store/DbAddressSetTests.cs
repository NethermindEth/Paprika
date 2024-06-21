using FluentAssertions;
using Paprika.Store;

namespace Paprika.Tests.Store;

public class DbAddressSetTests
{
    [Test]
    public void Dummy()
    {
        const uint max = 1000;

        var set = new DbAddressSet(DbAddress.Page(max));

        for (uint i = 0; i < max; i++)
        {
            var addr = DbAddress.Page(i);
            set[addr].Should().BeTrue();
            set[addr] = false;
        }

        for (uint i = 0; i < max; i++)
        {
            var addr = DbAddress.Page(i);
            set[addr].Should().BeFalse();
        }
    }
}