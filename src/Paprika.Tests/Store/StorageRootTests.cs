using System.Buffers.Binary;
using FluentAssertions;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Store;

[Platform("Win", Reason = "Memory protection tests work only on windows now")]
public class StorageRootTests : BasePageTests
{
    [Test]
    public void StorageRoot()
    {
        var batch = new TestBatchContext(1, null, true);

        var page = batch.GetNewPage(out _, true);
        page.Header.PageType = PageType.StorageRoot;

        var storage = new StorageRootPage(page);

        var keccaks = new[] { Values.Key0, Values.Key1, Values.Key2, Values.Key3 };

        Page updated = default;

        foreach (var keccak in keccaks)
        {
            var path = NibblePath.FromKey(keccak);

            for (var i = 0; i < 1000; i++)
            {
                Keccak slot = default;
                BinaryPrimitives.WriteInt32LittleEndian(slot.BytesAsSpan, i);
                updated = storage.Set(Key.StorageCell(path, slot), slot.BytesAsSpan, batch);
            }
        }

        updated.Should().Be(page);
    }
}