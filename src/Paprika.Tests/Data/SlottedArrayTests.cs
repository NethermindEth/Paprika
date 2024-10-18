using FluentAssertions;
using Paprika.Crypto;
using Paprika.Data;
using Paprika.Store;

namespace Paprika.Tests.Data;

public class SlottedArrayTests
{
    private static ReadOnlySpan<byte> Data0 => new byte[] { 23 };
    private static ReadOnlySpan<byte> Data1 => new byte[] { 29, 31 };

    private static ReadOnlySpan<byte> Data2 => new byte[] { 37, 39 };

    private static ReadOnlySpan<byte> Data3 => new byte[] { 31, 41 };
    private static ReadOnlySpan<byte> Data4 => new byte[] { 23, 24, 25 };
    private static ReadOnlySpan<byte> Data5 => new byte[] { 23, 24, 64 };

    [Test]
    public Task Set_Get_Delete_Get_AnotherSet()
    {
        var key0 = Values.Key0.Span;

        Span<byte> span = stackalloc byte[SlottedArray.MinimalSizeWithNoData + key0.Length + Data0.Length];
        var map = new SlottedArray(span);

        map.SetAssert(key0, Data0);

        map.GetAssert(key0, Data0);

        map.DeleteAssert(key0);
        map.GetShouldFail(key0);

        // should be ready to accept some data again
        map.SetAssert(key0, Data1, "Should have memory after previous delete");
        map.GetAssert(key0, Data1);

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public void Bug()
    {
        var map = new SlottedArray(stackalloc byte[Page.PageSize - 8]);

        var path0 = NibblePath.Parse("498FBC24196F969E81A7C5F8672622C2FE26549A3445F5F681EF66E5D9361234");
        Span<byte> data0 = [13];

        var path1 = NibblePath.Single(3, 1);
        Span<byte> data1 = [51];

        map.SetAssert(path0, data0);
        map.SetAssert(path1, data1);

        map.GetAssert(path0, data0);
        map.GetAssert(path1, data1);
    }

    [Test]
    public Task Enumerate_all([Values(0, 1)] int odd)
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.FromKey([7]).SliceFrom(odd);
        var key2 = NibblePath.FromKey([7, 13]).SliceFrom(odd);
        var key3 = NibblePath.FromKey([7, 13, 31]).SliceFrom(odd);
        var key4 = NibblePath.FromKey([7, 13, 31, 41]).SliceFrom(odd);

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);
        map.SetAssert(key2, Data2);
        map.SetAssert(key3, Data3);
        map.SetAssert(key4, Data4);

        map.GetAssert(key0, Data0);
        map.GetAssert(key1, Data1);
        map.GetAssert(key2, Data2);
        map.GetAssert(key3, Data3);
        map.GetAssert(key4, Data4);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key0).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key1).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data1).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key2).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data2).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key3).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data3).Should().BeTrue();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key4).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data4).Should().BeTrue();

        e.MoveNext().Should().BeFalse();

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Enumerate_nibble([Values(1, 2, 3, 4)] int nibble)
    {
        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.FromKey([0x1A]);
        var key2 = NibblePath.FromKey([0x2A, 13]);
        var key3 = NibblePath.FromKey([0x3A, 13, 31]);
        var key4 = NibblePath.FromKey([0x4A, 13, 31, 41]);

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);
        map.SetAssert(key2, Data2);
        map.SetAssert(key3, Data3);
        map.SetAssert(key4, Data4);

        map.GetAssert(key0, Data0);
        map.GetAssert(key1, Data1);
        map.GetAssert(key2, Data2);
        map.GetAssert(key3, Data3);
        map.GetAssert(key4, Data4);

        var expected = nibble switch
        {
            1 => key1,
            2 => key2,
            3 => key3,
            4 => key4,
            _ => throw new Exception()
        };

        var data = nibble switch
        {
            1 => Data1,
            2 => Data2,
            3 => Data3,
            4 => Data4,
            _ => throw new Exception()
        };

        using var e = map.EnumerateNibble((byte)nibble);

        e.MoveNext().Should().BeTrue();

        e.Current.Key.Equals(expected).Should().BeTrue();
        e.Current.RawData.SequenceEqual(data).Should().BeTrue();

        e.MoveNext().Should().BeFalse();

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public void Enumerate_2_nibbles([Values(1, 2, 3, 4)] int nibble0)
    {
        const byte nibble1 = 0xA;

        Span<byte> span = stackalloc byte[256];
        var map = new SlottedArray(span);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.FromKey([0x10 | nibble1]);
        var key2 = NibblePath.FromKey([0x20 | nibble1, 13]);
        var key3 = NibblePath.FromKey([0x30 | nibble1, 13, 31]);
        var key4 = NibblePath.FromKey([0x40 | nibble1, 13, 31, 41]);

        map.SetAssert(key0, Data0);
        map.SetAssert(key1, Data1);
        map.SetAssert(key2, Data2);
        map.SetAssert(key3, Data3);
        map.SetAssert(key4, Data4);

        map.GetAssert(key0, Data0);
        map.GetAssert(key1, Data1);
        map.GetAssert(key2, Data2);
        map.GetAssert(key3, Data3);
        map.GetAssert(key4, Data4);

        var expected = nibble0 switch
        {
            1 => key1,
            2 => key2,
            3 => key3,
            4 => key4,
            _ => throw new Exception()
        };

        var data = nibble0 switch
        {
            1 => Data1,
            2 => Data2,
            3 => Data3,
            4 => Data4,
            _ => throw new Exception()
        };

        using var e = map.Enumerate2Nibbles((byte)nibble0, nibble1);

        e.MoveNext().Should().BeTrue();

        e.Current.Key.Equals(expected).Should().BeTrue();
        e.Current.RawData.SequenceEqual(data).Should().BeTrue();

        e.MoveNext().Should().BeFalse();
    }

    [Test]
    public Task Enumerate_long_key([Values(0, 1)] int oddStart, [Values(0, 1)] int lengthCutOff)
    {
        Span<byte> span = stackalloc byte[128];
        var map = new SlottedArray(span);

        var key = NibblePath.FromKey(Keccak.EmptyTreeHash).SliceFrom(oddStart)
            .SliceTo(NibblePath.KeccakNibbleCount - oddStart - lengthCutOff);

        map.SetAssert(key, Data0);
        map.GetAssert(key, Data0);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(key).Should().BeTrue();
        e.Current.RawData.SequenceEqual(Data0).Should().BeTrue();

        e.MoveNext().Should().BeFalse();

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Set_Get_Empty()
    {
        var key0 = Values.Key0.Span;

        Span<byte> span = stackalloc byte[128];
        var map = new SlottedArray(span);

        var data = ReadOnlySpan<byte>.Empty;

        map.SetAssert(key0, data);
        map.GetAssert(key0, data);

        map.DeleteAssert(key0);
        map.GetShouldFail(key0);

        // should be ready to accept some data again
        map.SetAssert(key0, data, "Should have memory after previous delete");
        map.GetAssert(key0, data);

        using var e = map.EnumerateAll();

        e.MoveNext().Should().BeTrue();
        e.Current.Key.Equals(NibblePath.FromKey(key0));
        e.Current.RawData.SequenceEqual(data).Should().BeTrue();

        e.MoveNext().Should().BeFalse();

        // verify
        return Verify(span.ToArray());
    }

    private const int DefragmentationKeyCount = 27;

    [TestCase(0, TestName = "0th")]
    [TestCase(1, TestName = "1st")]
    [TestCase(2, TestName = "2nd")]
    [TestCase(DefragmentationKeyCount - 1, TestName = "last")]
    public void Defragmentation(int deleted)
    {
        // The size found by running the test multiple times.
        Span<byte> span = stackalloc byte[1330];
        var map = new SlottedArray(span);

        var rand = new Random(13);

        // Init values
        var keys = new Keccak[DefragmentationKeyCount];
        var data = new byte[DefragmentationKeyCount][];

        for (var i = 0; i < DefragmentationKeyCount; i++)
        {
            rand.NextBytes(keys[i].BytesAsSpan);
            data[i] = new byte[i]; // make them vary by length
            rand.NextBytes(data[i]);

            map.SetAssert(keys[i].Span, data[i], $"at {i}th iteration");
        }

        // Additional data
        var additionalKey = Keccak.Zero;
        rand.NextBytes(additionalKey.BytesAsSpan);
        byte[] additionalData = [rand.NextByte()];

        // Ensure that one needs to be deleted
        map.TrySet(NibblePath.FromKey(additionalKey), additionalData).Should()
            .BeFalse("There should be no space in the map to make the defragmentation test work");

        map.Delete(NibblePath.FromKey(keys[deleted])).Should().BeTrue("The key should have existed before");

        // Now insertion should be possible
        map.TrySet(NibblePath.FromKey(additionalKey), additionalData).Should().BeTrue();

        for (var i = 0; i < DefragmentationKeyCount; i++)
        {
            if (i != deleted)
            {
                map.GetAssert(keys[i].Span, data[i]);
            }
        }

        map.GetAssert(additionalKey.Span, additionalData);
    }

    [Test]
    public void Defragmentation_bug()
    {
        var map = SlottedArray.LoadFromDump(
            "7801881d02000000ffff8bd400190014302c353c3f3c0012602e8bcf902c9b1c102e001ddc1d0016b83f50ffcd7e2a7ee87d80fd627d7f7c17fcaffb917b29fb0b7b887a20fa5d79dedf8c3989e90018cc5f001fc02f89d98939895989a9302e001e303e60be606ef5f8d7786ff84c77e4f6e175c3755bf559753cb51fb501753e74d6f3b9b351f360ee429417f44024102430243e04802460246ad48c39a029a6a900290d39286934b3ccf264f2467228720a72a2f184716671fef096f0787010f0f26f8aef22ef20298029ffe5a8dc099c002ca02c3fac353ca34d0a7d002da02d902d9fadd02d046fc26e7f2d17edafec916c736c0beced6b85eb1debff6ae16ac36a5bea3d6ab4ee9c3e902eb02ef02efbfe105e602e603e475f6e5f402fb02fbc0f633f6e3f18eab0e9926974695669eee886e88368816819e8b1e7936775670de7ef66d166633f602f6fcf6f3f225f202f3cdf302f802fd02f6adc602c001c0010001c001b69e60766e2e5c4655ce53e65d6e4b8649a647c6414e4f66353633062cd617d62e7a4ffe6f2c6dda0ffe0dfd050a950c9ff9e4a7296e36bf29d25ef5bda0b5749d42efb7395e63bee40036072546c72d436df9ff6e98f84f70c88802009147943ab78e8c8b0787d0db004045f6100490648c2edac468b2790e108f7c7e197a93bd80df00424cd51f36184d31b283aca70176730cbb89962b5a74d7f1b77ba39cb58d4a3ba34d9d7a7991d597ba44b1d321931acfbb83997f82bdc280e18655542824cc72ace5b410d4f5f0ecbfeac2322007b895b9dbc6037a0a4d711ba51780b00bf0b47a717f967c9ecd45f42144112cc32a411fa5bd8c47f1ea18c665cc55b47ce170a11ced61ef333a3c2c3ab09881b1af3883362e9307e97840388fae664593792d057ca9642e864830a8aade324b81c0dd18a19cd555bbf41e26f5721a7e0c314c1d6852dcfe8337d9e5e535c3173a16af047aa0078e2245c2607349ddcb3ef7ad4d50f8bd9b7fd5a2c20e988e30bd0bed3e73fafc0c029b40181a4a7eb99a22449a50aafb74c96748ef880119b3f757375de1dd9450b36e6970494ece083188c3e516a22766432661fd96aa2151a88bf6ee5bf8849806d2d9098bd4135dbcfdb61e5de2958585602a2c4354b15370fe319a76f3c66773635fb727f85fcc42c721fe66cad66e3758e28a518cd96b8dfc800490648c2edac468b2790e108f7c7e197a93bd80df00424cd51f36184d31b283aca70176730cbb89962b5a74d7f1b77ba39cb58d4a3ba34d9d7a7991d597ba44b1d321931acfbb83997f82bdc280e18655542824cc72ace5b410d4f5f0ecbfeac2322205d7a6936cb8531ad6d27454012b09d7950cc76bfde56880496401d43bdbc09550603272bf202c67394b1e6dd764ab9c976e7a330dcbe69c7d078c8d67dfe3f9abf4f4f0f59f6e0bd7cf3550954fbcccd70ab3b86a73024ec186b0f58ed72bf4f4f0f59f6e0bd7cf3550954fbcccd70ab3b86a73024ec186b0f58e004045a46540b5a46540bc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219e0bbc86f405f600e966a36ca13aa927149524a9552b09bc82558a8fea5d9bb3898a03ea1968c72be7eaf1caa712ea0deec17af0980378a99936f1fc9c581289c71f7b9e15871d9e96f520038ad8b46a620c6ab6187fed1e892d72581289c71f7b9e15871d9e96f520038ad8b46a620c6ab6187fed1e892004042849491128494911c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219204c50ff450b8864924074304a9fca25ac5c970821beb05499f74c99d857204c50ff450b8864924074304a9fca25ac5c970821beb05499f74c99d80040445b0d28a45b0d28ac5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42180613c9612d6dfe7badec7c7d80ea03f6d853f4a132761f1a36389a0ec1c72613c9612d6dfe7badec7c7d80ea03f6d853f4a132761f1a36389a0ec10b383e21883e21833fe354e44292cc8a2b8c8c332cf0df23a108bb2a8ef6b94f7bab68cfc69c1727cade1d7a32d0e17e8b039c53e67673f272ebbde87147db3dbafe229a61b434c537687aa1b1843a48e1cc4031cd2582a966971f4886bb898bb097577b1b57f32d9dc720ffbdc6a778bd09cb15d3853e4882685f9cf6f6ca77490c43be6b59c20040446cdb69e46cdb69ec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42180c22bc50012cba127e4f264f0aae1ce24de60b33c739c004957bde0c7d5800ffbdc6a778bd09cb15d3853e4882685f9cf6f6ca77490c43be6b59c2372c29dcaa75be414aebd8ac8b7b79cdb133f2568f56706f6f1ecd663e0b00404643ae6fc643ae6fcc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219cc29dcaa75be414aebd8ac8b7b79cdb133f2568f56706f6f1ecd663e0b097d35d9f1fcf5c868a8fb20f5a239c0d98b65d2b6ebc65886a9219431fd572c22bc50012cba127e4f264f0aae1ce24de60b33c739c004957bde0c7d0040424967bc624967bc6c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42172d35d9f1fcf5c868a8fb20f5a239c0d98b65d2b6ebc65886a9219431fd004043c4c2e533c4c2e53c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4213bee40036072546c72d436df9ff6e98f84f70c88802009147943ab78e8c8b0787d0db004045f6142c45f6142c4c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4217247dd4e4a7544d15faa007defab535c7ce58c2c402955f97ef2ed928a5004042375c1812375c181c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219b47dd4e4a7544d15faa007defab535c7ce58c2c402955f97ef2ed928a5f9481cc56f3e612670057c5b2522a54d8edee3d7f533f93a5041ebbe6869e9c8a1dac03a4a4fb0a9873a7a47a2b992b4ec07ea8320468d2d2d8ab8713728a1dac03a4a4fb0a9873a7a47a2b992b4ec07ea8320468d2d2d8ab87100404147fe2ee147fe2eec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4217281cc56f3e612670057c5b2522a54d8edee3d7f533f93a5041ebbe68690b3e54e34e54e349c06449d84ef49ab3fe9ba4609d5ba85329fa56100808626e2577adc2d9172eca3d56a192cdec34654487d9aa17072edfce77ea6f32831d99355da20040402cc9b0d02cc9b0dc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219feca3d56a192cdec34654487d9aa17072edfce77ea6f32831d99355da2a93fbfbe5b55a93899c38eb80cb27d082daa074e139e646f4334282752ae49a54d3adf14ae80c43b3ef9b31730795e51b153d8c9890b0658b186396277254d3adf14ae80c43b3ef9b31730795e51b153d8c9890b0658b1863962004042bc856f82bc856f8c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42172fbfbe5b55a93899c38eb80cb27d082daa074e139e646f4334282752ae004045b9aaba35b9aaba3c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42180cb1ab037075dce1d4268abe42f0c8b9845d9dd12275e6cd91a1daacc637204b9038819104a9d61a779c4c629a1238cef412c5ea15f5f201493be7004044f4bdf874f4bdf87c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42198c88ed6068a0687c9b3582ec4e2aff4ccf5c2899211a2eff260f9de477d990fd9b0ac7c67dce1c809acbe45d356c595c8847bfa232b45f305462699720fd9b0ac7c67dce1c809acbe45d356c595c8847bfa232b45f3054626900404076a1c35076a1c35c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42172c88ed6068a0687c9b3582ec4e2aff4ccf5c2899211a2eff260f9de4770040403a7640f03a7640fc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421009bdc65e150ad36d8eaec51694d3240afd4c1908b90a4a792153a8bf72c0f54c368f2fa3f734b99bb107f7c00a272570efb6e0e84843a200df9a96378cf2d97e927cf0b3243ab8e2e9ecbaf0d4f3f54ad18e33e5d784ec99cbade13a45bc88390ce471c81bc5ec013c36961bf4dc1fefa2d023e676ae399510175b149fd88fdacc67fb137a4c0d6d108fd4e1a78332dc0657b1058d24689f42991f89de559792cdb5a6681b5c214942fc56a0dbe96d89e1efe306a8bced101abe9cce16e94489634f75dc744d9e9789677dba1437f2671c33b1c79a479928a20c7245e94a716926c84e59511d6eea30a1cc270c11cb3093988d751b674b5369d0a14ba01f6ebbbe1c3445bfe89eb4e823e9f579c0360a264f1a99ba02c4c9deb5c554605258485b7532990738d6fd4e7a443a1e0458248ecced1aa7a3cf0135203229ba49bf014857b39cc0263bbb3a5335d74ee6926236059ae4d32bee4b43cd4bfc766d657af40300d906f85f3c7043eea45edb9c444979fb6e99bdc9ce3d6c560a3a399de6fbf4e8cbf989bc786452966adad7716f4df1c00624ef6c42030bd5e595a315afa4ed6729bc786452966adad7716f4df1c00624ef6c42030bd5e595a315afa4ed00404181e39cf181e39cfc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421725cad6695a75d513357829194ef91a06b6557e86f9edd56bcd40a9f5a9004045d2978915d297891c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219d5cad6695a75d513357829194ef91a06b6557e86f9edd56bcd40a9f5a9372e9c160d184b9091445442545c39836289abf6e812c50c662992fa8f6a004047abe55d87abe55d8c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42196e9c160d184b9091445442545c39836289abf6e812c50c662992fa8f6aa72aac12cb6315edc38123a7425da6868629613d5248fa759bfd85adda9f00404507e930f507e930fc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4217213d59669b6e5daedd20eb13ba061cd73f7947ff23d0fbf0f2fbffb6de004045b1fb3b55b1fb3b5c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219a13d59669b6e5daedd20eb13ba061cd73f7947ff23d0fbf0f2fbffb6ded9bb2c7c1c834df3d8c8daac9a14f660a627068065af79293d0157459744d72517654f81ed3728044229b9c2c2a079fdbb9abd151f15f4841a9be8550040460f4386e60f4386ec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219e517654f81ed3728044229b9c2c2a079fdbb9abd151f15f4841a9be8550976c012929b6b5d6fd8d233565b5b91faae9f24ff39b223c752fe506477f926e601443251913cc3e1bb07757eb8368add4d2389f1836887418876df9726c012929b6b5d6fd8d233565b5b91faae9f24ff39b223c752fe5064770040416fbec6b16fbec6bc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421726e601443251913cc3e1bb07757eb8368add4d2389f1836887418876df0040476cbcace76cbcacec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42191fdfd65f7bd2cc89f8be33490726758cd9c57c5aa68a67b156603d45272b81b54e7d76dbfb2c189d3243fbbae937a43fd97a1cb1962de6dccfbc004045c6db2ec5c6db2ecc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421981b54e7d76dbfb2c189d3243fbbae937a43fd97a1cb1962de6dccfbc6723403d2023ee72a950ffbd2c4ec37848171ace9c9fadad91315fe9f62c004041a5b380d1a5b380dc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421004a8ab7c316ae3abdd0bce06646caf2d8fd99386831c8e87ce93dac1976941ee947dd315631d5554c75e6a325b4de9df2c4d74cea47fbc167691592c7d33f7debc3f2edffc84dab4ecf1fef704750dc8fb6d052bfc3c91fff9eeb6a08cf2c5a3dbcb19303df85cbbcb70eca45cef7492160e6cea70a167e0757a1b969c4f7d02c8e048c7c7e35ad63dd864ce04f84890f88118d4eb5d7513a334284dbac8f693d1c17996325e5fa43c2b031d0aa10cd7a359af8c74f93080c790536068367d0e419a1903403d2023ee72a950ffbd2c4ec37848171ace9c9fadad91315fe9f62c399440f0088dc960264f6a596135f82a4a5429da815a0495d14dff7e6bd9a4dc6b4a1b28676b7297332ae061588ede20c584aae6d8fc4c645393e35aa72a9440f0088dc960264f6a596135f82a4a5429da815a0495d14dff7e6b0040465df37ee65df37eec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219cc98a85f2eba57fa17c35e9d6a2a99d458ea1d1d14df58cc6cf098461b5005c39cedec2e331a0f140f243056450aeb7186014482d832ce3e2a45d1514a7f2b3586bcd5ea3866d32954c2bf8fcd440228a222d236967a1c364aba87926d7be336081469ca5c8b6bbe613453cb34d246dd5b4df3ff65a5ffdb6ac1408a1ddc6d9a136f9de40d0d6c215e29428ec514f3a2a84919fce712588f83afc1253045c555bd5d5ef4bccfcf69d236c06219b5e62a8109e0f5237a825bb36aa33de70d46102303a641fb67f20587a486124010cc8c613b7e4dd9eea7126e32a95fc87e72a9d7a26f3c4f6217b4320f8869448c027f787500ff9f9e1cc56237860e410fd82c28b7542e0c7eeacc4dc4e1cd31c06c510515567aeaffe0147b2cc1d8ac95315d272c98a85f2eba57fa17c35e9d6a2a99d458ea1d1d14df58cc6cf098461b004042d8665522d866552c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421003e56603f6f8a5cc797aac091480a9a48f83cbc5edbe26e1d7a4cdd5b50b2ac1bbb348f2b98219e11827a04cf8681e01fcc6d556b74d7ca974c733ce3adc813383b9d053b97ebdcd7d66adb6525d54b8716dbf7bec379277acf40c8f6a918d75783ba29db2ad8c2f99ebcf3d457b18532f0b0cc6c5d98abe38c3b9cd6fdc1135b059237ca619c42e968c4f792788ff693407d3217f49d95121ce88dbb86a5c66ee41fc90b431890d58d8925a2ba86b2693f497da978ed6231840c1b214f4f15441d1921ec2ebbffe06debb3a1e9cc88111a189ceb5fd3a8fe9be57346db627b508167256a9025206da6f94bf48a5023f818bd41bd3e77dbc5acafd4dac2ee06348dad4ad6c208bb10abae26c84bda9e9606ff7e96e609bd80db70eb5086f017adb39b725a4dc6b4a1b28676b7297332ae061588ede20c584aae6d8fc4c645393004043e55f6553e55f655c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42180aac12cb6315edc38123a7425da6868629613d5248fa759bfd85adda9f3720bbc86f405f600e966a36ca13aa927149524a9552b09bc82558a8fea50040467620b8367620b83c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421004455f3e60d87efb0c86dd351bf6eacaedf5942f185d3fed8d4f8159f4c54e61bf89094c3cd1eab022f4f2bb634a09fba81c84b2bbfa41fa5e238b970ffc2db6dc00c1543a6d888da0199b95bf0c7b5a9c11f39a71cce3e307287229128d821acfa9b507dc4b63ebdbd2e550aa26532ea6708870067d3cb3558d915c48723fd9b76168bc1ff3d11e82f3dd160245a811d41c47189b0a770ec29c96ca3af9d20e0c7b58a15a903e8de11842b57a64c54c7b0007a526e377294c8aff29424d8ca1a055c7206449d84ef49ab3fe9ba4609d5ba85329fa56100808626e2577adc2d9004045cea95725cea9572c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4210001261b2734885ff390b54a64126b4f3eec24503706acb6b8150d2acbca05332c4afa1e0cb66f4e723cfe7a99445fe7c06f6606ac11ba7e026656f991f367ceb07700b3e7bcc1c31fb7303691cf23159be96b5fd7bf35472501841e7d25ebde596b2de955a6dfe6fbdf69a4ff72ab0516fb1cea2b45eaebfef1210feaffa6df64109390546c72d436df9ff6e98f84f70c88802009147943ab78e8c8b0787d0db572d5c2d4ec4bfd43373528d2c70a0a28d4062244a0c0fe1965e70b4f6090040467400bd367400bd3c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219bd5c2d4ec4bfd43373528d2c70a0a28d4062244a0c0fe1965e70b4f609172b3898a03ea1968c72be7eaf1caa712ea0deec17af0980378a99936f1f00404221bc027221bc027c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42172e1fdfd65f7bd2cc89f8be33490726758cd9c57c5aa68a67b156603d450040472136c9572136c95c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42100d0e205245529cabe6ce34bbdcc5d5d1b49e747fc5dace5cd609a8d31036f5c55a6466e7501e41f710b96ca853f9ef3d6fcbd766c5b8af1ae2404d615f414988139c98a3e8c84a6b7b8483eba3ebb78d70a177b677cf3745560bdb9964e66feb121dad30117e836e436242c15fd42b5fd0468dd665ee773662b707a7cc0857a9b52f9a3cb18e737101ed6aca28e9ec7e0f09bdebad74b55040b0add2333fc2fb8176c74bbd70f1debfcc4cf2f834a3b3f4aa60720e0519e7e4e9c814c3724a1e57decd925a57bd532cc39f016836d392b9ae90fb8f4a60d09dc10418624d9165f69c78004b9038819104a9d61a779c4c629a1238cef412c5ea15f5f201493be7a72cb1ab037075dce1d4268abe42f0c8b9845d9dd12275e6cd91a1daacc6004045336091353360913c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42135ffc4d1b93882cd4ab3c1ac53209405ed5a2b90a10102df27a8e34f44bed2f64a620820d85415da88a79f0ae73e213cbf3cfb1ccff52fcb17477d2a2292699e2821005a01c59fa2330aa6dd6279b10eaf5220ce95a36ea5658fe57782a44d2d0e8546278c744f108cb386b2f2e89f1daf8f5d6a71fe889b4d1e1cedd1116c6151f47a3b5de7396845415250f10e3e192866754ea7b15f52ff70a7a015247c47174920aedfc7aeff64d84cb43043487fc12a4db924553495849847c99d3e2b10a0ea55099bdeed88c557943b7dbe04b57c02aec077117d7a75f02682ecbaae3ee82cb219580005052aae9689cd299769aaa5bdf316e51745cd74391206435e34b4a79ed6d8d8e8e664e35e18f874cf65255b8688cd66fdcc470d12a9714edbfd95e0cf836b97ea1002b3f2efb0a3a3e533fb784ee76030f74bf98bb3598f700a287b563404ca658bfe5ee40554ebb9526d6b56bfe0f053002a371a34f6dd8260f2d6886db3b43f0e72b2c7c1c834df3d8c8daac9a14f660a627068065af79293d015745974400404340869d2340869d2c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421");

        map.SetAssert(NibblePath.Single(0xA, 1), new byte[355]);
    }

    [Test]
    public Task Update_in_situ()
    {
        // by trial and error, found the smallest value that will allow to put these two
        Span<byte> span = stackalloc byte[128];
        var map = new SlottedArray(span);

        var key1 = Values.Key1.Span;

        map.SetAssert(key1, Data1);
        map.SetAssert(key1, Data2);

        map.GetAssert(key1, Data2);

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Update_in_resize()
    {
        var key0 = Values.Key0.Span;

        // Update the value, with the next one being bigger.
        Span<byte> span = stackalloc byte[SlottedArray.MinimalSizeWithNoData + key0.Length + Data0.Length];
        var map = new SlottedArray(span);

        map.SetAssert(key0, Data0);
        map.SetAssert(key0, Data2);

        map.GetAssert(key0, Data2);

        // verify
        return Verify(span.ToArray());
    }

    [Test]
    public Task Small_keys_compression()
    {
        Span<byte> span = stackalloc byte[512];
        var map = new SlottedArray(span);

        Span<byte> key = stackalloc byte[1];
        Span<byte> value = stackalloc byte[2];

        const int count = 34;

        for (byte i = 0; i < count; i++)
        {
            key[0] = i;
            value[0] = i;
            value[1] = i;

            map.SetAssert(key, value, $"{i}th was not set");
        }

        for (byte i = 0; i < count; i++)
        {
            key[0] = i;
            value[0] = i;
            value[1] = i;

            map.GetAssert(key, value);
        }

        // verify
        return Verify(span.ToArray());
    }

    [TestCase(0)]
    [TestCase(1)]
    public void Key_of_length_5(int odd)
    {
        const int length = 5;

        // One should be enough as the leftover path of length 1 should be encoded as a single byte 
        const int spaceForKey = 1;

        Span<byte> span = stackalloc byte[SlottedArray.MinimalSizeWithNoData + spaceForKey];

        var key = NibblePath.FromKey(stackalloc byte[] { 0x34, 0x5, 0x7A }, odd, length);

        var map = new SlottedArray(span);

        var value = ReadOnlySpan<byte>.Empty;
        map.SetAssert(key, value);
        map.GetAssert(key, value);
    }

    [Test]
    public void Key_of_length_6_even()
    {
        const int length = 6;

        // One should be enough as the leftover path of length 1 should be encoded as a single byte 
        const int spaceForKey = 1;

        Span<byte> span = stackalloc byte[SlottedArray.MinimalSizeWithNoData + spaceForKey];

        // 0b10 is the prefix of the nibble that can be densely encoded on one byte.
        var key = NibblePath.FromKey(stackalloc byte[] { 0x34, 0b1001_1101, 0x7A }, 0, length);

        var map = new SlottedArray(span);

        var value = ReadOnlySpan<byte>.Empty;
        map.SetAssert(key, value);
        map.GetAssert(key, value);
    }

    [Test]
    public void Key_of_length_6_odd()
    {
        const int length = 6;

        // One should be enough as the leftover path of length 1 should be encoded as a single byte 
        const int spaceForKey = 1;

        Span<byte> span = stackalloc byte[SlottedArray.MinimalSizeWithNoData + spaceForKey];

        // 0b10 is the prefix of the nibble that can be densely encoded on one byte. For odd, first 3 are consumed to prepare.
        var key = NibblePath.FromKey(stackalloc byte[] { 0x04, 0b1011_0010, 0xD9, 0x7A }, 0, length);

        var map = new SlottedArray(span);

        var value = ReadOnlySpan<byte>.Empty;
        map.SetAssert(key, value);
        map.GetAssert(key, value);
    }

    [Test(Description = "Make a lot of requests to make breach the vector count")]
    public void Breach_VectorSize_with_key_count()
    {
        const int seed = 13;
        var random = new Random(seed);
        Span<byte> key = stackalloc byte[4];

        var map = new SlottedArray(new byte[3 * 1024]);

        const int count = 257;

        for (var i = 0; i < count; i++)
        {
            random.NextBytes(key);
            map.SetAssert(key, [(byte)(i & 255)]);
        }

        // reset
        random = new Random(seed);
        for (var i = 0; i < count; i++)
        {
            random.NextBytes(key);
            map.GetAssert(key, [(byte)(i & 255)]);
        }
    }

    [Test(Description = "Make a lot of requests to make breach the vector count")]
    public void Set_Get_With_Specific_Lengths([Values(8, 16, 32, 64, 68, 72)] int count)
    {
        const int keyLength = 2;

        Span<byte> keys = stackalloc byte[count * 2];
        for (byte i = 0; i < count; i++)
        {
            keys[i * keyLength] = i;
            keys[i * keyLength + 1] = i;
        }

        var map = new SlottedArray(new byte[3 * 1024]);

        for (var i = 0; i < count; i++)
        {
            map.SetAssert(GetKey(keys, i), GetValue(i));
        }

        for (var i = 0; i < count; i++)
        {
            map.GetAssert(GetKey(keys, i), GetValue(i));
        }

        return;

        static NibblePath GetKey(Span<byte> keys, int i) => NibblePath.FromKey(keys.Slice(i * keyLength, keyLength));
        static ReadOnlySpan<byte> GetValue(int i) => new byte[(byte)(i & 255)];
    }

    private static ReadOnlySpan<byte> Data(byte key) => new[] { key };

    [TestCase(new[] { 1 })]
    [TestCase(new[] { 2, 4 })]
    [TestCase(new[] { 0, 1, 7 })]
    public void Remove_keys_from(int[] indexes)
    {
        var toRemove = new SlottedArray(stackalloc byte[512]);
        var map = new SlottedArray(stackalloc byte[512]);

        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");
        var key6 = NibblePath.Parse("56899A");
        var key7 = NibblePath.Parse("56899AB");
        var key8 = NibblePath.Parse("56899AB1");

        // Set receiver with all the keys
        map.SetAssert(NibblePath.Empty, Data(0));
        map.SetAssert(key1, Data(1));
        map.SetAssert(key2, Data(2));
        map.SetAssert(key3, Data(3));
        map.SetAssert(key4, Data(4));
        map.SetAssert(key5, Data(5));
        map.SetAssert(key6, Data(6));
        map.SetAssert(key7, Data(7));
        map.SetAssert(key8, Data(8));

        foreach (var index in indexes)
        {
            var removed = index switch
            {
                0 => NibblePath.Empty,
                1 => key1,
                2 => key2,
                3 => key3,
                4 => key4,
                5 => key5,
                6 => key6,
                7 => key7,
                8 => key8,
                _ => default
            };
            toRemove.SetAssert(removed, ReadOnlySpan<byte>.Empty);
            map.Contains(removed).Should().BeTrue();
        }

        map.RemoveKeysFrom(toRemove);

        // Assert non existence
        foreach (var index in indexes)
        {
            var removed = index switch
            {
                0 => NibblePath.Empty,
                1 => key1,
                2 => key2,
                3 => key3,
                4 => key4,
                5 => key5,
                6 => key6,
                7 => key7,
                8 => key8,
                _ => default
            };
            map.Contains(removed).Should().BeFalse();
        }
    }

    [Test]
    public void Move_to_1()
    {
        var original = new SlottedArray(stackalloc byte[256]);
        var copy0 = new SlottedArray(stackalloc byte[256]);

        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        original.SetAssert(NibblePath.Empty, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new MapSource(copy0));

        // original should have only empty
        original.Count.Should().Be(1);
        original.GetAssert(NibblePath.Empty, Data(0));
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy should have all but empty
        copy0.Count.Should().Be(5);
        copy0.GetShouldFail(NibblePath.Empty);
        copy0.GetAssert(key1, Data(1));
        copy0.GetAssert(key2, Data(2));
        copy0.GetAssert(key3, Data(3));
        copy0.GetAssert(key4, Data(4));
        copy0.GetAssert(key5, Data(5));
    }

    [Test]
    public void Move_to_SlottedArray()
    {
        var original = new SlottedArray(stackalloc byte[256]);
        var copy0 = new SlottedArray(stackalloc byte[256]);

        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        original.SetAssert(NibblePath.Empty, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo<NibbleSelector.All>(copy0);

        // original should have only empty
        original.Count.Should().Be(1);
        original.GetAssert(NibblePath.Empty, Data(0));
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy should have all but empty
        copy0.Count.Should().Be(5);
        copy0.GetShouldFail(NibblePath.Empty);
        copy0.GetAssert(key1, Data(1));
        copy0.GetAssert(key2, Data(2));
        copy0.GetAssert(key3, Data(3));
        copy0.GetAssert(key4, Data(4));
        copy0.GetAssert(key5, Data(5));
    }

    [Test]
    public void Move_to_respects_tombstones()
    {
        const int size = 256;

        var original = new SlottedArray(stackalloc byte[size]);
        var copy0 = new SlottedArray(stackalloc byte[size]);

        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        var tombstone = ReadOnlySpan<byte>.Empty;

        original.SetAssert(key1, tombstone);
        original.SetAssert(key2, tombstone);
        original.SetAssert(key3, tombstone);
        original.SetAssert(key4, tombstone);
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new MapSource(copy0), true);

        // original should have only empty
        original.Count.Should().Be(0);
        original.CapacityLeft.Should().Be(size - SlottedArray.HeaderSize);
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy should have all but empty
        copy0.Count.Should().Be(1);
        copy0.GetShouldFail(key1);
        copy0.GetShouldFail(key2);
        copy0.GetShouldFail(key3);
        copy0.GetShouldFail(key4);
        copy0.GetAssert(key5, Data(5));
    }

    [Test]
    public void Move_to_2()
    {
        var original = new SlottedArray(stackalloc byte[256]);
        var copy0 = new SlottedArray(stackalloc byte[256]);
        var copy1 = new SlottedArray(stackalloc byte[256]);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        original.SetAssert(key0, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new MapSource(copy0, copy1));

        // original should have only empty
        original.Count.Should().Be(1);
        original.GetAssert(key0, Data(0));
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy0 should have key2 and key4 and nothing else
        copy0.Count.Should().Be(2);
        copy0.GetShouldFail(key0);
        copy0.GetShouldFail(key1);
        copy0.GetAssert(key2, Data(2));
        copy0.GetShouldFail(key3);
        copy0.GetAssert(key4, Data(4));
        copy0.GetShouldFail(key5);

        // copy1 should have key1 and key3 and key5 and nothing else
        copy1.Count.Should().Be(3);
        copy1.GetShouldFail(key0);
        copy1.GetAssert(key1, Data(1));
        copy1.GetShouldFail(key2);
        copy1.GetAssert(key3, Data(3));
        copy1.GetShouldFail(key4);
        copy1.GetAssert(key5, Data(5));
    }

    [Test]
    public void Move_to_4()
    {
        var original = new SlottedArray(stackalloc byte[256]);
        var copy0 = new SlottedArray(stackalloc byte[256]);
        var copy1 = new SlottedArray(stackalloc byte[256]);
        var copy2 = new SlottedArray(stackalloc byte[256]);
        var copy3 = new SlottedArray(stackalloc byte[256]);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");

        original.SetAssert(key0, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));

        original.MoveNonEmptyKeysTo(new MapSource(copy0, copy1, copy2, copy3));

        // original should have only empty
        original.Count.Should().Be(1);
        original.GetAssert(key0, Data(0));
        original.GetShouldFail(key1);
        original.GetShouldFail(key2);
        original.GetShouldFail(key3);
        original.GetShouldFail(key4);
        original.GetShouldFail(key5);

        // copy0 should have key4
        copy0.Count.Should().Be(1);
        copy0.GetShouldFail(key0);
        copy0.GetShouldFail(key1);
        copy0.GetShouldFail(key2);
        copy0.GetShouldFail(key3);
        copy0.GetAssert(key4, Data(4));
        copy0.GetShouldFail(key5);

        // copy1 should have key1 and key5 and nothing else
        copy1.Count.Should().Be(2);
        copy1.GetShouldFail(key0);
        copy1.GetAssert(key1, Data(1));
        copy1.GetShouldFail(key2);
        copy1.GetShouldFail(key3);
        copy1.GetShouldFail(key4);
        copy1.GetAssert(key5, Data(5));

        // copy1 should have key2 and nothing else
        copy2.Count.Should().Be(1);
        copy2.GetShouldFail(key0);
        copy2.GetShouldFail(key1);
        copy2.GetAssert(key2, Data(2));
        copy2.GetShouldFail(key3);
        copy2.GetShouldFail(key4);
        copy2.GetShouldFail(key5);

        // copy1 should have key2 and nothing else
        copy3.Count.Should().Be(1);
        copy3.GetShouldFail(key0);
        copy3.GetShouldFail(key1);
        copy3.GetShouldFail(key2);
        copy3.GetAssert(key3, Data(3));
        copy3.GetShouldFail(key4);
        copy3.GetShouldFail(key5);
    }

    [Test]
    public void Move_to_8()
    {
        var original = new SlottedArray(stackalloc byte[512]);
        var copy0 = new SlottedArray(stackalloc byte[128]);
        var copy1 = new SlottedArray(stackalloc byte[128]);
        var copy2 = new SlottedArray(stackalloc byte[128]);
        var copy3 = new SlottedArray(stackalloc byte[128]);
        var copy4 = new SlottedArray(stackalloc byte[128]);
        var copy5 = new SlottedArray(stackalloc byte[128]);
        var copy6 = new SlottedArray(stackalloc byte[128]);
        var copy7 = new SlottedArray(stackalloc byte[128]);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");
        var key6 = NibblePath.Parse("6789AB");
        var key7 = NibblePath.Parse("789ABCD");
        var key8 = NibblePath.Parse("89ABCDEF");

        original.SetAssert(key0, Data(0));
        original.SetAssert(key1, Data(1));
        original.SetAssert(key2, Data(2));
        original.SetAssert(key3, Data(3));
        original.SetAssert(key4, Data(4));
        original.SetAssert(key5, Data(5));
        original.SetAssert(key6, Data(6));
        original.SetAssert(key7, Data(7));
        original.SetAssert(key8, Data(8));

        original.MoveNonEmptyKeysTo(new MapSource(copy0, copy1, copy2, copy3, copy4, copy5, copy6, copy7));

        // original should have only empty
        HasOnly(original, 0);

        HasOnly(copy0, 8);
        HasOnly(copy1, 1);
        HasOnly(copy2, 2);
        HasOnly(copy3, 3);
        HasOnly(copy4, 4);
        HasOnly(copy5, 5);
        HasOnly(copy6, 6);
        HasOnly(copy7, 7);

        return;

        static void HasOnly(in SlottedArray map, int key)
        {
            map.Count.Should().Be(1);

            for (byte i = 0; i < 8; i++)
            {
                var k = i switch
                {
                    0 => NibblePath.Empty,
                    1 => NibblePath.Parse("1"),
                    2 => NibblePath.Parse("23"),
                    3 => NibblePath.Parse("345"),
                    4 => NibblePath.Parse("4567"),
                    5 => NibblePath.Parse("56789"),
                    6 => NibblePath.Parse("6789AB"),
                    7 => NibblePath.Parse("789ABCD"),
                    _ => NibblePath.Parse("89ABCDEF")
                };

                if (i == key)
                {
                    map.GetAssert(k, Data(i));
                }
                else
                {
                    map.GetShouldFail(k);
                }
            }
        }
    }

    [Test]
    public void Hashing()
    {
        var hashes = new Dictionary<ushort, string>();

        // empty
        Unique("");

        // single nibble
        Unique("A");
        Unique("B");
        Unique("C");
        Unique("7");

        // two nibbles
        Unique("AC");
        Unique("AB");
        Unique("BC");

        // three nibbles
        Unique("ADC");
        Unique("AEB");
        Unique("BEC");

        // four nibbles
        Unique("ADC1");
        Unique("AEB1");
        Unique("BEC1");

        // 5 nibbles, with last changed
        Unique("AD0C2");
        Unique("AE0B2");
        Unique("BE0C2");

        // 6 nibbles, with last changed
        Unique("AD00C4");
        Unique("AE00B4");
        Unique("BE00C4");

        return;

        void Unique(string key)
        {
            var path = NibblePath.Parse(key);
            var hash = SlottedArray.HashForTests(path);

            if (hashes.TryAdd(hash, key) == false)
            {
                Assert.Fail($"The hash for {key} is the same as for {hashes[hash]}");
            }
        }
    }

    [TestCase(0, 0)]
    [TestCase(0, 1)]
    [TestCase(1, 1)]
    [TestCase(0, 2)]
    [TestCase(1, 2)]
    [TestCase(0, 3)]
    [TestCase(1, 3)]
    [TestCase(0, 4)]
    [TestCase(1, 4)]
    [TestCase(0, 6)]
    [TestCase(1, 6)]
    [TestCase(0, 64)]
    [TestCase(1, 63)]
    [TestCase(1, 62)]
    public void Prepare_UnPrepare(int sliceFrom, int length)
    {
        var key = NibblePath.FromKey(Keccak.EmptyTreeHash).Slice(sliceFrom, length);

        // prepare
        var hash = SlottedArray.PrepareKeyForTests(key, out var preamble, out var trimmed);
        var written = trimmed.IsEmpty ? ReadOnlySpan<byte>.Empty : trimmed.WriteTo(stackalloc byte[33]);

        Span<byte> working = stackalloc byte[32];

        var unprepared = SlottedArray.UnPrepareKeyForTests(hash, preamble, written, working, out var data);

        data.IsEmpty.Should().BeTrue();
        key.Equals(unprepared).Should().BeTrue();
    }

    [Test]
    public void Dump_and_load()
    {
        var map = new SlottedArray(stackalloc byte[512]);

        var key0 = NibblePath.Empty;
        var key1 = NibblePath.Parse("1");
        var key2 = NibblePath.Parse("23");
        var key3 = NibblePath.Parse("345");
        var key4 = NibblePath.Parse("4567");
        var key5 = NibblePath.Parse("56789");
        var key6 = NibblePath.Parse("6789AB");
        var key7 = NibblePath.Parse("789ABCD");
        var key8 = NibblePath.Parse("89ABCDEF");

        map.SetAssert(key0, Data(0));
        map.SetAssert(key1, Data(1));
        map.SetAssert(key2, Data(2));
        map.SetAssert(key3, Data(3));
        map.SetAssert(key4, Data(4));
        map.SetAssert(key5, Data(5));
        map.SetAssert(key6, Data(6));
        map.SetAssert(key7, Data(7));
        map.SetAssert(key8, Data(8));

        var dump = map.DumpToHexString();
        var loaded = SlottedArray.LoadFromDump(dump);

        loaded.GetAssert(key0, Data(0));
        loaded.GetAssert(key1, Data(1));
        loaded.GetAssert(key2, Data(2));
        loaded.GetAssert(key3, Data(3));
        loaded.GetAssert(key4, Data(4));
        loaded.GetAssert(key5, Data(5));
        loaded.GetAssert(key6, Data(6));
        loaded.GetAssert(key7, Data(7));
        loaded.GetAssert(key8, Data(8));
    }

    [Test]
    public void Bug_defragmentation()
    {
        const string dump =
            "4801ae1d03000000ffff302e001e102f165f0014001712770018eb35e025a4d5ae35001a0012001cb83f9a7ff77e957e2dfe2a7d477cdffb9c7a7e7a3c7a6cf94e794b7868772576001004c5402500254835e5c5ae05e535a435a0250015c68777b7c0277027002702759af47c745e74f6f38ef326f30873ea72a8722572bdf155f137711971fb700117f027ff8789b78027a7c7a027102770bb302b702b334bc02bc76bf02bfe9b93f075700df0a5ef876f1fef016fe36e7bee5d6e3f6ed7edb96d51ed336dcbec802b82bb502b001b538ba6cec02ea02ecede702e777e802e86ce313edf8ff02fad6c45ec276c646bfcea94ea766a586af0e9d2696ae94c69e4e87ce814e8f667d02ff90f761f702f101f123f124f103f402f001fb02fbc4fd1dedfdeffee0033d86770e708e7ea6682e66466fce5de65586577645964f1e389236b2388226a620e330023b65db63db83d001db83d802b101f001f102f37333023377391d800184c620a62a5e2876269628661a6619b611c2699257b255d25fb2493242be408639028b8ed001d0078989e3c5257906fedd8b7cc8329726eb4dade6d728ce1dd7c6333156a6065eb52983f99d7f09c24196f969e81a7c5f8672622c2fe26549a3445f5f681ef66e5d93612340092a8060b16daeac109fd355b2dd4e2142080633d3ee5b0657ab528bf02a6f9750d0d84b47b3148addf530343673aba46f1e55f087eaf003211269041268f30ee2ef17d947fe8307e694d80f2f9c8c95bdf700e9781930b7fd216a82aa94728ec9e2b06ef64e4e545b77325ee9684ab2798841916d17b8f4494603dcda0cf3c02803e91fc797392f754462df5a523b9992c0c0f21a8449e21329c5ee4fdf8808ba8e90e71a8947d682394a50a991fe9f79ae7333a8122df016186e29100cc86af1929838542f2555284ee86210b6a5c9e03656eb3abe212e33085331bbc7e9ba945015ea4d8f9f088a3421f7205a4147d42915df50ae54201acde31771f0a09e1eedccf51e9971a2cc23ed6a32f7a1fc2e27ddcc4a570dae0566ee28c12ff9069e88f2e8bf121113ea02d9503e7874faa9da5697fb8d4d8d20dd355b9610b0c862c5b8a7b6fe2ebd65a3fc26a3844e38eacb248a89be6a77d8ca3a0161dbb81c6afe0aac1a2fce7fc32f1fa99954bc955ab4f3a7f98ed8b4b932540a37fe5a1e857da039c2e6ac69b7638845ac3802e54d845d511b0eb4fd0a22d1148e7897d9e64e5fc3ea29ba53526085d1bd58342127764808f673b508e70991e10127264e5fc3ea29ba53526085d1bd58342127764808f673b508e70991e101004041bf755e51bf755e5c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4217224196f969e81a7c5f8672622c2fe26549a3445f5f681ef66e5d9361230040478176c3978176c39c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219c24196f969e81a7c5f8672622c2fe26549a3445f5f681ef66e5d93612340092a8060b16daeac109fd355b2dd4e2142080633d3ee5b0657ab528bf02a6f9750d0d84b47b3148addf530343673aba46f1e55f087eaf003211269041268f356eed0242beda4350642d534e226b06dc44a1dfb42c554e6cc19693163e940fb2533d7644852f53e6ea43500a7a3be99c4d7c016b473dd0ed8ab08e2ca0ccd7d7a71240e1e6b2abab352a90494ce27582b479f7395faff2aed061fb2456053d20dc4fa6e7968e836953acb17b6549bfdb2027a8992b2e6cfd40455c8512be6e2ba45ad480fe12bffaf0e04c7d3c6a37fd7fd1f3f03634f4686056d14b54cb3cd5859036bcc7f07b82afb112fa3f96ee9e7251af5cf45f61eb7664e16f5474de7236bcc7f07b82afb112fa3f96ee9e7251af5cf45f61eb7664e16f5474d004041aaafeb11aaafeb1c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42180e5a46ea8dbe58cd6a1e046cc502bacec24a676960bd9c0e6e0f107fe91721fd236df9fe422fa12bacdafbcd25517a36237218d252eae4a38371da0040469fe48bd69fe48bdc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421801fd236df9fe422fa12bacdafbcd25517a36237218d252eae4a38371da472e5a46ea8dbe58cd6a1e046cc502bacec24a676960bd9c0e6e0f107fe900404166280de166280dec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421960ac2023ad68b9549ff3accb4fc1cf56c90a5d91e73649daa9fe6f5fa71720ac2023ad68b9549ff3accb4fc1cf56c90a5d91e73649daa9fe6f5fa700404499c22a8499c22a8c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421723ab35684153e8882627c98a291fcae07de3eaa39dda87f779acfc28b00040458c7b36e58c7b36ec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219f45d383124746074ff29dca9b0e07b9560be862c874b676c2f0b38cdf58993ab35684153e8882627c98a291fcae07de3eaa39dda87f779acfc28b007245d383124746074ff29dca9b0e07b9560be862c874b676c2f0b38cdf500404674c417f674c417fc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421725ea42bc8056dfd0b19073a89e69d9dc3397c991521ab2464c6731a2b0004043b644d3e3b644d3ec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4217276b175a5b582fe2f6cc2de8e70398ebc9f354232b64edb7048c8644bd0040463ea463e63ea463ec5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219676b175a5b582fe2f6cc2de8e70398ebc9f354232b64edb7048c8644bdc72e36dd976537ad740470d63091a95ab27864327fb0f2db97ed0d2db4c10040437803bd037803bd0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42197e36dd976537ad740470d63091a95ab27864327fb0f2db97ed0d2db4c1772aa096d1e1240ecf593ecaa2cdfb270f6b30e93fafbae698b6eb287eb70040436e2093536e20935c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421966b1c4a78b21ec5a3cc274f88ed7ace5264263c0a1d55f1e9f467339a5c9eaa096d1e1240ecf593ecaa2cdfb270f6b30e93fafbae698b6eb287eb7d726b1c4a78b21ec5a3cc274f88ed7ace5264263c0a1d55f1e9f467339a5004042424a7c52424a7c5c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42172de29e554c622a96ed733f93e9c0043a52b7b1548de7477117bb255a88004046e48bbff6e48bbffc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42100a89133f09832f441588d60990cad726b08923b613aae89f1ce1d58adc7d7490f1b67596a099ed030cbf257aae406ab76ac838281fb3a186ac499c1faa806524697e44e7ee6a980381c9051a05425195c0bc6fed0604cce017ac1eaafa01c90c9816940a45b1f58cf35e478bfa649a28c94dc8faabf105602653bb1a40120eb96ced1813b87123ee98d398e1ac8376d4e64c206c589df029c5f95215378e7c43c82592ade75e3a3eb03387f778cc0b600ef74f0a49cc4cbaa8758b4dd83681d5c6eba93de29e554c622a96ed733f93e9c0043a52b7b1548de7477117bb255a888720cd0794d71eb34bf56293e2f213fd20ac0a6c54b47832c2f89a6bb048004047900499479004994c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421920cd0794d71eb34bf56293e2f213fd20ac0a6c54b47832c2f89a6bb048b7267e4cc66823b78f6c3fd238a6bbc5b1a510fe65c350617efe701efc70004041755b6ed1755b6edc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219e67e4cc66823b78f6c3fd238a6bbc5b1a510fe65c350617efe701efc709724c927fb50bdf9c9372220fc191391df4e34e23bb77ee51ce75a07609a004047018765f7018765fc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421974c927fb50bdf9c9372220fc191391df4e34e23bb77ee51ce75a07609a67282a0a2bb213de4d0324ce595de51417e3f288a524ebf5e0b9c3fb727600404038e550a038e550ac5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42190b989b276f21aada31a49a442563d1e4eff3fa8e3d543630c6ce0463c6b9382a0a2bb213de4d0324ce595de51417e3f288a524ebf5e0b9c3fb7276472b989b276f21aada31a49a442563d1e4eff3fa8e3d543630c6ce0463c6004043cb24a163cb24a16c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421922cabb33c183274512df1e82d059362f4c6e07c7acd71753298350ddf8797a33ca2792665c3120bd365bb75c23ec8ad77b3968d22760dd44118e4dc72a33ca2792665c3120bd365bb75c23ec8ad77b3968d22760dd44118e4d0040464a5e5eb64a5e5ebc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42199acad1325229a31d45e8d6aa42ce02ce1cdaf60799a1b66985a99d2931b72acad1325229a31d45e8d6aa42ce02ce1cdaf60799a1b66985a99d293100404116a74c7116a74c7c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4217229892065ad76c53599d47a24206f6d152e45d17fca60235fc517f1dea004046a396f046a396f04c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4219f29892065ad76c53599d47a24206f6d152e45d17fca60235fc517f1dea872adeee882d44e9d958311fd25c78a1b92eec45a72e14e455953af70fe600404327a6ab0327a6ab0c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42191adeee882d44e9d958311fd25c78a1b92eec45a72e14e455953af70fe6197dbbf5feb9857f2ce28d2ff864010c24edabdc85c389eee3e53b74f596b960f4dd7306c41361340938277914ed50a1967fcee9b2f592c99bd0d083872dbbf5feb9857f2ce28d2ff864010c24edabdc85c389eee3e53b74f596004046b28d7956b28d795c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421720f4dd7306c41361340938277914ed50a1967fcee9b2f592c99bd0d083004041b480b131b480b13c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421001144503a8ede8d0af60dea36bf33d9164935f2c61492fb4d3182231dd1fe9ad4f6a36790d50ae9ccf16b8c4386444f0ade6ffa275941daa3cfadbf3f93df7eba5f6bca59b2b65e16e03a52fd18e1e09dd083449c8a6e3501b9365455eca9f1ae4c6b26f11c33feb42393c7539a150a133eb1f571078990b3c60f3675ac7afa0c426434ee855cfdaf9c91fc268ea88245b0da5d768ceea6c9b6a0170d3125219737b7de077d8d2bfcf1c077d64c20f88e740e30979a574be9f014559843587f30da62069c802b2439ecef2a0cb6f62faa9e08db456499b99bbbbcf33391f3e8bc351d8084a741b9a3611033f2624b3fa921122209f1573b2f8031074f659df82c72e88b2a25c3eebfc24c15d198f0c52ae8a972af4ac44b49eaa24b5a8f10040462ad9eb162ad9eb1c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4217284a741b9a3611033f2624b3fa921122209f1573b2f8031074f659df82004045dc539e25dc539e2c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421727eaf72e2b85b5b75f00890af0cb2ed330eea08022cf556d7aec877cee004044282acc84282acc8c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42194e2fd057667beb0be78f94e617499828b2f17dc144d35c55f8b15f0b99c987eaf72e2b85b5b75f00890af0cb2ed330eea08022cf556d7aec877cee372e2fd057667beb0be78f94e617499828b2f17dc144d35c55f8b15f0b9900404458bc200458bc200c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42100f83a251935a72eb9797b4e69d7b0d0be25dbe6ee3a1cb92ddb676b1abb465cfe05f330af6f33d30a5147a5032111aed6ef45930c32d24e5ceb677f2288189ffe964116484dabe8479c2e0dbeca2dd9fa2b218b1571aa963a330ff3d68fe1a399942f1d7a0ddb477dd73c2069f74afe46743856f4e6845b9d62653d1fd16e4713e1ab0684c9428e204eb01c9223cd56342d86e083d1cfa4542f645ebc82f92ec5690e34daa3b7369dfcb88eafb97dfb24369ebf13028e52f42cd435a61ee7e44fed304bdaff23573114180e956efe165aaad8e91f65c3baa2f765eef2f4b5dbd95240fd0575824aeb690442b5487d72889af7edc998f4cad5cffc5518f4a68884fe921f6ef29b961b5191295dac099df60c7c2c8c25edc6f28ed0f3113810624488ac00ef921011f56b51752bca8de4a9e3c624b9a2a6752a9ca8ffd1da6a7ae4e49fbf85a21e1d6b06904df0016ceb7526f9d09d4d5f1df73b91fe28ec19616103e04e5b4578b791aba4f2a90ed7f101c0e292d309d743c2f15ddefd6ba8455d2da3802e18be0a0c07428d5c8ce1d9f8a35bdef11e77a2c3c1f2de53534bce6ccee13be33dbf1083351834bc24acd614c1fda3b1933b6fdd2cfd738eb13fd49784f75bbb59f37ab2d7af98082ae089289f4b98081f7d9489db44720626e0a41e3eb486cca630d8900a8d3c4f65ab8bb2091057fc20d849985cf2e9bebfbc39963bd3e4af2b649abc73f13f0f2de6fb4989bf92369dd296399a2ca16bec874f8eb68c038bf08d9beb63123c37c5b3f1439dc75ca2dec6b76a379fefec8b243f5d0e50914a8b3b23369f37a2512262d7d4fb16ad4ce7ec7f3a2686f866cdca00319f873fb0e40028760c8f2c9af1ac6d491d2f3ad3c075fc5f40b8fdd28ff93589fa56868a21f1728f526c2e3db8165a909a4e549d95d2dd4adf0b5da48b1291662ad623eb10ce61926731f1ad3459910c951f1b9fbc579f7e3c44ea61a239ab8861e4ce5fe04a181bbc92b30c166d81aeac4f7d22d31a6ef4e47cc64c15fb338a6e5e5409dadfaaac52ea8429dea7810dc86b1a1d1187dec1aaf959dfbc7eded286f47c24cd8317c25c1f35012dbc56422435fedc819c8e54f3463f14e219b0aa7dc9dbb084d50300f1492f31ce0b73162da92d605353c1299463e08c00d6c0346c0e50860674827d009f0305a22ceea4c93ac67c3683586ab80d48ded8687f7e09949e83e8ee6d8cbf2014a87862bdd3b0dbc131092bcee6f825df59889eef159889d8a4941509a987e8da68b0250084cb0191d93c8fd5aeb63f80c14b5fe33dfdf4b1d9dda0896b607da6dfdf000ddc9acf6d0084db72305fda209667fcfcd0694bfad033c6d313e44596587e42f2188cb57a18acdb8d6fa2396b42fcb61dace4335913a6d07e18808704ac0ba640785b115b5f20516e21795907e305afe57d968b20ae12a754b3e1b33e07ab18ed00bac218a7a19ace6362bbdd67fe2fc406111fcdb100db8b969526b7d578f8e4ffa19143f69dbfd68ae4ce0a0858c4637084f4b1dbb6e549c7ef2fe480e88b2a25c3eebfc24c15d198f0c52ae8a972af4ac44b49eaa24b5a8f10722b2439ecef2a0cb6f62faa9e08db456499b99bbbbcf33391f3e8bc351004043fbada9d3fbada9dc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42172c404230ce0b28fda25c4c13ea16892724b18fc68cf68f3cc9cff778430040425bfb87725bfb877c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42135bb5439983322ad26e1cd3a48a7e8aacbc3f7556f213dc2dac70d79575abc2466ca53c7c5f1fa465a9b5b8de2fbd3385d7644f211a83ea51bead5e5f9a76c0b123d80c404230ce0b28fda25c4c13ea16892724b18fc68cf68f3cc9cff778434006ea7b70e3e52ba6387606fd959515c980ee9a4d9f6a06e4e119b1e1c9ea91c02cd28f89c7020dcb5bc2abe6edfc9d329b5eedf95f12c25de09f8681e21ea7bf790a84e8663d7175fe4a4517551608adaa59ab48366fe3fb17e52afa630d618e211e12b15da2d6379390d4fbf0b7a15f9dbee5245421dd740500591c7129f57c93c79d4a457e0af475d9d599db92bcbf81e9bd637ea17c7a2d0d002afb2254d36b5dd54741b15bbdc9dcb4ebaba267ee2881fc64169adc45a982b90859c8259790687c0aca1fbc9f8895a23ddd4239868a744f51b02a8cf7306a4cdbd597f5a9c9cb9e58b15648aa7f5d604c2a4a4ee888220b4292aa0c4f7308b9c8e2655e3b3924d65f0957801df85ccc64d0debe5f16eaef8a74c97312d123d79818a88ee0f0a68fdc8effea9ed92807dffbc9f8a86a7afae02522f3a6ca54fef54a075b34ef90f722cabb33c183274512df1e82d059362f4c6e07c7acd71753298350ddf8004040439dc250439dc25c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b42100839540b6b4aabdc6dc3258c15e75cd9dd6926823210c8d0880322c49d9c6d88b21e41d55d67c7603315538523497c5f6c5f9c9c24549234233c9d807a2c2a9f38f9dcb09742c9d0479276c1fb1ec20ba8bcf10edc950f2c42c0185bf843b1c78a84b14196fb3c678ad61bc59706d09e5323a97afa871bfab3c1c9cb5a8c70cb7d0e43ba0c461f1001c79c72a81816332bb2424dc94dd5188315303400751490a6dfe25f3acca065ab23c62228f1891d9aa5671b549422feaed9feb2317ea1019927c953768626d81800dc467633a892959b4a29a5d8326236f9190e6804f13d1c5c200bf04e24317ccb2c057cd852d42e7ef987608cbf988d63cfa1469ec9e8f789fb70f8a31d2f1f17a2951cae9631a943753edd2eef0b1ec57d765bfb24dd3f98122fc7e5bc30a58c3607e75ef165c8160da3d8ac9bf32b9582446c0e28dc0f34c53e47c94a6994b88d57b7ed69d516691ac18c0cb6dc1d21fd014517c254952a7f414675660870271f9d7c78573d2f919f486688dbb3354bdfa677510d55e73661df13570b797a3e69b2ee7b53a346fe257164446c1e90f735153c2c06ff9b19d5159ca625b44f6ab1e701c05e6c1bb07f9a015256e485a036da220ba2327a8045016e1b77f608effdbbb75474b6da4ac9f2e2835bec49781ebafcaf4e3ff26766f4eab72fe12bffaf0e04c7d3c6a37fd7fd1f3f03634f4686056d14b54cb3cd58004041165dd7d1165dd7dc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a47056e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b4213062034b73f3450bb5ca91ed5be8c1f5cbb8faf6f287a9b0ca944ae5247ac9cf969a183b2ab721f6b2bec6f0e4a46ffa0fd2bb748f4e9a326749525a53737d1aca44f3fe1d22a0b3f6825b8b80cb0952502d642be43d66e22361db9bc78402866f7e0088155b72f42e8972a7c755a550747f30e9fb96e98c27f94a8ed1b92133342c25f4c9928a5bd87fd672a758980ea6babafd65a1986342ae93e0ca8d228474353949c357e68be4ff4c93f5f170fcb9988551d228976b67e402961c30f4fe5f5cbaf41d315405ec3ca36556e05bb3e8b50a05d4c8ea9687ac6bc3477350b2d789dde7895b39db2fb0a7d15b95f9fc4837d9930ef33172496b631fa4d08e99aab14a0b9c915ea42bc8056dfd0b19073a89e69d9dc3397c991521ab2464c6731a2b03";

        var map = SlottedArray.LoadFromDump(dump);
        var key = NibblePath.Parse("498FBC24196F969E81A7C5F8672622C2FE26549A3445F5F681EF66E5D9361234");
        var search = key.SliceFrom(3);

        map.Contains(search).Should().BeTrue();

        map.SetAssert(NibblePath.Single(3, 1), stackalloc byte[512]);

        map.Contains(search).Should().BeTrue();
    }
}

file static class FixedMapTestExtensions
{
    public static void SetAssert(this SlottedArray map, in NibblePath key, ReadOnlySpan<byte> data,
        string? because = null)
    {
        map.TrySet(key, data).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void SetAssert(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> data,
        string? because = null)
    {
        map.TrySet(NibblePath.FromKey(key), data).Should().BeTrue(because ?? "TrySet should succeed");
    }

    public static void DeleteAssert(this SlottedArray map, in ReadOnlySpan<byte> key)
    {
        map.Delete(NibblePath.FromKey(key)).Should().BeTrue("Delete should succeed");
    }

    public static void GetAssert(this SlottedArray map, in ReadOnlySpan<byte> key, ReadOnlySpan<byte> expected)
    {
        var retrieved = map.TryGet(NibblePath.FromKey(key), out var actual);
        retrieved.Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }

    public static void GetAssert(this SlottedArray map, in NibblePath key, ReadOnlySpan<byte> expected)
    {
        var retrieved = map.TryGet(key, out var actual);
        retrieved.Should().BeTrue();
        actual.SequenceEqual(expected).Should().BeTrue("Actual data should equal expected");
    }


    public static void GetShouldFail(this SlottedArray map, in ReadOnlySpan<byte> key)
    {
        map.TryGet(NibblePath.FromKey(key), out _).Should().BeFalse("The key should not exist");
    }

    public static void GetShouldFail(this SlottedArray map, in NibblePath key)
    {
        map.TryGet(key, out _).Should().BeFalse("The key should not exist");
    }
}