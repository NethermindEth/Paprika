// ReSharper disable once CheckNamespace
namespace Paprika.Data;

public readonly ref partial struct Key
{
    public static Key Merkle(NibblePath path) => new(path, DataType.Merkle, NibblePath.Empty);
}