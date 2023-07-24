namespace Paprika.Merkle;

/// <summary>
/// Represents an <see cref="ushort"/> backed nibble set.
/// </summary>
public struct NibbleSet
{
    private ushort _value;

    public bool HasNibble(byte nibble) => (_value & (1 << nibble)) != 0;

    public NibbleSet(byte nibbleA)
    {
        _value = (ushort)(1 << nibbleA);
    }
    
    public NibbleSet(byte nibbleA, byte nibbleB)
    {
        _value = (ushort)((1 << nibbleA) | (1 << nibbleB));
    }

    public NibbleSet(ushort rawValue)
    {
        _value = rawValue;
    }

    public bool this[byte nibble]
    {
        get => (_value & (1 << nibble)) != 0;
        set
        {
            if (value)
            {
                _value = (ushort)(_value | (1 << nibble));
            }
            else
            {
                _value = (ushort)(_value & ~(1 << nibble));
            }
        }
    }

    public static implicit operator ushort(NibbleSet set) => set._value;
}