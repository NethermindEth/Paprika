﻿using System.Buffers.Binary;
using System.Numerics;
using System.Text;

namespace Paprika.Merkle;

/// <summary>
/// Represents a set of nibbles, backed by a single <see cref="ushort"/>.
/// </summary>
public struct NibbleSet
{
    public const int MaxByteSize = 2;
    public const int NibbleCount = 16;

    private ushort _value;

    public NibbleSet(byte nibbleA)
    {
        _value = (ushort)(1 << nibbleA);
    }

    public NibbleSet(byte nibbleA, byte nibbleB)
    {
        _value = (ushort)((1 << nibbleA) |
                          (1 << nibbleB));
    }

    public NibbleSet(byte nibbleA, byte nibbleB, byte nibbleC)
    {
        _value = (ushort)((1 << nibbleA) |
                          (1 << nibbleB) |
                          (1 << nibbleC));
    }

    private NibbleSet(ushort rawValue)
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

    public int SetCount => BitOperations.PopCount(_value);
    public byte SmallestNibbleSet => (byte)BitOperations.TrailingZeroCount(_value);

    public static implicit operator ushort(NibbleSet set) => set._value;
    public static implicit operator Readonly(NibbleSet set) => new(set._value);

    public override string ToString()
    {
        if (_value == 0)
            return "{}";

        var sb = new StringBuilder();
        sb.Append('{');

        for (byte i = 0; i < 16; i++)
        {
            if (this[i])
            {
                sb.Append($"{i:X},");
            }
        }

        sb.Remove(sb.Length - 1, 1);
        sb.Append('}');

        return sb.ToString();
    }

    public readonly struct Readonly : IEquatable<Readonly>
    {
        private const int AllSetValue = 0b1111_1111_1111_1111;
        private readonly ushort _value;

        public Readonly(ushort value)
        {
            _value = value;
        }

        public bool this[byte nibble] => new NibbleSet(_value)[nibble];

        public static Readonly All => new(AllSetValue);

        public bool AllSet => _value == AllSetValue;

        public int SetCount => new NibbleSet(_value).SetCount;
        public byte SmallestNibbleSet => new NibbleSet(_value).SmallestNibbleSet;

        public static implicit operator ushort(Readonly set) => set._value;

        public Span<byte> WriteToWithLeftover(Span<byte> destination)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(destination, _value);
            return destination.Slice(MaxByteSize);
        }

        public static ReadOnlySpan<byte> ReadFrom(ReadOnlySpan<byte> source, out Readonly set)
        {
            set = new Readonly(BinaryPrimitives.ReadUInt16LittleEndian(source));
            return source.Slice(MaxByteSize);
        }

        public bool Equals(Readonly other) => _value == other._value;

        public override bool Equals(object? obj)
        {
            return obj is Readonly other && Equals(other);
        }

        public override int GetHashCode() => _value.GetHashCode();

        public Readonly Set(byte nibble)
        {
            return new NibbleSet(_value)
            {
                [nibble] = true
            };
        }

        public Readonly Remove(byte nibble)
        {
            return new NibbleSet(_value)
            {
                [nibble] = false
            };
        }

        public override string ToString() => new NibbleSet(_value).ToString();
    }
}