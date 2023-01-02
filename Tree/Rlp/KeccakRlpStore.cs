using System.Runtime.InteropServices;

namespace Tree.Rlp;

public unsafe class KeccakRlpStore : IDisposable
{
    private readonly void* _data;
    private readonly long _dataLength;
    private readonly byte[] _dirtyMap;

    private const int KeccakSize = 32;
    private const int BitsPerByte = 8;
    private const int NibbleCount = 16;
    
    public KeccakRlpStore(long maxMemorySize)
    {
        var lastLevel = 0;
        
        _dataLength = 0L;
        
        while (true)
        {
            var memoryForLevel = KeccakSize * (long)Math.Pow(NibbleCount, lastLevel + 1);

            if (_dataLength + memoryForLevel > maxMemorySize)
            {
                break;
            }

            _dataLength += memoryForLevel;
            lastLevel++;
        }
        
        _data = NativeMemory.Alloc((UIntPtr)_dataLength);

        var dirtyMapSize = _dataLength / KeccakSize / BitsPerByte;
        _dirtyMap = new byte[dirtyMapSize];
        
        GC.AddMemoryPressure(_dataLength);
        
        LastLevel = lastLevel;
    }

    public int LastLevel { get; }

    public void SetDirty(NibblePath branchPath, byte nibble)
    {
        
    }

    public void Dispose()
    {
        GC.RemoveMemoryPressure(_dataLength);
        NativeMemory.Free(_data);
    }
}