using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika;

/// <summary>
/// Describes an unmanaged range.
/// </summary>
/// <param name="Pointer">The pointer to prefetch.</param>
/// <param name="Length">The length to prefetch.</param>
public record struct AddressRange(UIntPtr Pointer, uint Length);

public static class Platform
{
    public static bool CanPrefetch => Manager != null;

    /// <summary>
    /// Schedules an OS dependent prefetch. Performs a sys-call and should not be called from the main thread.
    /// </summary>
    /// <param name="ranges"></param>
    public static void Prefetch(ReadOnlySpan<AddressRange> ranges) => Manager!.SchedulePrefetch(ranges);

    private static readonly IMemoryManager? Manager = CreateManager();

    private static IMemoryManager? CreateManager()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            return new WindowsMemoryManager();

        return RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ? new PosixMemoryManager() : null;
    }

    private sealed class PosixMemoryManager : IMemoryManager
    {
        [Flags]
        private enum Advice : int
        {
            // ReSharper disable InconsistentNaming
            /// <summary>
            ///  Expect access in the near future. (Hence, it might be a good idea to read some pages ahead.)
            /// </summary>
            MADV_WILLNEED = 0x3,

            /// <summary>
            ///  Do not expect access in the near future.
            /// (For the time being, the application is finished with the given range,
            /// so the kernel can free resources associated with it.)
            /// </summary>
            MADV_DONTNEED = 0x4,
            // ReSharper restore InconsistentNaming
        }

        [DllImport("LIBC_6", SetLastError = true)]
        static extern int madvise(IntPtr addr, UIntPtr length, Advice advice);

        // For Linux
        [DllImport("libc", SetLastError = true)]
        private static extern IntPtr __errno_location();

        // For macOS
        [DllImport("libc", SetLastError = true)]
        private static extern IntPtr __error();

        public static int GetErrno()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                return Marshal.ReadInt32(__errno_location());
            }

            if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
            {
                return Marshal.ReadInt32(__error());
            }

            throw new PlatformNotSupportedException("This platform is not supported.");
        }

        public void SchedulePrefetch(ReadOnlySpan<AddressRange> ranges)
        {
            const int success = 0;

            foreach (var t in ranges)
            {
                var result = madvise((IntPtr)t.Pointer, t.Length, Advice.MADV_WILLNEED);
                if (result != success)
                {
                    throw new SystemException($"{nameof(madvise)} failed with the following error: {GetErrno()}");
                }
            }
        }
    }

    private sealed class WindowsMemoryManager : IMemoryManager
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern unsafe bool PrefetchVirtualMemory(IntPtr hProcess, ulong numberOfEntries,
            Win32MemoryRangeEntry* entries, int flags);

        [StructLayout(LayoutKind.Sequential)]
        private struct Win32MemoryRangeEntry
        {
            /// <summary>
            /// Starting address of the memory range
            /// </summary>
            public IntPtr VirtualAddress;

            /// <summary>
            /// Size of the memory range in bytes
            /// </summary>
            public UIntPtr NumberOfBytes;
        }

        [SkipLocalsInit]
        public unsafe void SchedulePrefetch(ReadOnlySpan<AddressRange> ranges)
        {
            var count = ranges.Length;
            var ptr = stackalloc Win32MemoryRangeEntry[count];
            var span = new Span<Win32MemoryRangeEntry>(ptr, count);

            for (var i = 0; i < span.Length; i++)
            {
                span[i].VirtualAddress = (IntPtr)ranges[i].Pointer;
                span[i].NumberOfBytes = ranges[i].Length;
            }

            const int reserved = 0;

            if (PrefetchVirtualMemory(Process.GetCurrentProcess().Handle, (ulong)count, ptr, reserved) == false)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }
        }
    }

    private interface IMemoryManager
    {
        /// <summary>
        /// Schedules an OS dependent prefetch. Performs a sys-call and should not be called from the main thread.
        /// </summary>
        void SchedulePrefetch(ReadOnlySpan<AddressRange> addresses);
    }
}