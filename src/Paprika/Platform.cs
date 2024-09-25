using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Paprika;

public static class Platform
{
    public static void Prefetch(ReadOnlySpan<UIntPtr> addresses, UIntPtr size) => Manager.SchedulePrefetch(addresses, size);

    private static readonly IMemoryManager Manager =
        IsPosix() ? new PosixMemoryManager() : new WindowsMemoryManager();

    private static bool IsPosix() => RuntimeInformation.IsOSPlatform(OSPlatform.Linux) ||
                                     RuntimeInformation.IsOSPlatform(OSPlatform.OSX);

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

        public void SchedulePrefetch(ReadOnlySpan<UIntPtr> addresses, UIntPtr length)
        {
            // TODO:
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
        public unsafe void SchedulePrefetch(ReadOnlySpan<UIntPtr> addresses, UIntPtr length)
        {
            var count = addresses.Length;
            var ptr = stackalloc Win32MemoryRangeEntry[count];
            var span = new Span<Win32MemoryRangeEntry>(ptr, count);

            for (var i = 0; i < span.Length; i++)
            {
                span[i].VirtualAddress = (IntPtr)addresses[i];
                span[i].NumberOfBytes = length;
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
        /// Schedules an OS dependent prefetch.
        /// </summary>
        void SchedulePrefetch(ReadOnlySpan<UIntPtr> addresses, UIntPtr length);
    }
}