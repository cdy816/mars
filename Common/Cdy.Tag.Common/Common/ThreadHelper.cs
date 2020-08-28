//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/7/20 13:44:40.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ThreadHelper
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// CPU 个数
        /// </summary>
        /// <returns></returns>
        public static int GetProcessNumbers()
        {
            return Environment.ProcessorCount;
        }

        /// <summary>
        /// 将当前线程绑定到指定CPU
        /// </summary>
        /// <param name="cpus"></param>
        public static void AssignToCPU(params int[] cpus)
        {
            if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                SetThreadAffinityMaskWindows((UIntPtr)MaskFromIds(cpus));
            }
            else if(RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                SetThreadAffinityMaskLinux(MaskFromIds(cpus));
            }
            else
            {
                //to do none;
            }
        }

        /// <summary>
        /// 获取当前线程所在CPU ID
        /// </summary>
        /// <returns></returns>
        public static uint GetCurrentProcessorNumber()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return Win32Native.NtGetCurrentProcessorNumber();
            }
            else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                //ulong re = 0;
                //LinuxNative.pthread_getaffinity_np(LinuxNative.pthread_self(), ref re);
                //return (uint)re;
            }
            else
            {
                //to do none;
            }
            return uint.MaxValue;
        }

        /// <summary>
        ///     Sets a processor affinity mask for the current thread.
        /// </summary>
        /// <param name="mask">
        ///     A thread affinity mask where each bit set to 1 specifies a logical processor on which this thread is allowed to
        ///     run.
        ///     <remarks>Note: a thread cannot specify a broader set of CPUs than those specified in the process affinity mask.</remarks>
        /// </param>
        /// <returns>
        ///     The previous affinity mask for the current thread.
        /// </returns>
        /// <exception cref="Win32Exception"></exception>
        public static UIntPtr SetThreadAffinityMaskWindows(UIntPtr mask)
        {
            var threadAffinityMask = Win32Native.SetThreadAffinityMask(Win32Native.GetCurrentThread(), mask);
            if (threadAffinityMask == UIntPtr.Zero)
            {
                throw new Win32Exception(Marshal.GetLastWin32Error());
            }

            return threadAffinityMask;

            //return SetThreadAffinityMask(Win32Native.GetCurrentThread(), mask);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="mask"></param>
        public static void SetThreadAffinityMaskLinux(ulong mask)
        {
            LinuxNative.pthread_setaffinity_np(LinuxNative.pthread_self(),8,ref mask);
        }

        /// <summary>
        ///     Masks from ids.
        /// </summary>
        /// <param name="ids">The ids.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException">CPUId</exception>
        private static ulong MaskFromIds(IEnumerable<int> ids)
        {
            ulong mask = 0;
            foreach (var id in ids)
            {
                if (id < 0 || id >= Environment.ProcessorCount)
                {
                    throw new ArgumentOutOfRangeException("CPUId", id.ToString());
                }

                mask |= 1UL << id;
            }

            return mask;
        }

        /// <summary>
        ///  Ids from mask.
        /// </summary>
        /// <param name="mask">The mask.</param>
        /// <returns></returns>
        private static IEnumerable<int> IdsFromMask(ulong mask)
        {
            var ids = new List<int>();
            var i = 0;
            while (mask > 0UL)
            {
                if ((mask & 1UL) != 0)
                {
                    ids.Add(i);
                }

                mask >>= 1;
                i++;
            }

            return ids;
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class LinuxNative
    {
        private const string pthread = "libpthread.so.0";

        /// <summary>
        /// 
        /// </summary>
        /// <param name="threadHandel"></param>
        /// <param name="cpusize"></param>
        /// <param name="cpuset"></param>
        /// <returns></returns>
        [DllImport(pthread, CharSet = CharSet.Auto, SetLastError = true)]
        public static extern int pthread_setaffinity_np(IntPtr threadHandel, int cpusize,ref UInt64 cpuset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="threadHandel"></param>
        /// <param name="cpuset"></param>
        /// <returns></returns>
        [DllImport(pthread, CharSet = CharSet.Auto, SetLastError = true)]
        public static extern int pthread_getaffinity_np(IntPtr threadHandel,ref UInt64 cpuset);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        [DllImport(pthread, CharSet = CharSet.Auto, SetLastError = true)]
        public static extern IntPtr pthread_self();
    }


    /// <summary>
    ///     Win32Native Class
    /// </summary>
    public static class Win32Native
    {
        /// <summary>
        ///     The kernel32
        /// </summary>
        private const string Kernel32 = "kernel32.dll";

        /// <summary>
        ///     The NTDLL
        /// </summary>
        private const string Ntdll = "ntdll.dll";

        /// <summary>
        ///     The psapi
        /// </summary>
        private const string Psapi = "psapi.dll";

        /// <summary>
        ///     Enums the processes.
        /// </summary>
        /// <param name="processIds">The process ids.</param>
        /// <param name="size">The size.</param>
        /// <param name="needed">The needed.</param>
        /// <returns></returns>
        [DllImport(Psapi, CharSet = CharSet.Auto, SetLastError = true)]
        [ResourceExposure(ResourceScope.Machine)]
        internal static extern bool EnumProcesses(int[] processIds, int size, out int needed);

        /// <summary>
        ///     Opens the process.
        /// </summary>
        /// <param name="access">The access.</param>
        /// <param name="inherit">if set to <c>true</c> [inherit].</param>
        /// <param name="processId">The process identifier.</param>
        /// <returns></returns>
        [DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true)]
        [ResourceExposure(ResourceScope.Machine)]
        internal static extern IntPtr OpenProcess(int access, bool inherit, int processId);

        /// <summary>
        ///     Sets the thread affinity mask.
        /// </summary>
        /// <param name="handle">The handle.</param>
        /// <param name="mask">The mask.</param>
        /// <returns></returns>
        [DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true)]
        [ResourceExposure(ResourceScope.Process)]
        internal static extern IntPtr SetThreadAffinityMask(IntPtr handle, HandleRef mask);

        /// <summary>
        ///     Get current processor number.
        /// </summary>
        /// <returns></returns>
        [DllImport(Ntdll, CharSet = CharSet.Auto)]
        internal static extern uint NtGetCurrentProcessorNumber();

        /// <summary>
        ///     Gets the current thread. GetCurrentThread() returns only a pseudo handle. No need for a SafeHandle here.
        /// </summary>
        /// <returns></returns>
        [DllImport(Kernel32)]
        internal static extern IntPtr GetCurrentThread();

        /// <summary>
        ///     Sets the thread affinity mask.
        /// </summary>
        /// <param name="handle">The handle.</param>
        /// <param name="mask">The mask.</param>
        /// <returns></returns>
        [DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true)]
        internal static extern UIntPtr SetThreadAffinityMask(IntPtr handle, UIntPtr mask);

        /// <summary>
        ///     Sets the process affinity mask.
        /// </summary>
        /// <param name="handle">The handle.</param>
        /// <param name="mask">The mask.</param>
        /// <returns></returns>
        [DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true)]
        [ResourceExposure(ResourceScope.Machine)]
        internal static extern bool SetProcessAffinityMask(IntPtr handle, IntPtr mask);

        /// <summary>
        ///     Gets the process affinity mask.
        /// </summary>
        /// <param name="handle">The handle.</param>
        /// <param name="processMask">The process mask.</param>
        /// <param name="systemMask">The system mask.</param>
        /// <returns></returns>
        [DllImport(Kernel32, CharSet = CharSet.Auto, SetLastError = true)]
        [ResourceExposure(ResourceScope.None)]
        internal static extern bool GetProcessAffinityMask(
            IntPtr handle,
            out IntPtr processMask,
            out IntPtr systemMask);
    }

    
}
