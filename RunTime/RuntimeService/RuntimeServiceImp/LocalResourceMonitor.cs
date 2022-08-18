using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Management;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace RuntimeServiceImp
{
    /// <summary>
    /// 
    /// </summary>
    public class LocalResourceMonitor
    {
        /// <summary>
        /// 总共使用的内容
        /// </summary>
        public double FreeMemory { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double UsedPercent { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double TotalMemory { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //private Dictionary<string, DiskInfoItem> _DiskInfo = new Dictionary<string, DiskInfoItem>();

        /// <summary>
        /// 
        /// </summary>
        public static string OSVersion { get; set; }

        public static string DotnetVersion { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static int ProcessCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static bool Is64Bit { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static string MachineName { get; set; }

        /// <summary>
        /// 网络发送速率
        /// </summary>
        public double NetworkSendBytes { get; set; }

        /// <summary>
        /// 网络接收速率
        /// </summary>
        public double NetworkReceiveBytes { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double CPU { get; set; }

        private NetworkInfo networkinfo;


        static LocalResourceMonitor()
        {
            OSVersion = Environment.OSVersion.ToString();
            ProcessCount =Environment.ProcessorCount;
            Is64Bit = Environment.Is64BitOperatingSystem;
            DotnetVersion = Environment.Version.ToString();
            MachineName = Environment.MachineName.ToString();
        }


        /// <summary>
        /// 
        /// </summary>
        public void Update()
        {
            UpdateMemoryAndCPU();
            UpdateNetwork();
        }

        /// <summary>
        /// 
        /// </summary>
        private void UpdateNetwork()
        {
            try
            {
                if (networkinfo == null)
                    networkinfo = NetworkInfo.GetNetworkInfo();
                var speed = networkinfo.GetInternetSpeed(1000);
                NetworkSendBytes = speed.Send;
                NetworkReceiveBytes = speed.Received;
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            //Console.WriteLine("Send:{0} Kb:Receive{1} Kb",NetworkSendBytes,NetworkReceiveBytes);
        }

        private void UpdateWindowCPU()
        {
            try
            {
                ManagementObjectSearcher searcher = new ManagementObjectSearcher("root\\CIMV2", "SELECT * FROM Win32_PerfFormattedData_PerfOS_Processor");
                //Console.WriteLine("-----------------------------------");
                foreach (ManagementObject queryObj in searcher.Get())
                {
                    string sname = queryObj["Name"].ToString();
                    if (sname == "_Total")
                    {
                        CPU = Convert.ToDouble(queryObj["PercentProcessorTime"]);
                    }
                }
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }

        private static async Task<double> GetCpuUsageForProcess()

        {

            


            var startTime = DateTime.UtcNow;

            var startCpuUsage = Process.GetCurrentProcess().TotalProcessorTime;

            await Task.Delay(500);

            var endTime = DateTime.UtcNow;

            var endCpuUsage = Process.GetCurrentProcess().TotalProcessorTime;

            var cpuUsedMs = (endCpuUsage - startCpuUsage).TotalMilliseconds;

            var totalMsPassed = (endTime - startTime).TotalMilliseconds;

            var cpuUsageTotal = cpuUsedMs / (Environment.ProcessorCount * totalMsPassed);

            return cpuUsageTotal * 100;

        }

        /// <summary>
        /// 更新内存使用情况
        /// </summary>
        private void UpdateMemoryAndCPU()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                UpdateWindowMemoryUsedPercent();
                UpdateWindowCPU();
            }
            else
            {
                var dinfo = new DynamicInfo();
                var mm = dinfo.Memory;
                FreeMemory = mm.Free;
                this.TotalMemory = mm.Total;
                this.CPU = 100- dinfo.GetCpuState().Idolt;
            }
        }

        //private void UpdateDriverInfo()
        //{
        //    lock (_DiskInfo)
        //    {
        //        foreach (var vv in _DiskInfo)
        //        {
        //            GetDriverInfo(vv.Key, out string label, out double total, out double free);
        //            vv.Value.Free = free;
        //            vv.Value.Total = total;
        //            vv.Value.Lable = label;
        //        }
        //    }
        //}


        ///// <summary>
        ///// 获取剩余磁盘的占用率
        ///// </summary>
        ///// <param name="path"></param>
        //private void GetDriverInfo(string path,out string label,out double total,out double free)
        //{
        //    System.IO.DriveInfo dinfo = new System.IO.DriveInfo(System.IO.Path.GetPathRoot(path));
        //    label = dinfo.VolumeLabel;
        //    total = dinfo.TotalSize;
        //    free = dinfo.AvailableFreeSpace;
        //}

        public long GetProcessMemory(string name)
        {
            long re = 0;
            var vvs = Process.GetProcessesByName(name);
            if (vvs != null && vvs.Length > 0)
            {
                foreach (var vv in vvs)
                {
                    re += vv.WorkingSet64;
                }
            }
            return re;
        }

        /// <summary>
        /// 计算已使用内存
        /// </summary>
        /// <returns></returns>
        private long GetTotalMemoryUsed()
        {
            long re = 0;
            foreach(var vv in Process.GetProcesses())
            {
                re += vv.WorkingSet64;
            }
            return re;
        }

        #region 获得内存信息API
        [DllImport("kernel32.dll")]
        [return: MarshalAs(UnmanagedType.Bool)]
        public static extern bool GlobalMemoryStatusEx(ref MEMORY_INFO mi);

        //定义内存的信息结构
        [StructLayout(LayoutKind.Sequential)]
        public struct MEMORY_INFO
        {
            public uint dwLength; //当前结构体大小
            public uint dwMemoryLoad; //当前内存使用率
            public ulong ullTotalPhys; //总计物理内存大小
            public ulong ullAvailPhys; //可用物理内存大小
            public ulong ullTotalPageFile; //总计交换文件大小
            public ulong ullAvailPageFile; //总计交换文件大小
            public ulong ullTotalVirtual; //总计虚拟内存大小
            public ulong ullAvailVirtual; //可用虚拟内存大小
            public ulong ullAvailExtendedVirtual; //保留 这个值始终为0
        }
        #endregion

        /// <summary>
        /// 获得当前内存使用情况
        /// </summary>
        /// <returns></returns>
        public static MEMORY_INFO GetMemoryStatus()
        {
            MEMORY_INFO mi = new MEMORY_INFO();
            mi.dwLength = (uint)System.Runtime.InteropServices.Marshal.SizeOf(mi);
            GlobalMemoryStatusEx(ref mi);
            return mi;
        }

        private void UpdateWindowMemoryUsedPercent()
        {
            MEMORY_INFO mi = GetMemoryStatus();
            TotalMemory = mi.ullTotalPhys/1024.0/1024/1024;
            this.FreeMemory = mi.ullAvailPhys/1024.0/1024/1024;
            UsedPercent = mi.dwMemoryLoad;

            //Console.WriteLine("TotalMemory:{0} Gb Free:{1} Gb {2} {3}", TotalMemory, FreeMemory,(TotalMemory -FreeMemory)%TotalMemory, UsedPercent);
        }
    }

    //public class DiskInfoItem
    //{
    //    public string  Lable { get; set; }
    //    public double Total { get; set; }
    //    public double Free { get; set; }
    //}


}




