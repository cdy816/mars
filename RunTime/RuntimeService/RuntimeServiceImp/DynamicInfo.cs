﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;

namespace RuntimeServiceImp
{
    /// <summary>
    /// 获取 Linux 系统动态资源消耗信息
    /// </summary>
    public class DynamicInfo
    {

        private Tasks _tasks;
        private CpuState _cpuState;
        private Mem _mem;
        private Swap _swap;
        private Dictionary<int, PidInfo> _pidInfo;

        /// <summary>
        /// 
        /// </summary>
        public Mem Memory
        {
            get { return _mem; }
        }


        /// <summary>
        /// 
        /// </summary>
        public Swap SwapMemory
        {
            get { return _swap; }
        }


        /// <summary>
        /// 获取 Linux 系统动态资源消耗信息
        /// </summary>
        /// <exception cref="PlatformNotSupportedException">当算法不支持时</exception>
        public DynamicInfo()
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
            {
                var psi = new ProcessStartInfo("top", "-b -n 1") { RedirectStandardOutput = true };
                var proc = Process.Start(psi);
                if (proc != null)
                {
                    using (var sr = proc.StandardOutput)
                    {
                        Dictionary<int, string> dic = new Dictionary<int, string>();
                        try
                        {
                            int i = -1;
                            int n = 0;
                            while (!sr.EndOfStream)
                            {
                                ++i;
                                string str = sr.ReadLine();
                                if (i == 0 || i == 5 || i == 6)
                                    continue;
                                dic.Add(n, str);
                                ++n;
                            }

                            SetTasks(dic[0]);
                            SetCpuState(dic[1]);
                            SetMem(dic[2]);
                            SetSwap(dic[3]);
                            SetPidInfo(dic.Where(x => x.Key > 3).Select(x => x.Value).ToArray());
                        }
                        catch { }
                        if (!proc.HasExited)
                        {
                            proc.Kill();
                        }
                    }
                }
                else
                {
                    throw new PlatformNotSupportedException($"The current operating system is not supported.");
                }
            }
            else
            {
                throw new PlatformNotSupportedException($"The current operating system is not supported.");
            }
        }

        #region 获取信息算法

        /// <summary>
        /// 设置系统进程信息
        /// </summary>
        /// <param name="line"></param>
        private void SetTasks(string line)
        {
            // Tasks: 246 total,   1 running, 174 sleeping,   0 stopped,   0 zombie
            line = line.Replace(" ", string.Empty);
            line = line.Substring(line.IndexOf(':') + 1);
            string[] any = line.Split(',');
            if (any.Length != 5)
                return;
            try
            {
                _tasks = new Tasks()
                {
                    Total = Convert.ToInt32(GetNum(ref any[0])),
                    Running = Convert.ToInt32(GetNum(ref any[1])),
                    Sleeping = Convert.ToInt32(GetNum(ref any[2])),
                    Stopped = Convert.ToInt32(GetNum(ref any[3])),
                    Zombie = Convert.ToInt32(GetNum(ref any[4]))
                };
                _tasks.IsSuccess = true;
            }
            catch
            {
                return;
            }

        }

        /// <summary>
        /// 设置CPU信息
        /// </summary>
        /// <param name="line"></param>

        private void SetCpuState(string line)
        {
            // %Cpu(s):  4.9 us,  0.9 sy,  0.0 ni, 93.8 id,  0.4 wa,  0.0 hi,  0.0 si,  0.0 st

            line = line.Replace(" ", string.Empty);
            line = line.Substring(line.IndexOf(':') + 1);

            string[] any = line.Split(',');
            if (any.Length != 8)
                return;
            try
            {
                _cpuState = new CpuState()
                {
                    UserSpace = Convert.ToDouble(GetNum(ref any[0])),
                    Sysctl = Convert.ToDouble(GetNum(ref any[1])),
                    NI = Convert.ToDouble(GetNum(ref any[2])),
                    Idolt = Convert.ToDouble(GetNum(ref any[3])),
                    WaitIO = Convert.ToDouble(GetNum(ref any[4])),
                    HardwareIRQ = Convert.ToDouble(GetNum(ref any[5])),
                    SoftwareInterrupts = Convert.ToDouble(GetNum(ref any[6]))
                };
                _cpuState.IsSuccess = true;
            }
            catch
            {
                return;
            }
        }

        /// <summary>
        /// 设置内存信息
        /// </summary>
        /// <param name="line"></param>
        private void SetMem(string line)
        {
            // KiB Mem :  8105472 total,  1098760 free,  4061184 used,  2945528 buff/cache
            line = line.Replace(" ", string.Empty);
            line = line.Substring(line.IndexOf(':') + 1);
            string[] any = line.Split(',');
            if (any.Length != 4)
                return;
            try
            {
                _mem = new Mem()
                {
                    Total = Convert.ToInt32(GetNum(ref any[0])),
                    Free = Convert.ToInt32(GetNum(ref any[1])),
                    Used = Convert.ToInt32(GetNum(ref any[2])),
                    Buffers = Convert.ToInt32(GetNum(ref any[3]))
                };
                _mem.IsSuccess = true;
            }
            catch
            {
                return;
            }

        }

        /// <summary>
        /// 设置虚拟内存信息
        /// </summary>
        /// <param name="line"></param>

        private void SetSwap(string line)
        {
            // KiB Swap:  4194300 total,  4194300 free,        0 used.  3678612 avail Mem
            line = line.Replace(" ", string.Empty);
            line = line.Substring(line.IndexOf(':') + 1);
            string[] any = line.Split(',');
            if (any.Length != 3)
                return;
            try
            {
                string[] used = any[2].Split('.');
                _swap = new Swap()
                {
                    Total = Convert.ToInt32(GetNum(ref any[0])),
                    Free = Convert.ToInt32(GetNum(ref any[1])),
                    Used = Convert.ToInt32(GetNum(ref used[0])),
                    AvailMem = Convert.ToInt32(GetNum(ref used[1]))
                };
                _mem.IsSuccess = true;
            }
            catch
            {
                return;
            }
        }

        private void SetPidInfo(string[] lines)
        {
            // PID USER      PR  NI    VIRT    RES    SHR S  %CPU %MEM     TIME+ COMMAND                       
            // 7214 whuanle   20   0 5159720 1.508g 134084 S  27.9 19.5  16:21.66 mono-sgen   
            try
            {
                List<PidInfo> pids = new List<PidInfo>();
                for (int i = 0; i < lines.Length; i++)
                {
                    pids.Add(SetPidInfoLine(lines[i]));
                }

                _pidInfo = pids.ToDictionary(x => x.PID, x => x);
            }
            catch
            {
                _pidInfo = default;
            }

        }
        /// <summary>
        /// 获取一个进程的信息
        /// </summary>
        /// <param name="line"></param>
        /// <returns></returns>
        /// <exception cref="Exception"></exception>
        private PidInfo SetPidInfoLine(string line)
        {
            try
            {
                string[] result = GetStringArray(line.Trim());
                return new PidInfo()
                {
                    PID = Convert.ToInt32(result[0]),
                    User = result[1],
                    PR = result[2],
                    Nice = Convert.ToInt32(result[3]),
                    VIRT = result[4],
                    RES = result[5],
                    SHR = result[6],
                    State = Convert.ToChar(result[7]),
                    CPU = Convert.ToDouble(result[8]),
                    Mem = Convert.ToDouble(result[9]),
                    Command = result[11],
                    IsSuccess = true
                };
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        /// <summary>
        /// 去除字符串中的空格并生成字符串数组
        /// </summary>
        /// <param name="line"></param>
        /// <returns></returns>
        private string[] GetStringArray(string line)
        {
            // 7214 whuanle   20   0 5159720 1.508g 134084 S  27.9 19.5  16:21.66 mono-sgen 
            return line.Split(' ').Where(x => x != string.Empty).ToArray();
        }

        /// <summary>
        /// 从字符串中提取数字
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        private string GetNum(ref string str)
        {
            return str.Substring(0, GetCharIndex(ref str));
        }

        /// <summary>
        /// 获取数字索引后一位位置
        /// </summary>
        /// <param name="str"></param>
        /// <returns></returns>
        private int GetCharIndex(ref string str)
        {
            for (int i = 0; i < str.Length; i++)
            {
                if (char.IsLetter(str[i]))
                    return i;
            }
            return -1;
        }

        #endregion

        /// <summary>
        /// 获取进程列表
        /// </summary>
        /// <returns></returns>
        public Tasks GetTasks()
        {
            return _tasks;
        }


        /// <summary>
        /// 获取CPU负载状态
        /// </summary>
        /// <returns></returns>
        public CpuState GetCpuState()
        {
            return _cpuState;
        }


        /// <summary>
        /// 获取系统内存使用信息
        /// </summary>
        /// <returns></returns>
        public Mem GetMem()
        {
            return _mem;
        }


        /// <summary>
        /// 获取虚拟内存使用信息
        /// </summary>
        /// <returns></returns>
        public Swap GetSwap()
        {
            return _swap;
        }

        /// <summary>
        /// 获取所有进程的使用资源信息
        /// </summary>
        /// <returns></returns>
        public Dictionary<int, PidInfo> GetPidInfo()
        {
            return _pidInfo;
        }


        /// <summary>
        /// 获取对应进程的key-value 值
        /// </summary>
        /// <returns></returns>
        public KeyValuePair<string, object>[] GetkeyvaluesOfTasks()
        {
            var propertys = _tasks.GetType().GetProperties();
            return GetKeyVaues(propertys, _tasks);
        }

        private KeyValuePair<string, object>[] GetKeyVaues(PropertyInfo[] propertys, object obj)
        {
            var keyvalus = new KeyValuePair<string, object>[propertys.Length];
            for (int i = 0; i < keyvalus.Length; i++)
            {
                var keyValue = new KeyValuePair<string, object>(propertys[i].Name, propertys[i].GetValue(obj));
                keyvalus[i] = keyValue;
            }

            return keyvalus;
        }

        /// <summary>
        /// 获取内存资源数据
        /// </summary>
        /// <returns></returns>
        public KeyValuePair<string, object>[] GetKeyValuesOfMem()
        {
            var propertys = _mem.GetType().GetProperties();
            return GetKeyVaues(propertys, _mem);
        }

        /// <summary>
        /// 获取CPU资源数据
        /// </summary>
        /// <returns></returns>
        public KeyValuePair<string, object>[] GetKeyValuesOfCpu()
        {
            var propertys = _cpuState.GetType().GetProperties();
            return GetKeyVaues(propertys, _cpuState);
        }

        /// <summary>
        /// 获取虚拟内存资源数据
        /// </summary>
        /// <returns></returns>
        public KeyValuePair<string, object>[] GetKeyValuesOfSwap()
        {
            var propertys = _swap.GetType().GetProperties();
            return GetKeyVaues(propertys, _swap);
        }
    }

    /// <summary>
    /// 内存使用情况，单位 kb
    /// </summary>
    public class Mem
    {
        /// <summary>
        /// 判断是否能够获取到当前类型的信息
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// 总内存大小
        /// </summary>
        public long Total { get; set; }

        /// <summary>
        /// 已使用内存
        /// </summary>
        public long Used { get; set; }

        /// <summary>
        /// 剩余内存
        /// </summary>
        public long Free { get; set; }

        /// <summary>
        /// 缓存内存
        /// </summary>

        public long Buffers { get; set; }

        /// <summary>
        /// 实际剩余可用内存
        /// </summary>
        public long CanUsed
        {
            get { return Free + Buffers; }
        }
    }

    /// <summary>
    /// PID
    /// </summary>
    public class PidInfo
    {

        /// <summary>
        /// 判断是否能够获取到当前类型的信息
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// 进程id
        /// </summary>
        public int PID { get; set; }

        /// <summary>
        /// 进程名称
        /// </summary>
        public string Command { get; set; }

        /// <summary>
        /// 所属用户
        /// </summary>
        public string User { get; set; }

        /// <summary>
        /// 进程优先级
        /// </summary>
        public string PR { get; set; }

        /// <summary>
        /// 高低优先级
        /// </summary>
        public int Nice { get; set; }

        /// <summary>
        /// 进程占用虚拟内存
        /// </summary>
        public string VIRT { get; set; }

        /// <summary>
        /// 进程占用的物理内存
        /// </summary>
        public string RES { get; set; }

        /// <summary>
        /// 共享内存大小
        /// </summary>
        public string SHR { get; set; }

        /// <summary>
        /// 进程状态
        /// D 不可中断的睡眠状态
        /// R 运行
        /// S 睡眠
        /// T 跟踪/停止
        /// Z 僵尸进程
        /// </summary>
        public char State { get; set; }

        /// <summary>
        /// 进程最近占用CPU负载百分比
        /// </summary>
        public double CPU { get; set; }

        /// <summary>
        /// 进程使用物理内存的百分比
        /// </summary>
        public double Mem { get; set; }
    }


    /// <summary>
    /// 总进程数
    /// </summary>
    public class Tasks
    {
        /// <summary>
        /// 判断是否能够获取到当前类型的信息
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// 总进程数
        /// </summary>
        public int Total { get; set; }

        /// <summary>
        /// 正在运行
        /// </summary>
        public int Running { get; set; }

        /// <summary>
        /// 休眠
        /// </summary>
        public int Sleeping { get; set; }

        /// <summary>
        /// 已停止
        /// </summary>
        public int Stopped { get; set; }

        /// <summary>
        /// 僵尸进程
        /// </summary>
        public int Zombie { get; set; }
    }

    /// <summary>
    /// CPU 状态
    /// </summary>
    public class CpuState
    {
        /// <summary>
        /// 判断是否能够获取到当前类型的信息
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// 用户占用CPU负载百分比
        /// </summary>
        public double UserSpace { get; set; }

        /// <summary>
        /// 系统内核占用CPU负载百分比
        /// </summary>

        public double Sysctl { get; set; }

        /// <summary>
        /// 特殊优先级进程占用CPU负载百分比
        /// </summary>

        public double NI { get; set; }

        /// <summary>
        /// 剩余可用CPU负载百分比
        /// </summary>
        public double Idolt { get; set; }

        /// <summary>
        /// IO等待占用CPU负载百分比
        /// </summary>
        public double WaitIO { get; set; }

        /// <summary>
        /// 硬中断占用CPU负载百分比
        /// </summary>
        public double HardwareIRQ { get; set; }

        /// <summary>
        /// 软中断占用CPU负载百分比
        /// </summary>
        public double SoftwareInterrupts { get; set; }
    }

    /// <summary>
    /// Swap交换分区信息，单位 kb
    /// </summary>
    public class Swap
    {
        /// <summary>
        /// 判断是否能够获取到当前类型的信息
        /// </summary>
        public bool IsSuccess { get; set; }

        /// <summary>
        /// 总内存大小
        /// </summary>
        public long Total { get; set; }

        /// <summary>
        /// 已使用内存
        /// </summary>
        public long Used { get; set; }

        /// <summary>
        /// 剩余可使用内存
        /// </summary>
        public long Free { get; set; }

        /// <summary>
        /// 进程下一次可分配的物理内存
        /// </summary>
        public long AvailMem { get; set; }
    }
}
