using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO.MemoryMappedFiles;
using System.Net;
using System.Net.Http.Headers;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Timers;

namespace Cdy.Tag.Common
{
    /// <summary>
    /// 
    /// </summary>
    public class ProcessMemoryInfo:IDisposable
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        System.IO.MemoryMappedFiles.MemoryMappedViewAccessor viewAccessor;

        System.IO.MemoryMappedFiles.MemoryMappedFile mfile;


        private string mName;

        private Dictionary<string, ClientInfo> mClients= new Dictionary<string, ClientInfo>();

        /// <summary>
        /// 
        /// </summary>
        public static ProcessMemoryInfo Instances = new ProcessMemoryInfo();

        private object mLock = new object();

        private Timer mScanTimer;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public ProcessMemoryInfo()
        {
           
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Name
        {
            get
            {
                return mName;
            }
            set
            {
                if (mName != value)
                {
                    mName = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public double TotalMemory { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double TotalCPU { get; set; }

        /// <summary>
        /// 线程数
        /// </summary>
        public int ThreadCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public double CPU { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, ClientInfo> Clients
        {
            get { return mClients;}
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sname"></param>
        /// <returns></returns>
        public bool TryOpen()
        {
            try
            {
                if (IsExist(this.Name))
                {
                    if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    {
                        mfile = MemoryMappedFile.OpenExisting(this.Name);
                    }
                    else
                    {
                        string ssfile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, mName);
                        var stream = System.IO.File.Open(ssfile, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite, System.IO.FileShare.ReadWrite);
                        mfile = MemoryMappedFile.CreateFromFile(stream, null, 1024 * 32, MemoryMappedFileAccess.ReadWrite, System.IO.HandleInheritability.Inheritable, false);
                    }
                    if (mfile != null)
                    {
                        viewAccessor = mfile.CreateViewAccessor();
                        return true;
                    }
                }
            }
            catch
            { }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public static bool IsExist(string name)
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    var mfile = MemoryMappedFile.OpenExisting(name);
                    mfile.Dispose();
                    return true;
                }
                else
                {
                   
                    string ssfile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, name);
                    if (System.IO.File.Exists(ssfile))
                    {
                        try
                        {
                            var vss = System.IO.File.Open(ssfile, System.IO.FileMode.Open, System.IO.FileAccess.ReadWrite, System.IO.FileShare.None);
                            vss.Close();
                            return false;
                        }
                        catch
                        {
                            return true;
                        }
                    }
                }
            }
            catch
            { }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        public void RefreshMemory()
        {
            var pg = Process.GetCurrentProcess();
            CPU = GetCpuUsageForProcess(pg);
            TotalMemory = pg.WorkingSet64 / 1024.0;
            TotalCPU = pg.TotalProcessorTime.TotalMilliseconds;
            StartTime = pg.StartTime;
            ThreadCount = pg.Threads.Count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="pg"></param>
        /// <returns></returns>
        private double GetCpuUsageForProcess(Process pg)
        {
            var startTime = DateTime.UtcNow;
            var startCpuUsage = pg.TotalProcessorTime;
            Task.Delay(1000);
            var endTime = DateTime.UtcNow;
            var endCpuUsage = pg.TotalProcessorTime;
            var cpuUsedMs = (endCpuUsage - startCpuUsage).TotalMilliseconds;
            var totalMsPassed = (endTime - startTime).TotalMilliseconds;
            var cpuUsageTotal = cpuUsedMs / (Environment.ProcessorCount * totalMsPassed);
            return cpuUsageTotal * 100;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public void AddClient(string ip,int port)
        {
            lock (mLock)
            {
                string skey = ip + ":" + port;
                if (!mClients.ContainsKey(skey))
                {
                    mClients.Add(skey, new ClientInfo() { Ip = IPAddress.Parse(ip).Address, Port = port, Time = DateTime.Now });
                    Write();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public void RemoveClient(string ip,int port)
        {
            lock (mLock)
            {
                string skey = ip + ":" + port;
                if (mClients.ContainsKey(skey))
                {
                    mClients.Remove(skey);
                    Write();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void New()
        {
            try
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    mfile = MemoryMappedFile.CreateOrOpen(mName, 1024 * 32);
                }
                else
                {
                    string ssfile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, mName);
                    var stream = System.IO.File.Open(ssfile, System.IO.FileMode.OpenOrCreate, System.IO.FileAccess.ReadWrite, System.IO.FileShare.ReadWrite);
                    mfile = MemoryMappedFile.CreateFromFile(stream, null, 1024 * 32, MemoryMappedFileAccess.ReadWrite, System.IO.HandleInheritability.Inheritable, false);
                }
                viewAccessor = mfile.CreateViewAccessor();
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("ProcessMemoryInfo", $"New {ex.Message} {ex.StackTrace}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Write()
        {
            try
            {
                long ltmp = 0;

                viewAccessor?.Write(ltmp, this.CPU);
                ltmp += 8;
                viewAccessor?.Write(ltmp, this.TotalCPU);
                ltmp += 8;
                viewAccessor?.Write(ltmp, this.TotalMemory);
                ltmp += 8;
                viewAccessor?.Write(ltmp, this.StartTime.Ticks);
                ltmp += 8;

                viewAccessor?.Write(ltmp, this.ThreadCount);
                ltmp += 4;

                viewAccessor?.Write(ltmp, mClients.Count);
                ltmp += 4;
                foreach (var vv in mClients)
                {
                    ClientInfo cinfo = vv.Value;
                    viewAccessor?.Write<ClientInfo>(ltmp, ref cinfo);
                    ltmp += 20;
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("ProcessMemoryInfo", $"{ex.Message} {ex.StackTrace}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool Read()
        {
            try
            {
                long ltmp = 0;
                CPU = viewAccessor.ReadDouble(ltmp);
                ltmp += 8;
                TotalCPU = viewAccessor.ReadDouble(ltmp);
                ltmp += 8;
                this.TotalMemory = viewAccessor.ReadDouble(ltmp);
                ltmp += 8;
                this.StartTime = DateTime.FromBinary(viewAccessor.ReadInt64(ltmp));
                ltmp += 8;
                ThreadCount = viewAccessor.ReadInt32(ltmp);
                ltmp += 4;
                int count = viewAccessor.ReadInt32(ltmp);
                ltmp += 4;
                mClients.Clear();
                for (int i = 0; i < count; i++)
                {
                    ClientInfo cinfo;
                    viewAccessor.Read<ClientInfo>(ltmp, out cinfo);
                    ltmp += 20;
                    string sip = (new IPAddress(cinfo.Ip)).ToString() + ":" + cinfo.Port;
                    mClients.Add(sip, cinfo);
                }
                return true;
            }
            catch(Exception ex)
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            try
            {
                
                viewAccessor?.Dispose();
                if (mfile != null)
                {
                    mfile.Dispose();
                }

                if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                {
                    string ssfile = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, mName);
                    if(System.IO.File.Exists(ssfile))
                    {
                        try
                        {
                            System.IO.File.Delete(ssfile);
                        }
                        catch
                        {

                        }
                    }
                }
            }
            catch
            { 
            
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void StartMonitor(string name,int time=1000)
        {
            this.Name= name;
            this.New();
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                StartMonitor(time);
            }
            else
            {
                StartMonitor(5000);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void StartMonitor(int time)
        {
            mScanTimer = new Timer(time);
            mScanTimer.Elapsed += MScanTimer_Elapsed;
            mScanTimer.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MScanTimer_Elapsed(object sender, ElapsedEventArgs e)
        {
            lock (mLock)
            {
                mScanTimer?.Stop();
                RefreshMemory();
                lock (mLock)
                    Write();
                mScanTimer?.Start();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void StopMonitor()
        {
            lock (mLock)
            {
                mScanTimer.Elapsed -= MScanTimer_Elapsed;
                mScanTimer.Stop();
                mScanTimer.Dispose();
                mScanTimer = null;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    public struct ClientInfo
    {
        /// <summary>
        /// 
        /// </summary>
        public long Ip { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Port {
            get; set;
        }

        public DateTime Time { get; set; }
    }
}
