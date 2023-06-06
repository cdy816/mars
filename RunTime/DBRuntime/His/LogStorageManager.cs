using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Timers;

namespace Cdy.Tag
{
    /// <summary>
    /// 日志本地存储管理
    /// 其他模块首先将数据填充到内存里，防止不断的写磁盘导致程序阻塞
    /// 定时将内存里的数据写入到磁盘上
    /// </summary>
    public class LogStorageManager
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public static LogStorageManager Instance = new LogStorageManager();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, Dictionary<DateTime, LogRegion>> mLogs = new Dictionary<int, Dictionary<DateTime, LogRegion>>();

        private Dictionary<int, Dictionary<DateTime, LogRegion>> mManualLogs = new Dictionary<int, Dictionary<DateTime, LogRegion>>();

        private Thread mSaveThread;

        private bool mIsClosed = false;

        public static int LogSleepTime = 1000;

        public static int CachFlashSize = 5;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        #region 消息序列化

        /// <summary>
        /// 启动
        /// </summary>
        public void Start()
        {
            mSaveThread = new Thread(FlushToDisk);
            mSaveThread.IsBackground = true;
            mSaveThread.Start();
        }

        /// <summary>
        /// 停止
        /// </summary>
        public void Stop()
        {
            mIsClosed = true;
            while (mSaveThread.IsAlive) Thread.Sleep(1);
        }

        /// <summary>
        /// 将消息队列中的数据存储到磁盘内
        /// </summary>
        private void FlushToDisk()
        {
            List<LogRegion> lr = new List<LogRegion>();
            while (!mIsClosed)
            {
                lock (mLogs)
                {
                    foreach (var vv in mLogs)
                    {
                        foreach (var vvv in vv.Value)
                        {
                            lr.Add(vvv.Value);
                        }
                    }
                }

                foreach(var vv in lr)
                {
                    try
                    {
                        vv.FlushMemoryToDisk();
                    }
                    catch
                    {

                    }
                }
                lr.Clear();

                lock (mManualLogs)
                {
                    foreach (var vv in mManualLogs)
                    {
                        foreach (var vvv in vv.Value)
                        {
                            lr.Add(vvv.Value);
                        }
                    }
                }

                foreach (var vv in lr)
                {
                    try
                    {
                        vv.FlushMemoryToDisk();
                    }
                    catch
                    {

                    }
                }
                lr.Clear();

                ReleaseManualLogs();
                Thread.Sleep(LogSleepTime);
            }
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public LogRegion GetExistSystemLog(DateTime time, int id)
        {
            if (!LogRegion.Enable) return null;

            DateTime tm = time.Date.AddHours(time.Hour).AddMinutes((time.Minute / 5) * 5);

            lock (mLogs)
            {
                if (mLogs.ContainsKey(id))
                {
                    if (mLogs[id].ContainsKey(tm))
                    {
                        return mLogs[id][tm];
                    }
                }
            }
            return null;
        }

        //private object GetSystemLogLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public LogRegion GetSystemLog(DateTime time, int id)
        {
            if (!LogRegion.Enable) return null;

            DateTime tm = time.Date.AddHours(time.Hour).AddMinutes((time.Minute / 5) * 5);

            lock (mLogs)
            {
                if (mLogs.ContainsKey(id) && mLogs[id].ContainsKey(tm))
                {
                    return mLogs[id][tm];
                }
                else if (mLogs.ContainsKey(id))
                {
                    var lr = new LogRegion(Convert.ToInt64(ServiceLocator.Locator.Resolve("CachMemorySize"))) { Time = tm, AreaName = "system", Id = (byte)id };
                    lr.Init();
                    mLogs[id].Add(tm, lr);
                    return lr;
                }
                else
                {
                    Dictionary<DateTime, LogRegion> dd = new Dictionary<DateTime, LogRegion>();
                    var lr = new LogRegion(Convert.ToInt64(ServiceLocator.Locator.Resolve("CachMemorySize"))) { Time = tm, AreaName = "system", Id = (byte)id };
                    lr.Init();
                    dd.Add(tm, lr);
                    if (!mLogs.ContainsKey(id))
                        mLogs.Add(id, dd);
                    return lr;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public IEnumerable<LogRegion> GetSystemLog(DateTime time)
        {
            if (!LogRegion.Enable) yield return null;
            DateTime tm = time.Date.AddHours(time.Hour).AddMinutes((time.Minute / 5) * 5);
            lock (mLogs)
            {
                foreach (var log in mLogs)
                {
                    if (log.Value.ContainsKey(tm))
                    {
                        yield return log.Value[tm];
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public IEnumerable<LogRegion> GetManualLog(DateTime time)
        {
            if (!LogRegion.Enable) yield return null;

            DateTime tm = time.Date.AddHours(time.Hour).AddMinutes((time.Minute / 5) * 5);
            lock (mManualLogs)
            {
                foreach (var log in mManualLogs)
                {
                    if (log.Value.ContainsKey(tm))
                    {
                        yield return log.Value[tm];
                    }
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public LogRegion GetManualLog(DateTime time,int id)
        {
            if (!LogRegion.Enable) return null;

            DateTime tm = time.Date.AddHours(time.Hour).AddMinutes((time.Minute / 5) * 5);

            lock (mManualLogs)
            {
                if (mManualLogs.ContainsKey(id) && mManualLogs[id].ContainsKey(tm))
                {
                    return mManualLogs[id][tm];
                }
                else if (mManualLogs.ContainsKey(id))
                {
                    var lr = new LogRegion(Convert.ToInt64(ServiceLocator.Locator.Resolve("ManualTagCachMemorySize"))) { Time = tm, AreaName = "manual", Id = (byte)id };
                    lr.Init();
                    mManualLogs[id].Add(tm, lr);
                    return lr;
                }
                else
                {
                    Dictionary<DateTime, LogRegion> dd = new Dictionary<DateTime, LogRegion>();
                    var lr = new LogRegion(Convert.ToInt64(ServiceLocator.Locator.Resolve("ManualTagCachMemorySize"))) { Time = tm, AreaName = "manual", Id = (byte)id };
                    lr.Init();
                    dd.Add(tm, lr);
                    if (!mManualLogs.ContainsKey(id))
                        mManualLogs.Add(id, dd);
                    return lr;
                }
            }
        }

        public LogRegion GetExistManualLog(DateTime time, int id)
        {
            if (!LogRegion.Enable) return null;

            DateTime tm = time.Date.AddHours(time.Hour).AddMinutes((time.Minute / 5) * 5);

            lock (mManualLogs)
            {
                if (mManualLogs.ContainsKey(id)&& mManualLogs[id].ContainsKey(tm))
                {
                    return mManualLogs[id][tm];
                }
            }
            return null;
        }

        private object ReleaseSystemLogLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="area"></param>
        public void ReleaseSystemLog(DateTime time,int id)
        {
            if (LogRegion.Enable)
            {
                lock (ReleaseSystemLogLocker)
                {
                    var vv = GetExistSystemLog(time, id);
                    if (vv.RefCount <= 0)
                    {
                        try
                        {
                            LogRegion lr = null;
                            lock (mLogs)
                            {
                                if(mLogs.ContainsKey(id) && mLogs[id].ContainsKey(vv.Time))
                                lr = mLogs[id][vv.Time];
                            }

                            if (lr!=null)
                            {
                                lr.Dispose();
                                lr.Clear();
                                lock (mLogs)
                                {
                                    try
                                    {
                                        if (mLogs.ContainsKey(id))
                                            mLogs[id].Remove(vv.Time);
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
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void ReleaseSystemLog(DateTime time)
        {
            if (LogRegion.Enable)
            {
                List<LogRegion> lr = new List<LogRegion>();
                lock (mLogs)
                {
                    foreach(var vv in GetSystemLog(time))
                    {
                        if(vv!=null)
                        {
                            lr.Add(vv);
                        }
                    }

                }
                if (lr.Count > 0)
                {
                    foreach (var vv in lr)
                    {
                        vv.Dispose();
                        vv.Clear();
                        lock (mLogs)
                        {
                            try
                            {
                                if (mLogs.ContainsKey(vv.Id))
                                    mLogs[vv.Id].Remove(vv.Time);
                            }
                            catch
                            {

                            }
                           
                        }

                        
                    }
                }
            }
        }

        private object mManualLocker= new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="area"></param>
        public LogStorageManager IncManualRef(DateTime time,int id)
        {
            if (!LogRegion.Enable) return this;
            lock (mManualLocker)
                GetManualLog(time,id).RefCount++;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="area"></param>
        public LogStorageManager DecManualRef(DateTime time, int id)
        {
            if (!LogRegion.Enable) return this;
            lock (mManualLocker)
            {
                var re = GetExistManualLog(time, id);
                if (re != null) re.RefCount--;
            }
            return this;
        }

        private object ReleaseManualLogLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="area"></param>
        public LogStorageManager ReleaseManualLog(DateTime time,int id)
        {
            if (!LogRegion.Enable) return this;
            lock (ReleaseManualLogLocker)
            {
                var vv = GetExistManualLog(time,id);
                DateTime dnow = DateTime.Now;
                if (vv!=null && vv.RefCount <= 0 && (dnow - vv.CreatTime).TotalMinutes>5)
                {
                    LogRegion lr = null;
                    lock (mManualLogs)
                    {
                        if(mManualLogs.ContainsKey(id) && mManualLogs[id].ContainsKey(vv.Time))
                        {
                            lr = mManualLogs[id][vv.Time];
                        }
                    }

                    if (lr!=null)
                    {
                        lr.Dispose();
                        lr.Clear();
                        lock (mManualLogs)
                        {
                            try
                            {
                                if (mManualLogs.ContainsKey(id))
                                    mManualLogs[id].Remove(vv.Time);
                            }catch
                            {

                            }
                        }
                    }
                }
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public LogStorageManager ReleaseManualLogs()
        {
            if (!LogRegion.Enable) return this;
            List<LogRegion> list = new List<LogRegion>();
            lock (mManualLogs)
            {
                DateTime time = DateTime.Now;
                foreach (var region in mManualLogs.Values)
                {
                    foreach(var vv in region)
                    {
                        if(vv.Value.RefCount<= 0 && (time - vv.Value.CreatTime).TotalMinutes > 5)
                        {
                            list.Add(vv.Value);
                        }
                    }
                }
            }

            foreach (var vv in list)
            {
                lock (mManualLogs)
                {
                    try
                    {
                        mManualLogs[vv.Id].Remove(vv.Time);
                    }
                    catch
                    {

                    }
                }
                vv.Dispose();
                vv.Clear();
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            if (!LogRegion.Enable) return;

            foreach (var vv in mManualLogs)
            {
                foreach (var vvv in vv.Value)
                {
                    vvv.Value.Dispose();
                    vvv.Value.Clear();
                }
            }

            foreach (var vv in mLogs)
            {
                foreach (var vvv in vv.Value)
                {
                    vvv.Value.Dispose();
                    vvv.Value.Clear();

                }
            }
            lock (mLogs)
                mLogs.Clear();
            lock (mManualLogs)
                mManualLogs.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public SortedDictionary<DateTime,LogRegion> ListAllLogRegion()
        {
            string spath = System.IO.Path.Combine(ServiceLocator.Locator.Resolve("DatabaseLocation").ToString(), "Cache");
            SortedDictionary<DateTime, LogRegion> re = new SortedDictionary<DateTime, LogRegion>();
            if (System.IO.Directory.Exists(spath))
            {
                foreach (var vv in System.IO.Directory.EnumerateFiles(spath))
                {
                    string sname = System.IO.Path.GetFileNameWithoutExtension(vv);
                    string area = sname.Substring(0, 6);
                    string datetime = sname.Substring(6);

                    int year = int.Parse(datetime.Substring(0, 4));
                    int month = int.Parse(datetime.Substring(4, 2));
                    int day = int.Parse(datetime.Substring(6, 2));
                    int hh = int.Parse(datetime.Substring(8, 2));
                    int mm = int.Parse(datetime.Substring(10, 2));

                    int ss = int.Parse(datetime.Substring(12, datetime.Length-12));

                    LogRegion lr = new LogRegion() { AreaName = area, Time = new DateTime(year, month, day, hh, mm, ss%60),FileName=vv,Id=ss };

                    if (!re.ContainsKey(lr.Time))
                        re.Add(lr.Time, lr);
                }
            }
            return re;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }

    /// <summary>
    /// 
    /// </summary>
    public class LogRegion:IDisposable
    {

        #region ... Variables  ...
        
        System.IO.MemoryMappedFiles.MemoryMappedFile mmf;
        System.IO.MemoryMappedFiles.MemoryMappedViewAccessor accessor;
        MemoryMappedViewStream mViewStream;

        private long mCalSize = 1;

        public static string LogBasePath = "";

        private object mLock = new object();

        /// <summary>
        /// 
        /// </summary>
        public static bool Enable { get; set; } = true;

        private MarshalMemoryBlock mCachMemroy1;
        
        private MarshalMemoryBlock mCachMemroy2;

        private MarshalMemoryBlock mCurrentMemory;

        private bool mIsFlushBusy = false;

        private bool mIsWriteBusy = false;

        private bool mIsDisposed = false;

        private static object mLocker = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public LogRegion()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public LogRegion(long size)
        {
            mCalSize=size;
        }

        #endregion ...Constructor...

        #region ... Properties ...


        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 记录的当前位置
        /// </summary>
        public int Position { get; set; }

        /// <summary>
        /// 分区名称
        /// </summary>
        public string AreaName { get; set; }

        /// <summary>
        /// 引用统计
        /// </summary>
        public int RefCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public string  FileName { get; set; }

        public DateTime CreatTime { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 初始化
        /// </summary>
        public void Init()
        {
            CreatTime=DateTime.Now;
            int msize = (int)(((mCalSize / LogStorageManager.CachFlashSize / 1024) + 1) * 1024);
            if(msize==0)
            {
                msize = 1024;
            }
            mCachMemroy1 = new MarshalMemoryBlock(msize, msize);
            mCachMemroy2 = new MarshalMemoryBlock(msize, msize);
            mCurrentMemory = mCachMemroy1;

            Task.Run(() => {
                Init(mCalSize);
            });
        }


        private void SwitchMemroyBlock()
        {
            lock (mLock)
            {
                if (mCurrentMemory == mCachMemroy1)
                {
                    mCurrentMemory = mCachMemroy2;
                }
                else
                {
                    mCurrentMemory = mCachMemroy1;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void FlushMemoryToDisk()
        {
           
            mIsFlushBusy = true;
            if (mIsDisposed || !mIsInited)
            {
                mIsFlushBusy = false;
                return;
            }
            
            try
            {
                var mm = mCurrentMemory;
                if (mm != null && mm.Position > 0 && !mm.IsDisposed && mViewStream != null)
                {
                    SwitchMemroyBlock();
                    CheckAndResize(mm.Position + Position);
                    mm.WriteToStream(mViewStream);
                    Position += (int)mm.Position;
                    mm.Clear();
                }
            }
            catch
            {

            }
            mIsFlushBusy = false;
        }

        private bool mIsInited = false;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        private void Init(long size)
        {
            mIsInited = false;
            if (!Enable) return;

            if (accessor != null)
            {
                accessor.Dispose();
            }
            
            if (mViewStream != null)
            {
                mViewStream.Dispose();
            }
            
            if (mmf != null)
            {
                mmf.Dispose();
            }

            lock (mLocker)
            {
                try
                {
                    string spath = System.IO.Path.Combine(ServiceLocator.Locator.Resolve("DatabaseLocation").ToString(), "Cache");

                    if (!System.IO.Directory.Exists(spath))
                    {
                        System.IO.Directory.CreateDirectory(spath);
                    }

                    spath = System.IO.Path.Combine(spath, AreaName + Time.ToString("yyyyMMddHHmm") + Id + ".ch");

                    mmf = MemoryMappedFile.CreateFromFile(spath, FileMode.OpenOrCreate, null, size);
                    accessor = mmf.CreateViewAccessor(0, size);
                    mViewStream = mmf.CreateViewStream();
                    mCalSize = size;
                }
                catch
                {

                }
            }

            mIsInited = true;

        }

        /// <summary>
        /// 
        /// </summary>
        public void Open()
        {
            string spath="";

            if(string.IsNullOrEmpty(FileName))
            {
                spath = System.IO.Path.Combine(ServiceLocator.Locator.Resolve("DatabaseLocation").ToString(), "Cache");
                spath = System.IO.Path.Combine(spath, AreaName + Time.ToString("yyyyMMddHHmm") + Id + ".ch");
            }
            else
            {
                spath = FileName;
            }
            

            if (System.IO.File.Exists(spath))
            {

                var vss = System.IO.File.Open(spath, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite);
                mmf = MemoryMappedFile.CreateFromFile(vss,null,vss.Length,MemoryMappedFileAccess.Read,HandleInheritability.Inheritable,false);
                accessor = mmf.CreateViewAccessor(0,vss.Length,MemoryMappedFileAccess.Read);
                mCalSize=vss.Length;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Flush()
        {
            accessor?.Flush();
            mViewStream?.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void CheckAndResize(long size)
        {
            if (!Enable) return;
            if (size>mCalSize)
            {
                LoggerService.Service.Warn("LogStorageManager", $"New Resize from {size} to {mCalSize}");
                Init((long)(size*1.2));
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="time"></param>
        ///// <param name="quality"></param>
        ///// <param name="value"></param>
        //public void Append<T>(int id,DateTime time,byte quality, params T[] value)
        //{
        //    if (!Enable) return;
        //    lock (mLock)
        //    {

        //        var tsize = 39;
                
        //        if(typeof(T) == typeof(string))
        //        {
        //            tsize = 15 + 256;
        //        }

        //        CheckAndResize(Position+ tsize);

        //        byte type = GetDataType<T>();
        //        var sname = typeof(T).Name.ToLower();
        //        accessor.Write(Position, id);
        //        Position += 4;
        //        accessor.Write(Position,time.Ticks);
        //        Position += 8;
        //        accessor.Write(Position, GetDataType<T>());
        //        Position += 1;
        //        switch (type)
        //        {
        //            case 0:
        //                accessor.Write(Position,Convert.ToBoolean(value[0]));
        //                Position += 1;
        //                break;
        //            case 1:
        //                accessor.Write(Position, Convert.ToByte(value[0]));
        //                Position += 1;
        //                break;
        //            case 2:
        //                accessor.Write(Position, Convert.ToInt16(value[0]));
        //                Position += 2;
        //                break;
        //            case 3:
        //                accessor.Write(Position, Convert.ToUInt16(value[0]));
        //                Position += 2;
        //                break;
        //            case 4:
        //                accessor.Write(Position, Convert.ToInt32(value[0]));
        //                Position += 4;
        //                break;
        //            case 5:
        //                accessor.Write(Position, Convert.ToUInt32(value[0]));
        //                Position += 4;
        //                break;
        //            case 6:
        //                accessor.Write(Position, Convert.ToInt64(value[0]));
        //                Position += 8;
        //                break;
        //            case 7:
        //                accessor.Write(Position, Convert.ToUInt64(value[0]));
        //                Position += 8;
        //                break;
        //            case 8:
        //                accessor.Write(Position, Convert.ToSingle(value[0]));
        //                Position += 4;
        //                break;
        //            case 9:
        //                accessor.Write(Position, Convert.ToDouble(value[0]));
        //                Position += 8;
        //                break;
        //            case 10:
        //                accessor.Write(Position, Convert.ToDateTime(value[0]).Ticks);
        //                Position += 8;
        //                break;
        //            case 11:
        //                var bs = Encoding.UTF8.GetBytes(value[0] != null ? value[0].ToString() : "");
        //                short len = (short)Math.Min(bs.Length, 256);
        //                accessor.Write(Position, len);
        //                Position += 2;
        //                accessor.WriteArray<byte>(Position, bs, 0, len);
        //                Position += 254;
        //                break;
        //            case 12:
        //                accessor.Write(Position, Convert.ToInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToInt32(value[1]));
        //                Position += 4;
        //                break;
        //            case 13:
        //                accessor.Write(Position, Convert.ToUInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToUInt32(value[1]));
        //                Position += 4;
        //                break;
        //            case 14:
        //                accessor.Write(Position, Convert.ToInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToInt32(value[1]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToInt32(value[2]));
        //                Position += 4;
        //                break;
        //            case 15:
        //                accessor.Write(Position, Convert.ToUInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToUInt32(value[1]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToUInt32(value[2]));
        //                Position += 4;
        //                break;
        //            case 16:
        //                accessor.Write(Position, Convert.ToInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToInt64(value[1]));
        //                Position += 8;
        //                break;
        //            case 17:
        //                accessor.Write(Position, Convert.ToUInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToUInt64(value[1]));
        //                Position += 8;
        //                break;
        //            case 18:
        //                accessor.Write(Position, Convert.ToInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToInt64(value[1]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToInt64(value[2]));
        //                Position += 8;
        //                break;
        //            case 19:
        //                accessor.Write(Position, Convert.ToUInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToUInt64(value[1]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToUInt64(value[2]));
        //                Position += 8;
        //                break;
        //        }
        //        accessor.Write(Position, quality);
        //        Position += 1;
        //        accessor.Write(Position, (byte)1);
        //        Position += 1;
        //    }
        //}

        /// <summary>
        /// 追加数据
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        public void Append(TagType type, int id, DateTime time, byte quality, params object[] value)
        {
            if (!Enable) return;
           
            lock (mLock)
            {
                mIsWriteBusy = true;
                if (mIsDisposed || mCurrentMemory == null || mCurrentMemory.IsDisposed)
                {
                    mIsWriteBusy = false;
                    return;
                }

              
                try
                {
                    var tsize = 39;

                    if (type == TagType.String)
                    {
                        tsize = 15 + 256;
                    }
                    mCurrentMemory.CheckAndResize(mCurrentMemory.Position + tsize);
                    //CheckAndResize(mCurrentMemory.Position + tsize);
                    mCurrentMemory.Write(id);
                    mCurrentMemory.Write(time.Ticks);
                    mCurrentMemory.Write((byte)type);
                    switch (type)
                    {
                        case TagType.Bool:
                            mCurrentMemory.Write(Convert.ToByte(value[0]));
                            break;
                        case TagType.Byte:
                            mCurrentMemory.Write(Convert.ToByte(value[0]));
                            break;
                        case TagType.Short:
                            mCurrentMemory.Write(Convert.ToInt16(value[0]));
                            break;
                        case TagType.UShort:
                            mCurrentMemory.Write(Convert.ToUInt16(value[0]));
                            break;
                        case TagType.Int:
                            mCurrentMemory.Write(Convert.ToInt32(value[0]));
                            break;
                        case TagType.UInt:
                            mCurrentMemory.Write(Convert.ToUInt32(value[0]));
                            break;
                        case TagType.Long:
                            mCurrentMemory.Write(Convert.ToInt64(value[0]));
                            break;
                        case TagType.ULong:
                            mCurrentMemory.Write(Convert.ToUInt64(value[0]));
                            break;
                        case TagType.Float:
                            mCurrentMemory.Write(Convert.ToSingle(value[0]));
                            break;
                        case TagType.Double:
                            mCurrentMemory.Write(Convert.ToDouble(value[0]));
                            break;
                        case TagType.DateTime:
                            mCurrentMemory.Write(Convert.ToDateTime(value[0]).Ticks);
                            break;
                        case TagType.String:
                            var bs = Encoding.UTF8.GetBytes(value[0] != null ? value[0].ToString() : "");
                            short len = (short)Math.Min(bs.Length, 256);
                            mCurrentMemory.Write(len);
                            mCurrentMemory.WriteBytes(mCurrentMemory.Position, bs, 0, len);
                            break;
                        case TagType.IntPoint:
                            mCurrentMemory.Write(Convert.ToInt32(value[0]));
                            mCurrentMemory.Write(Convert.ToInt32(value[1]));
                            break;
                        case TagType.UIntPoint:
                            mCurrentMemory.Write(Convert.ToUInt32(value[0]));
                            mCurrentMemory.Write(Convert.ToUInt32(value[1]));
                            break;
                        case TagType.IntPoint3:
                            mCurrentMemory.Write(Convert.ToInt32(value[0]));
                            mCurrentMemory.Write(Convert.ToInt32(value[1]));
                            mCurrentMemory.Write(Convert.ToInt32(value[2]));
                            break;
                        case TagType.UIntPoint3:
                            mCurrentMemory.Write(Convert.ToUInt32(value[0]));
                            mCurrentMemory.Write(Convert.ToUInt32(value[1]));
                            mCurrentMemory.Write(Convert.ToUInt32(value[2]));
                            break;
                        case TagType.LongPoint:
                            mCurrentMemory.Write(Convert.ToInt64(value[0]));
                            mCurrentMemory.Write(Convert.ToInt64(value[1]));
                            break;
                        case TagType.ULongPoint:
                            mCurrentMemory.Write(Convert.ToUInt64(value[0]));
                            mCurrentMemory.Write(Convert.ToUInt64(value[1]));
                            break;
                        case TagType.LongPoint3:
                            mCurrentMemory.Write(Convert.ToInt64(value[0]));
                            mCurrentMemory.Write(Convert.ToInt64(value[1]));
                            mCurrentMemory.Write(Convert.ToInt64(value[2]));
                            break;
                        case TagType.ULongPoint3:
                            mCurrentMemory.Write(Convert.ToUInt64(value[0]));
                            mCurrentMemory.Write(Convert.ToUInt64(value[1]));
                            mCurrentMemory.Write(Convert.ToUInt64(value[2]));
                            break;
                    }
                    mCurrentMemory.Write(quality);
                    mCurrentMemory.Write((byte)1);
                }
                catch
                {

                }
                mIsWriteBusy = false;
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="id"></param>
        ///// <param name="time"></param>
        ///// <param name="quality"></param>
        ///// <param name="value"></param>
        //public void Append(TagType type,int id, DateTime time, byte quality, params object[] value)
        //{
        //    if (!Enable) return;
        //    lock (mLock)
        //    {

        //        var tsize = 39;

        //        if (type == TagType.String)
        //        {
        //            tsize = 15 + 256;
        //        }

        //        CheckAndResize(Position + tsize);
        //        accessor.Write(Position, id);
        //        Position += 4;
        //        accessor.Write(Position, time.Ticks);
        //        Position += 8;
        //        accessor.Write(Position, (byte)type);
        //        Position += 1;
        //        switch (type)
        //        {
        //            case TagType.Bool:
        //                accessor.Write(Position, Convert.ToBoolean(value[0]));
        //                Position += 1;
        //                break;
        //            case TagType.Byte:
        //                accessor.Write(Position, Convert.ToByte(value[0]));
        //                Position += 1;
        //                break;
        //            case TagType.Short:
        //                accessor.Write(Position, Convert.ToInt16(value[0]));
        //                Position += 2;
        //                break;
        //            case TagType.UShort:
        //                accessor.Write(Position, Convert.ToUInt16(value[0]));
        //                Position += 2;
        //                break;
        //            case TagType.Int:
        //                accessor.Write(Position, Convert.ToInt32(value[0]));
        //                Position += 4;
        //                break;
        //            case TagType.UInt:
        //                accessor.Write(Position, Convert.ToUInt32(value[0]));
        //                Position += 4;
        //                break;
        //            case TagType.Long:
        //                accessor.Write(Position, Convert.ToInt64(value[0]));
        //                Position += 8;
        //                break;
        //            case TagType.ULong:
        //                accessor.Write(Position, Convert.ToUInt64(value[0]));
        //                Position += 8;
        //                break;
        //            case TagType.Float:
        //                accessor.Write(Position, Convert.ToSingle(value[0]));
        //                Position += 4;
        //                break;
        //            case TagType.Double:
        //                accessor.Write(Position, Convert.ToDouble(value[0]));
        //                Position += 8;
        //                break;
        //            case TagType.DateTime:
        //                accessor.Write(Position, Convert.ToDateTime(value[0]).Ticks);
        //                Position += 8;
        //                break;
        //            case TagType.String:
        //                var bs = Encoding.UTF8.GetBytes(value[0] != null ? value[0].ToString() : "");
        //                short len = (short)Math.Min(bs.Length, 256);
        //                accessor.Write(Position, len);
        //                Position += 2;
        //                accessor.WriteArray<byte>(Position, bs, 0, len);
        //                Position += 254;
        //                break;
        //            case TagType.IntPoint:
        //                accessor.Write(Position, Convert.ToInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToInt32(value[1]));
        //                Position += 4;
        //                break;
        //            case TagType.UIntPoint:
        //                accessor.Write(Position, Convert.ToUInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToUInt32(value[1]));
        //                Position += 4;
        //                break;
        //            case TagType.IntPoint3:
        //                accessor.Write(Position, Convert.ToInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToInt32(value[1]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToInt32(value[2]));
        //                Position += 4;
        //                break;
        //            case TagType.UIntPoint3:
        //                accessor.Write(Position, Convert.ToUInt32(value[0]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToUInt32(value[1]));
        //                Position += 4;
        //                accessor.Write(Position, Convert.ToUInt32(value[2]));
        //                Position += 4;
        //                break;
        //            case TagType.LongPoint:
        //                accessor.Write(Position, Convert.ToInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToInt64(value[1]));
        //                Position += 8;
        //                break;
        //            case TagType.ULongPoint:
        //                accessor.Write(Position, Convert.ToUInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToUInt64(value[1]));
        //                Position += 8;
        //                break;
        //            case TagType.LongPoint3:
        //                accessor.Write(Position, Convert.ToInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToInt64(value[1]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToInt64(value[2]));
        //                Position += 8;
        //                break;
        //            case TagType.ULongPoint3:
        //                accessor.Write(Position, Convert.ToUInt64(value[0]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToUInt64(value[1]));
        //                Position += 8;
        //                accessor.Write(Position, Convert.ToUInt64(value[2]));
        //                Position += 8;
        //                break;
        //        }
        //        accessor.Write(Position, quality);
        //        Position += 1;
        //        accessor.Write(Position, (byte)1);
        //        Position += 1;
        //    }
        //}

        //private long mCount = 0;

        /// <summary>
        /// 追加数据
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        public void Append(TagType type, int id, DateTime time, byte quality, byte[] value)
        {
            if (!Enable) return;

            lock (mLock)
            {
                mIsWriteBusy = true;
                //Console.WriteLine($"Record {id} {DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");
                if (mIsDisposed || mCurrentMemory == null || mCurrentMemory.IsDisposed)
                {
                    mIsWriteBusy = false;
                    return;
                }
                
              
                try
                {
                    var tsize = 39;

                    if (type == TagType.String)
                    {
                        tsize = 15 + 256;
                    }

                    mCurrentMemory.CheckAndResize(mCurrentMemory.Position + tsize);
                    //CheckAndResize(mCurrentMemory.Position + tsize);
                    mCurrentMemory.Write(id);
                    mCurrentMemory.Write(time.Ticks);
                    mCurrentMemory.Write((byte)type);

                    if (type == TagType.String)
                    {
                        mCurrentMemory.Write((short)value.Length);
                        mCurrentMemory.WriteBytes(mCurrentMemory.Position, value, 0, value.Length);
                    }
                    else
                    {
                        mCurrentMemory.WriteBytes(mCurrentMemory.Position, value, 0, value.Length);
                    }

                    mCurrentMemory.Write(quality);
                    mCurrentMemory.Write((byte)1);
                }
                catch
                {

                }
                mIsWriteBusy = false;
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="type"></param>
        ///// <param name="id"></param>
        ///// <param name="time"></param>
        ///// <param name="quality"></param>
        ///// <param name="value"></param>
        //public void Append(TagType type, int id, DateTime time, byte quality, byte[] value)
        //{
        //    if (!Enable) return;

        //    lock (mLock)
        //    {
        //        //Console.WriteLine($"Record {id} {DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.fff")} ");

        //        var tsize = 39;

        //        if (type == TagType.String)
        //        {
        //            tsize = 15 + 256;
        //        }

        //        CheckAndResize(Position + tsize);
        //        accessor.Write(Position, id);
        //        Position += 4;
        //        accessor.Write(Position, time.Ticks);
        //        Position += 8;
        //        accessor.Write(Position, (byte)type);
        //        Position += 1;

        //        if(type== TagType.String)
        //        {
        //            accessor.Write(Position, (short)value.Length);
        //            Position += 2;
        //            accessor.WriteArray<byte>(Position, value, 0, value.Length);
        //            Position += 254;
        //        }
        //        else
        //        {
        //            accessor.WriteArray<byte>(Position, value, 0, value.Length);
        //            Position += value.Length;
        //        }

        //        accessor.Write(Position, quality);
        //        Position += 1;
        //        accessor.Write(Position, (byte)1);
        //        Position += 1;
        //        //mCount++;

        //    }
        //}

        /// <summary>
        /// 读取
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public LogRegion Read(out TagType type,out int id ,out DateTime time,out byte quality,out object value,out bool isavaiable)
        {
            if (Position + 15 > mCalSize)
            {
                type = TagType.Bool;
                id=-1;
                time = DateTime.MinValue;
                quality=0;
                value=null;
                isavaiable=false;
                return this;
            }
            id = accessor.ReadInt32(Position);
            Position += 4;
            time = DateTime.FromBinary(accessor.ReadInt64(Position));

            if(time == DateTime.MinValue)
            {
                type = TagType.Bool;
                id = -1;
                time = DateTime.MinValue;
                quality = 0;
                value = null;
                isavaiable = false;
                return this;
            }

            Position += 8;
            type = (TagType)(accessor.ReadByte(Position));
            Position++;
            switch(type)
            {
                case TagType.Bool:
                    value = accessor.ReadBoolean(Position++);
                    break;
                case TagType.Byte:
                    value = accessor.ReadByte(Position++);
                    break;
                case TagType.Short:
                    value = accessor.ReadInt16(Position);
                    Position += 2;
                    break;
                case TagType.UShort:
                    value = accessor.ReadUInt16(Position);
                    Position += 2;
                    break;
                case TagType.Int:
                    value = accessor.ReadInt32(Position);
                    Position += 4;
                    break;
                case TagType.UInt:
                    value = accessor.ReadUInt32(Position);
                    Position += 4;
                    break;
                case TagType.Long:
                    value = accessor.ReadInt64(Position);
                    Position += 8;
                    break;
                case TagType.ULong:
                    value = accessor.ReadUInt64(Position);
                    Position += 8;
                    break;
                case TagType.Double:
                    value = accessor.ReadDouble(Position);
                    Position += 8;
                    break;
                case TagType.Float:
                    value = accessor.ReadSingle(Position);
                    Position += 4;
                    break;
                case TagType.DateTime:
                    value = DateTime.FromBinary(accessor.ReadInt64(Position));
                    Position += 8;
                    break;
                case TagType.String:
                   var len =  accessor.ReadUInt16(Position);
                    Position += 2;
                    if (len > 0)
                    {
                        var bts = new byte[len];
                        accessor.ReadArray<byte>(Position, bts, 0, bts.Length);
                        value = Encoding.UTF8.GetString(bts);
                    }
                    else
                    {
                        value = "";
                    }
                    Position += 254;
                    break;
                case TagType.IntPoint:
                    var ival1 = accessor.ReadInt32(Position);
                    Position += 4;
                    var ival2 = accessor.ReadInt32(Position);
                    Position += 4;
                    value= new IntPointData(ival1, ival2);
                    break;
                case TagType.IntPoint3:
                    ival1 = accessor.ReadInt32(Position);
                    Position += 4;
                    ival2 = accessor.ReadInt32(Position);
                    Position += 4;
                    var ival3 = accessor.ReadInt32(Position);
                    Position += 4;
                    value = new IntPoint3Data(ival1, ival2,ival3);
                    break;
                case TagType.UIntPoint:
                    var uival1 = accessor.ReadUInt32(Position);
                    Position += 4;
                    var uival2 = accessor.ReadUInt32(Position);
                    Position += 4;
                    value = new UIntPointData(uival1, uival2);
                    break;
                case TagType.UIntPoint3:
                    uival1 = accessor.ReadUInt32(Position);
                    Position += 4;
                    uival2 = accessor.ReadUInt32(Position);
                    Position += 4;
                    var uival3 = accessor.ReadUInt32(Position);
                    Position += 4;
                    value = new UIntPoint3Data(uival1, uival2, uival3);
                    break;
                case TagType.LongPoint:
                    var lval1 = accessor.ReadInt64(Position);
                    Position += 8;
                    var lval2 = accessor.ReadInt64(Position);
                    Position += 8;
                    value = new LongPointData(lval1, lval2);
                    break;
                case TagType.ULongPoint:
                    var ulval1 = accessor.ReadUInt64(Position);
                    Position += 8;
                    var ulval2 = accessor.ReadUInt64(Position);
                    Position += 8;
                    value = new ULongPointData(ulval1, ulval2);
                    break;
                case TagType.LongPoint3:
                    lval1 = accessor.ReadInt64(Position);
                    Position += 8;
                    lval2 = accessor.ReadInt64(Position);
                    Position += 8;
                    var lval3 = accessor.ReadInt64(Position);
                    Position += 8;
                    value = new LongPoint3Data(lval1, lval2, lval3);
                    break;
                case TagType.ULongPoint3:
                    ulval1 = accessor.ReadUInt64(Position);
                    Position += 8;
                    ulval2 = accessor.ReadUInt64(Position);
                    Position += 8;
                    var ulval3 = accessor.ReadUInt64(Position);
                    Position += 8;
                    value = new ULongPoint3Data(ulval1, ulval2, ulval3);
                    break;
                default:
                    value = null;
                    break;
            }
            quality = accessor.ReadByte(Position++);
            isavaiable = accessor.ReadByte(Position++) > 0;
            return this;
        }

        /// <summary>
        /// 读取所有
        /// </summary>
        /// <param name="type"></param>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public IEnumerable<CachItem> ListAllRead()
        {
            while(Position<mCalSize)
            {
                Read(out TagType type,out int id,out DateTime time,out byte quality,out object value,out bool isAvaiable);
                if(isAvaiable)
                {
                    yield return new CachItem() { Id = id, Time = time,Quality = quality,Value = value,Type = type};
                }
                else
                {
                    break;
                }
            }
        }

        //private byte GetDataType<T>()
        //{
        //    var sname = typeof(T).Name.ToLower();

        //    switch (sname)
        //    {
        //        case "boolean":
        //           return 0;
        //        case "byte":
        //            return 1;
        //        case "int16":
        //            return 2;
        //        case "uint16":
        //            return 3;
        //        case "int32":
        //            return 4;
        //        case "uint32":
        //            return 5;
        //        case "int64":
        //            return 6;
        //        case "uint64":
        //            return 7;
        //        case "single":
        //            return 8;
        //        case "double":
        //            return 9;
        //        case "datetime":
        //            return 10;
        //        case "string":
        //            return 11;
        //        case "intpointdata":
        //            return 12;
        //        case "uintpointdata":
        //            return 13;
        //        case "intpoint3data":
        //            return 14;
        //        case "uintpoint3data":
        //            return 15;
        //        case "longpointdata":
        //            return 16;
        //        case "ulongpointdata":
        //            return 17;
        //        case "longpoint3data":
        //            return 18;
        //        case "ulongpoint3data":
        //            return 19;
        //    }

        //    return 0;
        //}

        /// <summary>
        /// 清空缓冲文件
        /// </summary>
        public void Clear()
        {
            lock (mLocker)
            {
                try
                {
                    string spaty = System.IO.Path.Combine(ServiceLocator.Locator.Resolve("DatabaseLocation").ToString(), "Cache", AreaName + Time.ToString("yyyyMMddHHmm") + Id + ".ch");

                    if (!string.IsNullOrEmpty(FileName))
                    {
                        spaty = FileName;
                    }
                    Task.Run(() =>
                    {
                        try
                        {
                            if (System.IO.File.Exists(spaty))
                            {
                                System.IO.File.Delete(spaty);
                            }
                        }
                        catch (Exception eex)
                        {
                            LoggerService.Service.Warn("LogStorageManager", $"{eex.Message} {eex.StackTrace}");
                        }
                    });

                }
                catch (Exception ex)
                {
                    LoggerService.Service.Warn("LogStorageManager", $"{ex.Message} {ex.StackTrace}");
                }
            }
        }

      

        /// <summary>
        /// Dispose
        /// </summary>
        public void Dispose()
        {
            while (mIsWriteBusy || mIsFlushBusy) Thread.Sleep(1);
            mIsDisposed = true;

            if (accessor != null)
            {
                accessor.Dispose();
                accessor= null;
            }
            if(mViewStream!=null)
            {
                mViewStream.Dispose();
                mViewStream = null;
            }
            if (mmf != null)
            {
                mmf.Dispose();
                mmf= null;
            }

            mCachMemroy1?.Dispose();
            mCachMemroy2?.Dispose();
            mCachMemroy2 = null;
            mCachMemroy1 = null;
            mCurrentMemory = null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public struct CachItem
    {
        public TagType Type { get; set; }
        
        public int Id { get; set; }

        public DateTime Time { get; set; }

        public byte Quality { get; set; }
        public object Value { get; set; }
    }
}
