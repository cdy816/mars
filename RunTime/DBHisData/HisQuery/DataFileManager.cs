//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DataFileManager
    {

        #region ... Variables  ...

        private Dictionary<int,Dictionary<int, YearTimeFile>> mTimeFileMaps = new Dictionary<int,Dictionary<int, YearTimeFile>>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, LogFileInfo> mLogFileMaps = new Dictionary<string, LogFileInfo>();

        /// <summary>
        /// 记录所有变量历史记录中最后的时间
        /// </summary>
        internal static Dictionary<string, DateTime> CurrentDateTime = new Dictionary<string, DateTime>();

        /// <summary>
        /// 
        /// </summary>
        private string mDatabaseName;

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFileExtends = ".dbd";

        public const string HisDataFileExtends = ".his";

        /// <summary>
        /// 
        /// </summary>
        public const string LogFileExtends = ".log";

        /// <summary>
        /// 
        /// </summary>
        public const int FileHeadSize = 84;

        private System.IO.FileSystemWatcher hisDataWatcher;

        private System.IO.FileSystemWatcher backhisDataWatcher;

        private System.IO.FileSystemWatcher logDataWatcher;

        private object mLocker = new object();

        private ManualResetEvent mResetEvent = new ManualResetEvent(false);
        private bool mIsClosed = false;

        private Dictionary<string, WatcherChangeTypes> mLogFileCach = new Dictionary<string, WatcherChangeTypes>();

        private Dictionary<string, WatcherChangeTypes> mHisFileCach = new Dictionary<string, WatcherChangeTypes>();

        private int mResetCount = 0;

        private Thread mFileProcessThread;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public DataFileManager(string dbname)
        {
            mDatabaseName = dbname;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 单个文件内变量的个数
        /// </summary>
        public int TagCountOneFile { get; set; } = 100000;

        /// <summary>
        /// 
        /// </summary>
        public string PrimaryHisDataPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string PrimaryLogDataPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string BackHisDataPath { get; set; }

        /// <summary>
        /// 最后的日志时间
        /// </summary>
        public DateTime LastLogTime { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetPrimaryHisDataPath()
        {
            return string.IsNullOrEmpty(PrimaryHisDataPath) ? PathHelper.helper.GetDataPath(this.mDatabaseName,"HisData") : System.IO.Path.IsPathRooted(PrimaryHisDataPath) ? PrimaryHisDataPath : PathHelper.helper.GetDataPath(this.mDatabaseName,PrimaryHisDataPath);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetPrimaryLogDataPath()
        {
            return string.IsNullOrEmpty(PrimaryLogDataPath)?PathHelper.helper.GetDataPath(this.mDatabaseName, "Log"): System.IO.Path.IsPathRooted(PrimaryLogDataPath) ? PrimaryLogDataPath : PathHelper.helper.GetDataPath(this.mDatabaseName, PrimaryLogDataPath);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetBackHisDataPath()
        {
            return string.IsNullOrEmpty(BackHisDataPath) ? "" : System.IO.Path.IsPathRooted(BackHisDataPath) ? BackHisDataPath : PathHelper.helper.GetDataPath(this.mDatabaseName, BackHisDataPath);
        }


        /// <summary>
        /// 
        /// </summary>
        public async Task Int()
        {
            string datapath = GetPrimaryHisDataPath();
            
            await Scan(datapath);

            string backuppath = GetBackHisDataPath();
            if (!string.IsNullOrEmpty(backuppath))
                await Scan(backuppath);

            if (System.IO.Directory.Exists(datapath))
            {
                if (!System.IO.Directory.Exists(datapath))
                {
                    try
                    {
                        System.IO.Directory.CreateDirectory(datapath);
                    }
                    catch
                    {

                    }
                }

                hisDataWatcher = new System.IO.FileSystemWatcher(datapath);
                hisDataWatcher.Changed += HisDataWatcher_Changed;
                hisDataWatcher.EnableRaisingEvents = true;
            }

            if(!string.IsNullOrEmpty(backuppath))
            {
                if (!System.IO.Directory.Exists(backuppath))
                {
                    try
                    {
                        System.IO.Directory.CreateDirectory(backuppath);
                    }
                    catch
                    {

                    }
                }
                
                if (System.IO.Directory.Exists(backuppath))
                {
                    backhisDataWatcher = new System.IO.FileSystemWatcher(backuppath);
                    backhisDataWatcher.Changed += HisDataWatcher_Changed;
                    backhisDataWatcher.EnableRaisingEvents = true;
                }
            }

            //只有在不支持内存查询的情况，才需要监视日志文件
            if (ServiceLocator.Locator.Resolve<IHisQueryFromMemory>() == null)
            {
                string logpath = GetPrimaryLogDataPath();
                ScanLogFile(logpath);
                if (System.IO.Directory.Exists(logpath))
                {
                    logDataWatcher = new System.IO.FileSystemWatcher(logpath);
                    logDataWatcher.Changed += LogDataWatcher_Changed;
                    logDataWatcher.EnableRaisingEvents = true;
                }
            }

            foreach (var vv in this.mTimeFileMaps)
            {
                foreach (var vvv in vv.Value)
                {
                    vvv.Value.UpdateLastDatetime();
                }
            }

           //await Scan(GetBackHisDataPath());
        }

        

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mIsClosed= false;
            mFileProcessThread = new Thread(FileProcess);
            mFileProcessThread.IsBackground = true;
            mFileProcessThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsClosed = true;
            mResetEvent.Close();
            while (mFileProcessThread.IsAlive) Thread.Sleep(1);
        }

        /// <summary>
        /// 
        /// </summary>
        private void FileProcess()
        {
            List<KeyValuePair<string, WatcherChangeTypes>> ltmp = null;
            string backupdatapath = GetBackHisDataPath();

            while (!mIsClosed)
            {
                mResetEvent.WaitOne();
                mResetEvent.Reset();
                if (mIsClosed) break;
                mResetCount = 0;

                while (mResetCount<10)
                {
                    Thread.Sleep(100);
                    mResetCount++;
                }

                if(mLogFileCach.Count>0)
                {
                   
                    lock(mLocker)
                    {
                        ltmp = mLogFileCach.ToList();
                        mLogFileCach.Clear();
                    }

                    foreach(var vv in ltmp)
                    {
                        LoggerService.Service.Info("DataFileMananger", "LogFile " + vv.Value + " add to FileCach！");
                        ParseLogFile(vv.Key);
                    }
                }


                if(mHisFileCach.Count>0)
                {
                    lock (mLocker)
                    {
                        ltmp = this.mHisFileCach.ToList();
                        mHisFileCach.Clear();
                    }

                    foreach (var vv in ltmp)
                    {
                        var vifno = new System.IO.FileInfo(vv.Key);
                        if (vv.Value == System.IO.WatcherChangeTypes.Created)
                        {
                            LoggerService.Service.Info("DataFileMananger", "HisDataFile " + vv.Key + " is Created & will be add to dataFileCach!");
                            if (vifno.Extension == DataFileExtends)
                            {
                                ParseFileName(vifno);
                            }
                        }
                        else
                        {
                            LoggerService.Service.Info("DataFileMananger", "HisDataFile " + vv.Key + " is changed & will be processed!");

                            var vfile = CheckAndGetDataFile(vv.Key);
                            if (vfile != null)
                            {
                                backupdatapath = GetBackHisDataPath();
                                if (!string.IsNullOrEmpty(backupdatapath))
                                {
                                    if(!vfile.FileName.StartsWith(backupdatapath))
                                    {
                                        vfile.UpdateLastDatetime();
                                    }
                                }
                                else
                                {
                                    vfile.UpdateLastDatetime();
                                }
                            }
                            else
                            {
                                if (vifno.Extension == DataFileExtends)
                                    ParseFileName(vifno);
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void LogDataWatcher_Changed(object sender, System.IO.FileSystemEventArgs e)
        {
            mResetEvent.Set();
            mResetCount = 0;
            if (e.ChangeType == System.IO.WatcherChangeTypes.Deleted)
            {
                if(mLogFileMaps.ContainsKey(e.FullPath))
                {
                    mLogFileMaps.Remove(e.FullPath);
                }

                TagHeadOffsetManager.manager.RemoveLogHead(e.FullPath);
            }
            else 
            {
                lock (mLocker)
                {
                    if (!mLogFileCach.ContainsKey(e.FullPath))
                    {
                        mLogFileCach.Add(e.FullPath, e.ChangeType);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void HisDataWatcher_Changed(object sender, System.IO.FileSystemEventArgs e)
        {
            mResetEvent.Set();
            mResetCount = 0;
            if (e.ChangeType == System.IO.WatcherChangeTypes.Created)
            {
                lock (mLocker)
                {
                    if(!mHisFileCach.ContainsKey(e.FullPath))
                    {
                        var vifno = new System.IO.FileInfo(e.FullPath);
                        if (vifno.Extension == DataFileExtends)
                        {
                            mHisFileCach.Add(e.FullPath, e.ChangeType);
                        }
                    }
                }
            }
            else if(e.ChangeType == System.IO.WatcherChangeTypes.Changed)
            {
                lock (mLocker)
                {
                    if (!mHisFileCach.ContainsKey(e.FullPath))
                    {
                        var vifno = new System.IO.FileInfo(e.FullPath);
                        if (vifno.Extension == DataFileExtends)
                        {
                            mHisFileCach.Add(e.FullPath, e.ChangeType);
                        }
                    }
                }
            }
            else if(e.ChangeType == WatcherChangeTypes.Deleted)
            {
                CheckRemoveOldFile(e.FullPath);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        public void ScanLogFile(string path)
        {
            System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(path);
            if (dir.Exists)
            {
                foreach (var vv in dir.GetFiles())
                {
                    if (vv.Extension == LogFileExtends)
                    {
                        ParseLogFile(vv.FullName);
                    }
                }
                //foreach (var vv in dir.GetDirectories())
                //{
                //    await ScanLogFile(vv.FullName);
                //}
            }
        }

        /// <summary>
        /// 搜索文件
        /// </summary>
        /// <param name="path"></param>
        private async Task Scan(string path)
        {
            System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(path);
            if (dir.Exists)
            {
                foreach (var vv in dir.GetFiles())
                {
                    if (vv.Extension == DataFileExtends || vv.Extension == HisDataFileExtends)
                    {
                        ParseFileName(vv);
                    }
                }
                foreach (var vv in dir.GetDirectories())
                {
                    await Scan(vv.FullName);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        private void ParseLogFile(string sfileName)
        {
            var vname = System.IO.Path.GetFileNameWithoutExtension(sfileName);
            DateTime dt = new DateTime(int.Parse(vname.Substring(0, 4)), int.Parse(vname.Substring(4, 2)), int.Parse(vname.Substring(6, 2)), int.Parse(vname.Substring(8, 2)), int.Parse(vname.Substring(10, 2)), int.Parse(vname.Substring(12, 2)));
            int timelen = int.Parse(vname.Substring(14, 3));

            if(!mLogFileMaps.ContainsKey(sfileName))
            {
                var ddt = dt.AddSeconds(timelen);
                mLogFileMaps.Add(sfileName, new LogFileInfo() { FileName = sfileName, StartTime = dt, EndTime = ddt });
                LastLogTime = ddt > LastLogTime ? ddt : LastLogTime;
            }
        }

        private DataFileInfo4 CheckAndGetDataFile(string file)
        {
            string sname = file.Replace(DataFileExtends, "");
            string stime = sname.Substring(sname.Length - 12, 12);
            int yy = 0, mm = 0, dd = 0;

            int id = -1;
            int.TryParse(sname.Substring(sname.Length - 15, 3), out id);

            if (id == -1)
                return null;

            if (!int.TryParse(stime.Substring(0, 4), out yy))
            {
                return null;
            }

            if (!int.TryParse(stime.Substring(4, 2), out mm))
            {
                return null;
            }

            if (!int.TryParse(stime.Substring(6, 2), out dd))
            {
                return null;
            }
            int hhspan = int.Parse(stime.Substring(8, 2));

            int hhind = int.Parse(stime.Substring(10, 2));

            int hh = hhspan * hhind;


            DateTime startTime = new DateTime(yy, mm, dd, hh, 0, 0);
            try
            {
                if (mTimeFileMaps.ContainsKey(id)&& mTimeFileMaps[id].ContainsKey(yy))
                {
                    return mTimeFileMaps[id][yy].GetDataFile(startTime);
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("DataFileMananger", ex.StackTrace);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName"></param>
        public void UpdateFile(string fileName)
        {
            if (System.IO.File.Exists(fileName))
            {
                System.IO.FileInfo finfo = new FileInfo(fileName);
                if (finfo != null)
                {
                    if (finfo.Extension == DataFileExtends || finfo.Extension == HisDataFileExtends)
                    {
                        ParseFileName(finfo);
                    }
                }
            }
            else
            {
                CheckRemoveOldFile(fileName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filename"></param>
        private void CheckRemoveOldFile(string filename)
        {
            string sname = filename.Replace(DataFileExtends, "").Replace(HisDataFileExtends, "");
            string stime = sname.Substring(sname.Length - 12, 12);
            int yy = 0, mm = 0, dd = 0;

            int id = -1;
            int.TryParse(sname.Substring(sname.Length - 15, 3), out id);

            if (id == -1)
                return;

            if (!int.TryParse(stime.Substring(0, 4), out yy))
            {
                return;
            }

            if (!int.TryParse(stime.Substring(4, 2), out mm))
            {
                return;
            }

            if (!int.TryParse(stime.Substring(6, 2), out dd))
            {
                return;
            }
            int hhspan = int.Parse(stime.Substring(8, 2));

            int hhind = int.Parse(stime.Substring(10, 2));

            int hh = hhspan * hhind;


            DateTime startTime = new DateTime(yy, mm, dd, hh, 0, 0);

            YearTimeFile yt = null;

            if (mTimeFileMaps.ContainsKey(id))
            {
                if (mTimeFileMaps[id].ContainsKey(yy))
                {
                    yt = mTimeFileMaps[id][yy];
                }
            }

            if (yt != null)
            {
                yt.CheckFileExist(filename, startTime);
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName"></param>
        private void ParseFileName(System.IO.FileInfo file)
        {
            string sname = file.Name.Replace(DataFileExtends, "").Replace(HisDataFileExtends, "");
            string stime = sname.Substring(sname.Length - 12, 12);
            int yy=0, mm=0, dd=0;

            int id = -1;
            try
            {
                id = int.Parse(sname.Substring(sname.Length - 15, 3), System.Globalization.NumberStyles.HexNumber);

                if (id <0)
                    return;
            }
            catch
            {
                return;
            }

            if (!int.TryParse(stime.Substring(0, 4),out yy))
            {
                return;
            }

            if (!int.TryParse(stime.Substring(4, 2), out mm))
            {
                return;
            }

            if (!int.TryParse(stime.Substring(6, 2), out dd))
            {
                return;
            }
            int hhspan = int.Parse(stime.Substring(8, 2));
            
            int hhind = int.Parse(stime.Substring(10, 2));

            int hh = hhspan * hhind;


            DateTime startTime = new DateTime(yy, mm, dd, hh, 0, 0);

            YearTimeFile yt = new YearTimeFile() { TimeKey = yy };
           
            if(mTimeFileMaps.ContainsKey(id))
            {
                if (mTimeFileMaps[id].ContainsKey(yy))
                {
                    yt = mTimeFileMaps[id][yy];
                }
                else
                {
                    mTimeFileMaps[id].Add(yy, yt);
                }
            }
            else
            {
                mTimeFileMaps.Add(id, new Dictionary<int, YearTimeFile>());
                mTimeFileMaps[id].Add(yy, yt);
            }

            if (file.Extension == DataFileExtends)
            {
                yt.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new DataFileInfo4() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName, FId = mDatabaseName + id });
            }
            else if (file.Extension==HisDataFileExtends)
            {
                yt.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new HisDataFileInfo4() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName, FId = mDatabaseName + id });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private LogFileInfo GetLogDataFile(DateTime time)
        {
            foreach(var vv in mLogFileMaps.Values.ToArray())
            {
                if (vv.StartTime <= time && time < vv.EndTime) return vv;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        private bool  CheckDataInLogFile(DateTime time,int id)
        {
            string sname = mDatabaseName + (id/ TagCountOneFile);
            if(CurrentDateTime.ContainsKey(sname))
            return CurrentDateTime[sname] < time;
            else
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public DataFileInfo4 GetDataFile(DateTime time,int Id)
        {
            int id = Id / TagCountOneFile;

            if (CheckDataInLogFile(time,id))
            {
                //如果查询时间，比最近更新的时间还要新，则需要查询日志文件
                return null;
            }
            else
            {
                if (mTimeFileMaps.ContainsKey(id) && mTimeFileMaps[id].ContainsKey(time.Year))
                {
                    return mTimeFileMaps[id][time.Year].GetDataFile(time);
                }
            }
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public List<DataFileInfo4> GetDataFiles(DateTime starttime, DateTime endtime, out Tuple<DateTime, DateTime> logFileTimes, int Id)
        {
            DateTime dt = starttime;
            var vfiles = GetDataFiles(starttime, endtime - starttime, Id);
            foreach (var vv in vfiles)
            {
                dt = vv.LastTime>dt?vv.LastTime:dt;
            }
            logFileTimes = new Tuple<DateTime, DateTime>(dt, endtime);
            return vfiles;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="span"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public List<DataFileInfo4> GetDataFiles(DateTime startTime, TimeSpan span, int Id)
        {
            List<DataFileInfo4> re = new List<DataFileInfo4>();
            int id = Id / TagCountOneFile;
            if (mTimeFileMaps.ContainsKey(id))
            {
                var nxtYear = new DateTime(startTime.Year+1, 1, 1);
                if (nxtYear > startTime + span)
                {
                    int mon = startTime.Year;
                    if (mTimeFileMaps[id].ContainsKey(mon))
                    {
                        re.AddRange((mTimeFileMaps[id][mon]).GetDataFiles(startTime, span));
                    }
                }
                else
                {
                    int mon = startTime.Year;
                    if (mTimeFileMaps[id].ContainsKey(mon))
                    {
                        re.AddRange((mTimeFileMaps[id][mon]).GetDataFiles(startTime, span));
                    }
                    re.AddRange(GetDataFiles(nxtYear, startTime + span - nxtYear,Id));
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public SortedDictionary<DateTime, DataFileInfo4> GetDataFiles(List<DateTime> times, List<DateTime> logFileTimes,int Id)
        {
            SortedDictionary<DateTime, DataFileInfo4> re = new SortedDictionary<DateTime, DataFileInfo4>();
            foreach(var vv in times)
            {
                if (CheckDataInLogFile(vv, Id))
                {
                    logFileTimes.Add(vv);
                }
                else
                {
                    re.Add(vv, GetDataFile(vv, Id));
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public SortedDictionary<DateTime, LogFileInfo> GetLogDataFiles(List<DateTime> times)
        {
            SortedDictionary<DateTime, LogFileInfo> re = new SortedDictionary<DateTime, LogFileInfo>();
            foreach (var vvd in times)
            {
                re.Add(vvd, GetLogDataFile(vvd));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="endtime"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public List<LogFileInfo> GetLogDataFiles(DateTime startTime, DateTime endtime)
        {
            List<LogFileInfo> re = new List<LogFileInfo>();
            foreach (var vv in mLogFileMaps.ToArray())
            {
                if ((vv.Value.StartTime >= startTime && vv.Value.StartTime < endtime) || (vv.Value.EndTime >= startTime && vv.Value.EndTime < endtime))
                {
                    re.Add(vv.Value);
                }
            }
            return re;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
