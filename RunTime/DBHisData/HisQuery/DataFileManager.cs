﻿//==============================================================
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
using System.Timers;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DataFileManager: IDataFileService
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

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFile2Extends = ".dbd2";

        /// <summary>
        /// 压缩
        /// </summary>
        public const string ZipDataFile2Extends = ".zdbd2";

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataMetaFile2Extends = ".dbm2";


        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string ZipDataMetaFile2Extends = ".zdbm2";

        /// <summary>
        /// 
        /// </summary>
        public const string HisDataFileExtends = ".his";

        /// <summary>
        /// 
        /// </summary>
        public const string ZipHisDataFileExtends = ".zhis";

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
            ServiceLocator.Locator.Registor<IDataFileService>(this);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 单个文件内变量的个数
        /// </summary>
        public  int TagCountOneFile { get; set; } = 100000;

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

        /// <summary>
        /// 第一个值的时间
        /// </summary>
        public DateTime FirstValueTime { get; set; }

        /// <summary>
        /// 最后一个值的时间
        /// </summary>
        public DateTime LastValueTime { get; set; }

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

            await PartScan(datapath);

            if (System.IO.Directory.Exists(datapath))
            {
                hisDataWatcher = new System.IO.FileSystemWatcher(datapath) { IncludeSubdirectories=true};
                hisDataWatcher.Changed += HisDataWatcher_Changed;
                hisDataWatcher.EnableRaisingEvents = true;
            }


            string backuppath = GetBackHisDataPath();

            if (!string.IsNullOrEmpty(backuppath))
            {
                await PartScan(backuppath);

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
                    backhisDataWatcher = new System.IO.FileSystemWatcher(backuppath) { IncludeSubdirectories=true};
                    backhisDataWatcher.Changed += HisDataWatcher_Changed;
                    backhisDataWatcher.EnableRaisingEvents = true;
                }
            }

            ////只有在不支持内存查询的情况，才需要监视日志文件
            //if (ServiceLocator.Locator.Resolve<IHisQueryFromMemory>() == null)
            //{
            //    string logpath = GetPrimaryLogDataPath();
            //    ScanLogFile(logpath);

            //    if (!System.IO.Directory.Exists(logpath))
            //    {
            //        try
            //        {
            //            System.IO.Directory.CreateDirectory(logpath);
            //        }
            //        catch
            //        {

            //        }
            //    }

            //    if (System.IO.Directory.Exists(logpath))
            //    {
            //        logDataWatcher = new System.IO.FileSystemWatcher(logpath);
            //        logDataWatcher.Changed += LogDataWatcher_Changed;
            //        logDataWatcher.EnableRaisingEvents = true;
            //    }
            //}

            foreach (var vv in this.mTimeFileMaps)
            {
                foreach (var vvv in vv.Value)
                {
                    vvv.Value.UpdateLastDatetime();
                }
            }

            ScanFirstValueTime();
            ScanLastValueTime();

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
            mResetEvent.Set();
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

                //if(mLogFileCach.Count>0)
                //{
                   
                //    lock(mLocker)
                //    {
                //        ltmp = mLogFileCach.ToList();
                //        mLogFileCach.Clear();
                //    }

                //    foreach(var vv in ltmp)
                //    {
                //        LoggerService.Service.Info("DataFileMananger", "LogFile " + vv.Value + " add to FileCach！");
                //        ParseLogFile(vv.Key);
                //    }
                //}


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
                            if ((vifno.Extension == DataFileExtends)||(vifno.Extension == DataFile2Extends))
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
                                if ((vifno.Extension == DataFileExtends) || (vifno.Extension == DataFile2Extends))
                                    ParseFileName(vifno);
                            }
                        }
                    }
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="sender"></param>
        ///// <param name="e"></param>
        //private void LogDataWatcher_Changed(object sender, System.IO.FileSystemEventArgs e)
        //{
            
        //    if (e.ChangeType == System.IO.WatcherChangeTypes.Deleted)
        //    {
        //        if(mLogFileMaps.ContainsKey(e.FullPath))
        //        {
        //            mLogFileMaps.Remove(e.FullPath);
        //        }

        //        TagHeadOffsetManager.manager.RemoveLogHead(e.FullPath);
        //    }
        //    else 
        //    {
        //        lock (mLocker)
        //        {
        //            if (!mLogFileCach.ContainsKey(e.FullPath))
        //            {
        //                mLogFileCach.Add(e.FullPath, e.ChangeType);
        //            }
        //        }
        //    }

        //    mResetCount = 0;
        //    mResetEvent.Set();
            
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void HisDataWatcher_Changed(object sender, System.IO.FileSystemEventArgs e)
        {
            //过滤掉临时生成文件
            if(e.FullPath.Contains(@"\tmp\"))
            {
                return;
            }

            if (e.ChangeType == System.IO.WatcherChangeTypes.Created)
            {
                lock (mLocker)
                {
                    if(!mHisFileCach.ContainsKey(e.FullPath))
                    {
                        var vifno = new System.IO.FileInfo(e.FullPath);
                        if (vifno.Extension == DataFileExtends||vifno.Extension == DataFile2Extends)
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
                        if (vifno.Extension == DataFileExtends || vifno.Extension == DataFile2Extends)
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

            mResetCount = 0;
            mResetEvent.Set();
           
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="path"></param>
        //public void ScanLogFile(string path)
        //{
        //    System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(path);
        //    if (dir.Exists)
        //    {
        //        foreach (var vv in dir.GetFiles())
        //        {
        //            if (vv.Extension == LogFileExtends)
        //            {
        //                ParseLogFile(vv.FullName);
        //            }
        //        }
        //        //foreach (var vv in dir.GetDirectories())
        //        //{
        //        //    await ScanLogFile(vv.FullName);
        //        //}
        //    }
        //}

        public void ScanLastValueTime()
        {
            List<int> mtmps = new List<int>();
            //从主路径中搜索
            var spath = GetPrimaryHisDataPath();
            if (System.IO.Directory.Exists(spath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(spath).GetDirectories())
                {
                    if (int.TryParse(vv.Name, out int mm))
                    {
                        if (!mtmps.Contains(mm))
                            mtmps.Add(mm);
                    }
                }
            }

            //从备份路径中搜索
            spath = GetBackHisDataPath();
            if (System.IO.Directory.Exists(spath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(spath).GetDirectories())
                {
                    if (int.TryParse(vv.Name, out int mm))
                    {
                        if (!mtmps.Contains(mm))
                            mtmps.Add(mm);
                    }
                }
            }

            foreach (var vv in mtmps)
            {
                var vtime = ScanHasValueLastTime(vv);
                if (this.LastValueTime == DateTime.MinValue || this.LastValueTime < vtime)
                {
                    this.LastValueTime = vtime;
                }
            }
        }

        public DateTime ScanHasValueLastTime(int id)
        {
            int pid = id;
            string spath = System.IO.Path.Combine(GetPrimaryHisDataPath(), pid.ToString("X3"));
            if (System.IO.Directory.Exists(spath))
            {
                var vtime = ScanHasValueLastYear(spath);
                if (vtime != DateTime.MinValue)
                {
                    return vtime;
                }
            }
            //搜索备份路径
            spath = System.IO.Path.Combine(GetBackHisDataPath(), pid.ToString("X3"));
            if(System.IO.Directory.Exists(spath))
            {
                var vtime = ScanHasValueLastYear(spath);
                if (vtime != DateTime.MinValue)
                {
                    return vtime;
                }
            }
            return DateTime.MinValue;
        }

        private DateTime ScanHasValueLastYear(string path)
        {
            SortedDictionary<int,string> mtmps = new SortedDictionary<int, string>();
            foreach(var vv in new System.IO.DirectoryInfo(path).GetDirectories())
            {
                if (int.TryParse(vv.Name, out int mm))
                {
                    if (!mtmps.ContainsKey(mm))
                        mtmps.Add(mm, vv.FullName);
                }
            }
            foreach(var vv in mtmps.Keys.OrderByDescending(x => x))
            {
                var time = ScanHasValueLastMonth(mtmps[vv]);
                if(time!=DateTime.MinValue)
                {
                    return time;
                }
            }
            return DateTime.MinValue;
        }

        private DateTime ScanHasValueLastMonth(string path)
        {
            SortedDictionary<int, string> mtmps = new SortedDictionary<int, string>();
            foreach (var vv in new System.IO.DirectoryInfo(path).GetDirectories())
            {
                if (int.TryParse(vv.Name, out int mm))
                {
                    if (!mtmps.ContainsKey(mm))
                        mtmps.Add(mm, vv.FullName);
                }
            }
            foreach (var vv in mtmps.Keys.OrderByDescending(x => x))
            {
                var time = ScanHasValueLastDay(mtmps[vv]);
                if (time != DateTime.MinValue)
                {
                    return time;
                }
            }
            return DateTime.MinValue;
        }

        private DateTime ScanHasValueLastDay(string path)
        {
            SortedDictionary<int, string> mtmps = new SortedDictionary<int, string>();
            foreach (var vv in new System.IO.DirectoryInfo(path).GetDirectories())
            {
                if (int.TryParse(vv.Name, out int mm))
                {
                    if (!mtmps.ContainsKey(mm))
                        mtmps.Add(mm, vv.FullName);
                }
            }
            foreach (var vv in mtmps.Keys.OrderByDescending(x => x))
            {
                var time = ScanHasValueLastDayInner(mtmps[vv]);
                if (time != DateTime.MinValue)
                {
                    return time;
                }
            }
            return DateTime.MinValue;
        }

        private DateTime ScanHasValueLastDayInner(string path)
        {
            List<DateTime> dates = new List<DateTime>();
            foreach (var vv in new System.IO.DirectoryInfo(path).GetFiles())
            {
                if (vv.Extension == DataFileExtends || vv.Extension == HisDataFileExtends || vv.Extension == ZipHisDataFileExtends || vv.Extension == DataFile2Extends || vv.Extension == ZipDataFile2Extends)
                {
                    DateTime startTime = ParseFileToTime(vv.Name, out int id, out int hhspan);
                    dates.Add(startTime.AddHours(hhspan));
                }
            }
            if (dates.Count > 0)
            {
                return dates.Max();
            }
            else
            {
                return DateTime.MinValue;
            }
        }

        public void ScanFirstValueTime()
        {
            List<int> mtmps = new List<int>();
            //从主路径中搜索
            var spath = GetPrimaryHisDataPath();
            if (System.IO.Directory.Exists(spath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(spath).GetDirectories())
                {
                    if (int.TryParse(vv.Name, out int mm))
                    {
                        if (!mtmps.Contains(mm))
                            mtmps.Add(mm);
                    }
                }
            }

            //从备份路径中搜索
            spath = GetBackHisDataPath();
            if (System.IO.Directory.Exists(spath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(spath).GetDirectories())
                {
                    if (int.TryParse(vv.Name, out int mm))
                    {
                        if (!mtmps.Contains(mm))
                            mtmps.Add(mm);
                    }
                }
            }

            foreach (var vv in mtmps)
            {
                var vtime = ScanHasValueFirstTime(vv);
                if(this.FirstValueTime==DateTime.MinValue || this.FirstValueTime>vtime)
                {
                   this.FirstValueTime = vtime;
                }
            }
        }

        public DateTime ScanHasValueFirstTime(int id)
        {
            int pid = id;
            string spath = System.IO.Path.Combine(GetPrimaryHisDataPath(), pid.ToString("X3"));
            if (System.IO.Directory.Exists(spath))
            {
                var vtime = ScanHasValueFirstYear(spath);
                if (vtime != DateTime.MinValue)
                {
                    return vtime;
                }
            }
            //搜索备份路径
            spath = System.IO.Path.Combine(GetBackHisDataPath(), pid.ToString("X3"));
            if (System.IO.Directory.Exists(spath))
            {
                var vtime = ScanHasValueFirstYear(spath);
                if (vtime != DateTime.MinValue)
                {
                    return vtime;
                }
            }
            return DateTime.MinValue;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        /// <returns></returns>
        private DateTime ScanHasValueFirstYear(string path)
        {
            SortedDictionary<int, string> mtmps = new SortedDictionary<int, string>();
            foreach (var vv in new System.IO.DirectoryInfo(path).GetDirectories())
            {
                if (int.TryParse(vv.Name, out int mm))
                {
                    if (!mtmps.ContainsKey(mm))
                        mtmps.Add(mm, vv.FullName);
                }
            }
            foreach (var vv in mtmps.Keys)
            {
                var time = ScanHasValueFirstMonth(mtmps[vv]);
                if (time != DateTime.MinValue)
                {
                    return time;
                }
            }
            return DateTime.MinValue;
        }

        private DateTime ScanHasValueFirstMonth(string path)
        {
            SortedDictionary<int, string> mtmps = new SortedDictionary<int, string>();
            foreach (var vv in new System.IO.DirectoryInfo(path).GetDirectories())
            {
                if (int.TryParse(vv.Name, out int mm))
                {
                    if (!mtmps.ContainsKey(mm))
                        mtmps.Add(mm, vv.FullName);
                }
            }
            foreach (var vv in mtmps.Keys)
            {
                var time = ScanHasValueFirstDay(mtmps[vv]);
                if (time != DateTime.MinValue)
                {
                    return time;
                }
            }
            return DateTime.MinValue;
        }

        private DateTime ScanHasValueFirstDay(string path)
        {
            SortedDictionary<int, string> mtmps = new SortedDictionary<int, string>();
            foreach (var vv in new System.IO.DirectoryInfo(path).GetDirectories())
            {
                if (int.TryParse(vv.Name, out int mm))
                {
                    if (!mtmps.ContainsKey(mm))
                        mtmps.Add(mm, vv.FullName);
                }
            }
            foreach (var vv in mtmps.Keys)
            {
                var time = ScanHasValueFirstDayInner(mtmps[vv]);
                if (time != DateTime.MinValue)
                {
                    return time;
                }
            }
            return DateTime.MinValue;
        }

        private DateTime ScanHasValueFirstDayInner(string path)
        {
            List<DateTime> dates = new List<DateTime>();
            foreach (var vv in new System.IO.DirectoryInfo(path).GetFiles())
            {
                if (vv.Extension == DataFileExtends || vv.Extension == HisDataFileExtends || vv.Extension == ZipHisDataFileExtends || vv.Extension == DataFile2Extends || vv.Extension == ZipDataFile2Extends)
                {
                    DateTime startTime = ParseFileToTime(vv.Name, out int id, out int hhspan);
                    if (startTime != DateTime.MinValue)
                        dates.Add(startTime);
                }
            }

            if (dates.Count > 0)
            {
                return dates.Min();
            }
            else
            {
                return DateTime.MinValue;
            }
        }

        private async Task PartScan(string rootpath)
        {
            //扫描近2个月内的数据，防止数据量过大
            DateTime dnow = DateTime.Now;
            System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(rootpath);
            if(dir.Exists)
            {
                foreach(var vdir in dir.GetDirectories())
                {

                    DateTime dtmp = dnow.AddMonths(-1);
                    string spath = System.IO.Path.Combine(vdir.FullName, dtmp.Year.ToString(), dtmp.Month.ToString());
                    if(System.IO.Directory.Exists(spath))
                    {
                       await Scan(spath);
                    }

                    dtmp = dnow;
                    spath = System.IO.Path.Combine(vdir.FullName, dtmp.Year.ToString(), dtmp.Month.ToString());
                    if (System.IO.Directory.Exists(spath))
                    {
                        await Scan(spath);
                    }

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        private async Task PartScan(int id, DateTime time)
        {
            string datapath = GetPrimaryHisDataPath();
            await PartScan(id, datapath, time);
            var backdatapath = GetBackHisDataPath();
            if(!string.IsNullOrEmpty(backdatapath))
            {
                await PartScan(id, backdatapath, time);
            }
            int pid = id / TagCountOneFile;
            if (!mTimeFileMaps.ContainsKey(pid))
            {
                var dd = new Dictionary<int, YearTimeFile>();
                dd.Add(time.Year, new YearTimeFile() { DataPath = System.IO.Path.Combine(datapath,pid.ToString("X3")), BackPath = System.IO.Path.Combine(backdatapath, pid.ToString("X3")),Database=this.mDatabaseName });
                mTimeFileMaps.Add(pid, dd);
            }
            else if(!mTimeFileMaps[pid].ContainsKey(time.Year))
            {
                mTimeFileMaps[pid].Add(time.Year, new YearTimeFile() { DataPath = System.IO.Path.Combine(datapath, pid.ToString("X3")), BackPath = System.IO.Path.Combine(backdatapath, pid.ToString("X3")), Database = this.mDatabaseName });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="rootpath"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        private async Task PartScan(int id,string rootpath,DateTime time)
        {
            int pid = id / TagCountOneFile;
            string spath = System.IO.Path.Combine(rootpath, pid.ToString("X3"));
            if(System.IO.Directory.Exists(spath))
            {
                spath = System.IO.Path.Combine(spath, time.Year.ToString(), time.Month.ToString(),time.Day.ToString());
                await Scan(spath);
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
                    if (vv.Extension == DataFileExtends || vv.Extension == HisDataFileExtends || vv.Extension == ZipHisDataFileExtends || vv.Extension == DataFile2Extends || vv.Extension == ZipDataFile2Extends)
                    {
                        ParseFileName(vv);
                    }
                }
                foreach (var vv in dir.GetDirectories())
                {
                    if(vv.Name!="tmp")
                    await Scan(vv.FullName);
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="file"></param>
        //private void ParseLogFile(string sfileName)
        //{
        //    var vname = System.IO.Path.GetFileNameWithoutExtension(sfileName);
        //    DateTime dt = new DateTime(int.Parse(vname.Substring(0, 4)), int.Parse(vname.Substring(4, 2)), int.Parse(vname.Substring(6, 2)), int.Parse(vname.Substring(8, 2)), int.Parse(vname.Substring(10, 2)), int.Parse(vname.Substring(12, 2)));
        //    int timelen = int.Parse(vname.Substring(14, 3));

        //    if(!mLogFileMaps.ContainsKey(sfileName))
        //    {
        //        var ddt = dt.AddSeconds(timelen);

        //        if(dt.Minute == ddt.Minute)
        //        {
        //            ddt= ddt.AddMilliseconds(999);
        //        }

        //        mLogFileMaps.Add(sfileName, new LogFileInfo() { FileName = sfileName, StartTime = dt, EndTime = ddt });
        //        LastLogTime = ddt > LastLogTime ? ddt : LastLogTime;
        //    }
        //}

        public static DateTime ParseFileToTime(string file,out int id,out int hhspan)
        {
            string sname = System.IO.Path.GetFileNameWithoutExtension(file);
            string stime = sname.Substring(sname.Length - 12, 12);
            int yy = 0, mm = 0, dd = 0;

            id = -1;
            hhspan = 0;
            int.TryParse(sname.Substring(sname.Length - 15, 3), out id);

            if (id == -1)
                return DateTime.MinValue;

            if (!int.TryParse(stime.Substring(0, 4), out yy))
            {
                return DateTime.MinValue;
            }

            if (!int.TryParse(stime.Substring(4, 2), out mm))
            {
                return DateTime.MinValue;
            }

            if (!int.TryParse(stime.Substring(6, 2), out dd))
            {
                return DateTime.MinValue;
            }
            hhspan = int.Parse(stime.Substring(8, 2));

            int hhind = int.Parse(stime.Substring(10, 2));

            int hh = hhspan * hhind;

            return new DateTime(yy, mm, dd, hh, 0, 0);
        }

        private IDataFile CheckAndGetDataFile(string file)
        {
            try
            {
                //string sname =  System.IO.Path.GetFileNameWithoutExtension(file);
                //string stime = sname.Substring(sname.Length - 12, 12);
                //int yy = 0, mm = 0, dd = 0;

                //int id = -1;
                //int.TryParse(sname.Substring(sname.Length - 15, 3), out id);

                //if (id == -1)
                //    return null;

                //if (!int.TryParse(stime.Substring(0, 4), out yy))
                //{
                //    return null;
                //}

                //if (!int.TryParse(stime.Substring(4, 2), out mm))
                //{
                //    return null;
                //}

                //if (!int.TryParse(stime.Substring(6, 2), out dd))
                //{
                //    return null;
                //}
                //int hhspan = int.Parse(stime.Substring(8, 2));

                //int hhind = int.Parse(stime.Substring(10, 2));

                //int hh = hhspan * hhind;
                DateTime startTime = ParseFileToTime(file,out int id,out int hhspan);
            
                if (mTimeFileMaps.ContainsKey(id)&& mTimeFileMaps[id].ContainsKey(startTime.Year))
                {
                    return mTimeFileMaps[id][startTime.Year].GetDataFile(startTime);
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
                    if (finfo.Extension == DataFile2Extends || finfo.Extension == ZipDataFile2Extends || finfo.Extension == DataFileExtends || finfo.Extension == HisDataFileExtends || finfo.Extension == ZipHisDataFileExtends)
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
            //string sname = filename.Replace(DataFileExtends, "").Replace(HisDataFileExtends, "");
            //string sname = System.IO.Path.GetFileNameWithoutExtension(filename);
            //string stime = sname.Substring(sname.Length - 12, 12);
            //int yy = 0, mm = 0, dd = 0;

            //int id = -1;
            //int.TryParse(sname.Substring(sname.Length - 15, 3), out id);

            //if (id == -1)
            //    return;

            //if (!int.TryParse(stime.Substring(0, 4), out yy))
            //{
            //    return;
            //}

            //if (!int.TryParse(stime.Substring(4, 2), out mm))
            //{
            //    return;
            //}

            //if (!int.TryParse(stime.Substring(6, 2), out dd))
            //{
            //    return;
            //}
            //int hhspan = int.Parse(stime.Substring(8, 2));

            //int hhind = int.Parse(stime.Substring(10, 2));

            //int hh = hhspan * hhind;


            //DateTime startTime = new DateTime(yy, mm, dd, hh, 0, 0);

            DateTime startTime = ParseFileToTime(filename, out int id, out int hhspan);

            YearTimeFile yt = null;

            if (mTimeFileMaps.ContainsKey(id))
            {
                if (mTimeFileMaps[id].ContainsKey(startTime.Year))
                {
                    yt = mTimeFileMaps[id][startTime.Year];
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


            DateTime startTime = ParseFileToTime(file.Name, out int id, out int hhspan);

            int yy = startTime.Year;

            YearTimeFile yt = new YearTimeFile() { TimeKey = startTime.Year ,BackPath= System.IO.Path.Combine(GetBackHisDataPath(),id.ToString("X3")),DataPath=System.IO.Path.Combine( GetPrimaryHisDataPath(),id.ToString("X3")),Database=this.mDatabaseName};
           
            if(mTimeFileMaps.ContainsKey(id))
            {
                if (mTimeFileMaps[id].ContainsKey(startTime.Year))
                {
                    yt = mTimeFileMaps[id][startTime.Year];
                }
                else
                {
                    mTimeFileMaps[id].Add(startTime.Year, yt);
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
            else if (file.Extension==HisDataFileExtends || file.Extension== ZipHisDataFileExtends)
            {
                yt.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new HisDataFileInfo4() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName, FId = mDatabaseName + id ,IsZipFile= (file.Extension == ZipHisDataFileExtends) });
            }
            else if (file.Extension == DataFile2Extends || file.Extension == ZipDataFile2Extends)
            {
                yt.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new DataFileInfo6() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName, FId = mDatabaseName + id, IsZipFile = (file.Extension == ZipDataFile2Extends) });
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
        public bool  CheckDataInLogFile(DateTime time,int id)
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
        /// <param name="id"></param>
        /// <returns></returns>
        public bool CheckDataInLogFile(DateTime time, string sname)
        {
            if (CurrentDateTime.ContainsKey(sname))
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
        public IDataFile GetDataFile(DateTime time,int Id)
        {

            int id = Id / TagCountOneFile;
            if (mTimeFileMaps.Count == 0 || !mTimeFileMaps.ContainsKey(id))
            {
                PartScan(id, time).Wait();
            }
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
        /// <param name="time"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public IDataFile GetDataFileWithoutCheck(DateTime time, int Id)
        {

            int id = Id / TagCountOneFile;
            if (mTimeFileMaps.Count == 0 || !mTimeFileMaps.ContainsKey(id))
            {
                PartScan(id, time).Wait();
            }
            if (mTimeFileMaps.ContainsKey(id) && mTimeFileMaps[id].ContainsKey(time.Year))
            {
                return mTimeFileMaps[id][time.Year].GetDataFile(time);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Id"></param>
        /// <param name="dateTimes"></param>
        /// <param name="dic"></param>
        /// <param name="logfiles"></param>
        public void FillDataFile(int Id, Dictionary<int, Dictionary<int, RecordList<DateTime>>> dateTimes, SortedDictionary<DateTime, IDataFile> dic, List<DateTime> logFileTimes)
        {
            int id = Id / TagCountOneFile;
            if (mTimeFileMaps.Count == 0 || !mTimeFileMaps.ContainsKey(id))
            {
                PartScan(id, dateTimes.First().Value.First().Value.First()).Wait();
            }

            if (mTimeFileMaps.ContainsKey(id))
            {
                foreach (var vv in dateTimes)
                {
                    if (mTimeFileMaps[id].ContainsKey(vv.Key))
                    {
                        mTimeFileMaps[id][vv.Key].FillDataFile(vv.Value, dic, logFileTimes);
                    }
                    else
                    {
                        foreach (var vvt in vv.Value)
                        {
                            logFileTimes.AddRange(vvt.Value);
                        }
                    }
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="Id"></param>
        /// <param name="dateTimes"></param>
        /// <param name="dic"></param>
        /// <param name="logfiles"></param>
        public void FillDataFile(int Id, Dictionary<int, Dictionary<int, RecordList<DateTime>>> dateTimes, SortedDictionary<DateTime, IDataFile> dic, ArrayList<DateTime> logFileTimes)
        {
            int id = Id / TagCountOneFile;
            if (mTimeFileMaps.Count == 0 || !mTimeFileMaps.ContainsKey(id))
            {
                PartScan(id, dateTimes.First().Value.First().Value.First()).Wait();
            }

            if (mTimeFileMaps.ContainsKey(id))
            {
                foreach (var vv in dateTimes)
                {
                    if (mTimeFileMaps[id].ContainsKey(vv.Key))
                    {
                        mTimeFileMaps[id][vv.Key].FillDataFile(vv.Value, dic, logFileTimes);
                    }
                    else
                    {
                        foreach (var vvt in vv.Value)
                        {
                            logFileTimes.AddRange(vvt.Value);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public List<IDataFile> GetDataFiles(DateTime starttime, DateTime endtime, out Tuple<DateTime, DateTime> logFileTimes, int Id)
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
        public List<IDataFile> GetDataFiles(DateTime startTime, TimeSpan span, int Id)
        {
            List<IDataFile> re = new List<IDataFile>();
            int id = Id / TagCountOneFile;
            if (mTimeFileMaps.Count == 0 || !mTimeFileMaps.ContainsKey(id))
            {
                PartScan(id, startTime).Wait();
            }

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
                    else
                    {
                        PartScan(id, startTime).Wait();
                        if (mTimeFileMaps[id].ContainsKey(mon))
                        {
                            re.AddRange((mTimeFileMaps[id][mon]).GetDataFiles(startTime, span));
                        }
                    }
                }
                else
                {
                    int mon = startTime.Year;
                    if (mTimeFileMaps[id].ContainsKey(mon))
                    {
                        re.AddRange((mTimeFileMaps[id][mon]).GetDataFiles(startTime, span));
                    }
                    else
                    {
                        PartScan(id, startTime).Wait();
                        if (mTimeFileMaps[id].ContainsKey(mon))
                        {
                            re.AddRange((mTimeFileMaps[id][mon]).GetDataFiles(startTime, span));
                        }
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
        public SortedDictionary<DateTime, IDataFile> GetDataFiles(List<DateTime> times, List<DateTime> logFileTimes,int Id)
        {
            SortedDictionary<DateTime, IDataFile> re = new SortedDictionary<DateTime, IDataFile>();
            foreach (var vv in times)
            {
                if (CheckDataInLogFile(vv, Id))
                {
                    logFileTimes.Add(vv);
                }
                else
                {
                    var df = GetDataFile(vv, Id);
                    if (df != null)
                        re.Add(vv, df);
                    else
                    {
                        logFileTimes.Add(vv);
                    }
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
        public SortedDictionary<DateTime, IDataFile> GetDataFiles(List<DateTime> times, ArrayList<DateTime> logFileTimes, int Id)
        {
            SortedDictionary<DateTime, IDataFile> re = new SortedDictionary<DateTime, IDataFile>();
            foreach (var vv in times)
            {
                if (CheckDataInLogFile(vv, Id))
                {
                    logFileTimes.Add(vv);
                }
                else
                {
                    var df = GetDataFile(vv, Id);
                    if (df != null)
                        re.Add(vv, df);
                    else
                    {
                        logFileTimes.Add(vv);
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <param name="logFileTimes"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public SortedDictionary<DateTime, IDataFile> GetDataFiles(TimeValueDictionary times, List<DateTime> logFileTimes, int Id)
        {
            SortedDictionary<DateTime, IDataFile> re = new SortedDictionary<DateTime, IDataFile>();
            FillDataFile(Id,times, re,logFileTimes);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <param name="logFileTimes"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public SortedDictionary<DateTime, IDataFile> GetDataFiles(TimeValueDictionary times, ArrayList<DateTime> logFileTimes, int Id)
        {
            SortedDictionary<DateTime, IDataFile> re = new SortedDictionary<DateTime, IDataFile>();
            FillDataFile(Id, times, re, logFileTimes);
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public bool CheckHisFileExist(string filePath, out string realfilepath)
        {
            string sfile = System.IO.Path.Combine(GetPrimaryHisDataPath(), filePath);
            if (File.Exists(sfile))
            {
                realfilepath = sfile;
                return true;
            }
            if(!string.IsNullOrEmpty(BackHisDataPath))
            {
                sfile = System.IO.Path.Combine (GetBackHisDataPath(), filePath);
                if(File.Exists(sfile))
                {
                    realfilepath=sfile;
                    return true;
                }
            }
            realfilepath = null;
            return false;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class TimeValueDictionary : Dictionary<int, Dictionary<int, RecordList<DateTime>>>,IDisposable
    {
        private int mCount = 0;

        /// <summary>
        /// 
        /// </summary>
        public int Count
        {
            get
            {
                return mCount;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void AppendTime(DateTime time)
        {
            mCount++;
            var dd = time.Year;
            var mm = time.Month;
            if(this.ContainsKey(dd))
            {
                if(this[dd].ContainsKey(mm))
                {
                    this[dd][mm].Add(time);
                }
                else
                {
                    this[dd].Add(mm, new RecordList<DateTime>() { time });
                }
            }
            else
            {
                Dictionary<int, RecordList<DateTime>> md = new Dictionary<int, RecordList<DateTime>>();
                md.Add(mm, new RecordList<DateTime> { time });
                this.Add(dd, md);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        public void AddRang(IEnumerable<DateTime> times)
        {
            foreach(var vv in times)
            {
                AppendTime(vv);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            foreach(var vv in this)
            {
                foreach(var vvv in vv.Value)
                {
                    vvv.Value.Dispose();
                }
            }
            this.Clear();
        }
    }


    /// <summary>
    /// 
    /// </summary>
    /// <typeparam name="T"></typeparam>
    /// <typeparam name="TT"></typeparam>
    public class RecordList<T> : List<T>,IDisposable
    {
        public int Current { get; set; } = 0;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public T Get()
        {
            return this[Current++];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public T PeekGet()
        {
            return this[Current];
        }

        /// <summary>
        /// 
        /// </summary>
        public void Next()
        {
            Current++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool CanGet()
        {
            return Current < Count;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            Current = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public interface IDataFileService
    {
        public bool CheckHisFileExist(string filePath,out string realfilepath);

    }
}
