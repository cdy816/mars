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
using System.Linq;
using System.Text;
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
        /// 
        /// </summary>
        internal static Dictionary<string, DateTime> CurrentDateTime = new Dictionary<string, DateTime>();

        private string mDatabaseName;

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFileExtends = ".dbd";

        public const string LogFileExtends = ".log";

        /// <summary>
        /// 
        /// </summary>
        public const int FileHeadSize = 72;

        private System.IO.FileSystemWatcher hisDataWatcher;

        private System.IO.FileSystemWatcher logDataWatcher;

        private object mLocker = new object();
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


        public string PrimaryLogDataPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string BackHisDataPath { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetPrimaryHisDataPath()
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
        private string GetBackHisDataPath()
        {
            return string.IsNullOrEmpty(BackHisDataPath) ? PathHelper.helper.GetDataPath(this.mDatabaseName, "HisData") : System.IO.Path.IsPathRooted(BackHisDataPath) ? BackHisDataPath : PathHelper.helper.GetDataPath(this.mDatabaseName, BackHisDataPath);
        }


        /// <summary>
        /// 
        /// </summary>
        public async Task Int()
        {
            string datapath = GetPrimaryHisDataPath();
            await Scan(datapath);
            if (System.IO.Directory.Exists(datapath))
            {
                hisDataWatcher = new System.IO.FileSystemWatcher(GetPrimaryHisDataPath());
                hisDataWatcher.Changed += HisDataWatcher_Changed;
                hisDataWatcher.EnableRaisingEvents = true;
            }

            string logpath = GetPrimaryLogDataPath();
            if (System.IO.Directory.Exists(logpath))
            {
                logDataWatcher = new System.IO.FileSystemWatcher(logpath);
                logDataWatcher.Changed += LogDataWatcher_Changed;

                logDataWatcher.EnableRaisingEvents = true;
            }

           //await Scan(GetBackHisDataPath());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void LogDataWatcher_Changed(object sender, System.IO.FileSystemEventArgs e)
        {
            if (e.ChangeType == System.IO.WatcherChangeTypes.Deleted)
            {
                if(mLogFileMaps.ContainsKey(e.FullPath))
                {
                    mLogFileMaps.Remove(e.FullPath);
                }
            }
            else 
            {
                LoggerService.Service.Info("DataFileMananger", "LogFile "+ e.Name + " add to FileCach！");
                ParseLogFile(e.FullPath);
            }
        }

        private void HisDataWatcher_Changed(object sender, System.IO.FileSystemEventArgs e)
        {
            if(e.ChangeType == System.IO.WatcherChangeTypes.Created)
            {
                lock (mLocker)
                {
                    LoggerService.Service.Info("DataFileMananger", "HisDataFile " + e.Name + " is Created & will be add to dataFileCach！");
                    var vifno = new System.IO.FileInfo(e.FullPath);
                    if (vifno.Extension == DataFileExtends)
                    {
                        ParseFileName(vifno);
                    }
                }
            }
            else if(e.ChangeType == System.IO.WatcherChangeTypes.Changed)
            {
                LoggerService.Service.Info("DataFileMananger", "HisDataFile " + e.Name + " is changed & will be processed！");
                var vtmp = new System.IO.FileInfo(e.FullPath);
                lock (mLocker)
                {
                   
                    if (vtmp.Extension == DataFileExtends)
                    {
                        var vfile = CheckAndGetDataFile(e.Name);
                        if (vfile != null)
                        {
                            vfile.UpdateLastDatetime();
                        }
                        else
                        {
                            ParseFileName(vtmp);
                        }
                    }
                }
            }
        }

        public async Task ScanLogFile(string path)
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
                foreach (var vv in dir.GetDirectories())
                {
                    await ScanLogFile(vv.FullName);
                }
            }
        }

        /// <summary>
        /// 搜索文件
        /// </summary>
        /// <param name="path"></param>
        public async Task Scan(string path)
        {
            System.IO.DirectoryInfo dir = new System.IO.DirectoryInfo(path);
            if (dir.Exists)
            {
                foreach (var vv in dir.GetFiles())
                {
                    if (vv.Extension == DataFileExtends)
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
                mLogFileMaps.Add(sfileName, new LogFileInfo() { FileName = sfileName, StartTime = dt, EndTime = dt.AddSeconds(timelen) });
            }
        }

        private DataFileInfo CheckAndGetDataFile(string file)
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
                if (mTimeFileMaps.ContainsKey(id))
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
        private void ParseFileName(System.IO.FileInfo file)
        {
            string sname = file.Name.Replace(DataFileExtends, "");
            string stime = sname.Substring(sname.Length - 12, 12);
            int yy=0, mm=0, dd=0;

            int id = -1;
            int.TryParse(sname.Substring(sname.Length - 15, 3), out id);

            if (id == -1)
                return;

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
            yt.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new DataFileInfo() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName,FId= mDatabaseName + id });
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
            string sname = mDatabaseName + id;
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
        public DataFileInfo GetDataFile(DateTime time,int Id)
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
        public List<DataFileInfo> GetDataFiles(DateTime starttime, DateTime endtime,out Tuple<DateTime,DateTime> logFileTimes, int Id)
        {
            string sid = mDatabaseName + Id;
            if (CurrentDateTime.ContainsKey(sid))
            {
                if (starttime > CurrentDateTime[sid])
                {
                    logFileTimes = new Tuple<DateTime, DateTime>(starttime, endtime);
                    return new List<DataFileInfo>();
                }
                else if (endtime <= CurrentDateTime[sid])
                {
                    logFileTimes = new Tuple<DateTime, DateTime>(DateTime.MinValue, DateTime.MinValue);
                    return GetDataFiles(starttime, endtime - starttime, Id);
                }
                else
                {
                    logFileTimes = new Tuple<DateTime, DateTime>(CurrentDateTime[sid], endtime);
                    return GetDataFiles(starttime, CurrentDateTime[sid] - starttime, Id);
                }
            }
            else
            {
                logFileTimes = new Tuple<DateTime, DateTime>(DateTime.MinValue, DateTime.MinValue);
                return GetDataFiles(starttime, endtime - starttime, Id);
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="span"></param>
        /// <param name="Id"></param>
        /// <returns></returns>
        public List<DataFileInfo> GetDataFiles(DateTime startTime, TimeSpan span, int Id)
        {
            List<DataFileInfo> re = new List<DataFileInfo>();
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
        public SortedDictionary<DateTime, DataFileInfo> GetDataFiles(List<DateTime> times, List<DateTime> logFileTimes,int Id)
        {
            SortedDictionary<DateTime, DataFileInfo> re = new SortedDictionary<DateTime, DataFileInfo>();
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
