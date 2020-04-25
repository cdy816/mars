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

        private string mDatabaseName;

        /// <summary>
        /// 数据文件扩展名
        /// </summary>
        public const string DataFileExtends = ".dbd";

        public const int FileHeadSize = 72;

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
        private string GetBackHisDataPath()
        {
            return string.IsNullOrEmpty(BackHisDataPath) ? PathHelper.helper.GetDataPath(this.mDatabaseName, "HisData") : System.IO.Path.IsPathRooted(BackHisDataPath) ? BackHisDataPath : PathHelper.helper.GetDataPath(this.mDatabaseName, BackHisDataPath);
        }


        /// <summary>
        /// 
        /// </summary>
        public async Task Int()
        {
           await Scan(GetPrimaryHisDataPath());
           await Scan(GetBackHisDataPath());
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
            yt.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new DataFileInfo() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName } );

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

            if (mTimeFileMaps.ContainsKey(id) && mTimeFileMaps[id].ContainsKey(time.Year))
            {
                return mTimeFileMaps[id][time.Year].GetDataFile(time);
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
        public List<DataFileInfo> GetDataFiles(DateTime starttime, DateTime endtime, int Id)
        {
            return GetDataFiles(starttime,endtime-starttime,Id);
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
        public SortedDictionary<DateTime, DataFileInfo> GetDataFiles(List<DateTime> times, int Id)
        {
            SortedDictionary<DateTime, DataFileInfo> re = new SortedDictionary<DateTime, DataFileInfo>();
            foreach(var vv in times)
            {
                re.Add(vv, GetDataFile(vv, Id));
            }
            return re;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="time"></param>
        ///// <returns></returns>
        //public MinuteTimeFile GetFile(DateTime time,int Id)
        //{
        //    int id = Id / TagCountOneFile;

        //    if (mTimeFileMaps.ContainsKey(id) && mTimeFileMaps[id].ContainsKey(time.Year))
        //    {
        //        return mTimeFileMaps[id][time.Year].GetFile(time);
        //    }
        //    return null;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="starttime"></param>
        ///// <param name="endtime"></param>
        ///// <returns></returns>
        //public List<MinuteTimeFile> GetFiles(DateTime starttime,DateTime endtime,int Id)
        //{
        //    List<MinuteTimeFile> re = new List<MinuteTimeFile>();
        //    DateTime sstart = starttime;
        //    while (sstart <= endtime)
        //    {
        //        var sfile = GetFile(sstart, Id);
        //        if (sfile != null)
        //            re.Add(sfile);
        //        sstart = sstart.AddMinutes(1);
        //    }
        //    return re;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="times"></param>
        ///// <returns></returns>
        //public Dictionary<DateTime, MinuteTimeFile> GetFiles(List<DateTime> times,int Id)
        //{
        //    Dictionary<DateTime,MinuteTimeFile> re = new Dictionary<DateTime, MinuteTimeFile>();
        //    foreach(var vv in times)
        //    {
        //        re.Add(vv,GetFile(vv,Id));
        //    }
        //    return re;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="datafile"></param>
        ///// <returns></returns>
        //private DataFileSeriserbase GetFileSerise(string datafile)
        //{
        //    return null;
        //}

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
