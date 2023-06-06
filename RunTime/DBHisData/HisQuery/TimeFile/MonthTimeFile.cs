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
using System.Net.Http.Headers;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class MonthTimeFile : TimeFileBase
    {

        #region ... Variables  ...

        private SortedDictionary<DateTime, DayFileItem> mFileMaps = new SortedDictionary<DateTime, DayFileItem>();

        private DateTime mMaxTime = DateTime.MinValue;

        private DateTime mMinScanTime = DateTime.MaxValue;

        private DateTime mMaxScanTime = DateTime.MinValue;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public YearTimeFile Parent { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void UpdateLastDatetime()
        {
            lock (this)
            {
                if (mFileMaps.ContainsKey(mMaxTime))
                {
                    mFileMaps[mMaxTime].File1?.UpdateLastDatetime();
                    mFileMaps[mMaxTime].File2?.UpdateLastDatetime();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="startTime"></param>
        public void CheckFileExist(string file, DateTime startTime)
        {
            lock (this)
            {
                if (mFileMaps.ContainsKey(startTime))
                {
                    if (mFileMaps[startTime].File1?.FileName == file)
                    {
                        if (!System.IO.File.Exists(file))
                        {
                            var vv = mFileMaps[startTime];
                            vv.File1 = null;
                            mFileMaps[startTime] = vv;
                        }
                    }
                    else if (mFileMaps[startTime].File2?.FileName == file)
                    {
                        if (!System.IO.File.Exists(file))
                        {
                            var vv = mFileMaps[startTime];
                            vv.File2 = null;
                            mFileMaps[startTime] = vv;
                        }
                    }
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="duration"></param>
        /// <param name="file"></param>
        public void AddFile(DateTime startTime,TimeSpan duration, IDataFile file)
        {
            lock (this)
            {
                if (!mFileMaps.ContainsKey(startTime))
                {
                    var vdd = new DayFileItem() { Duration = duration, Time = startTime,EndTime=startTime+duration };
                    if (file is HisDataFileInfo4)
                    {
                        vdd.File1 = file;
                    }
                    else
                    {
                        vdd.File2 = file;
                    }

                    mFileMaps.Add(startTime, vdd);

                    if (startTime > mMaxTime)
                    {
                        mMaxTime = startTime;
                    }

                    var vdata = startTime.Date;

                    if(vdata < mMinScanTime)
                    {
                        mMinScanTime = vdata;
                    }

                    if(vdata > mMaxScanTime)
                    {
                        mMaxScanTime = vdata;
                    }

                }
                else
                {
                    var vv = mFileMaps[startTime];
                    vv.Duration = duration;
                    vv.EndTime = vv.Time+ duration;
                    if (file is HisDataFileInfo4)
                    {
                        vv.File1 = file;
                    }
                    else
                    {
                        vv.File2 = file;
                    }
                    mFileMaps[startTime] = vv;

                    if (startTime > mMaxTime)
                    {
                        mMaxTime = startTime;
                    }

                    var vdata = startTime.Date;

                    if (vdata < mMinScanTime)
                    {
                        mMinScanTime = vdata;
                    }

                    if (vdata > mMaxScanTime)
                    {
                        mMaxScanTime = vdata;
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public IDataFile GetDataFile(DateTime dateTime)
        {
            IDataFile re = null;
            lock (this)
            {
               
                foreach (var vv in mFileMaps)
                {
                    //if (vv.Key <= dateTime && dateTime < (vv.Key + vv.Value.Duration))
                    if (vv.Key <= dateTime && dateTime < vv.Value.EndTime)
                    {
                        re = vv.Value.File1 != null ? vv.Value.File1 : (vv.Value.File2);
                    }
                }
                //如果临时加载一个文件，则再重新判断一遍
                if (re == null && CheckAndLoad(dateTime))
                {
                    foreach (var vv in mFileMaps)
                    {
                        //if (vv.Key <= dateTime && dateTime < (vv.Key + vv.Value.Duration))
                        if (vv.Key <= dateTime && dateTime < vv.Value.EndTime)
                        {
                            return vv.Value.File1 != null ? vv.Value.File1 : (vv.Value.File2);
                        }
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public void FillDataFile(RecordList<DateTime> dateTimes, SortedDictionary<DateTime, IDataFile> dic, List<DateTime> logFileTimes)
        {
            IDataFile re = null;
            lock (this)
            {
                DateTime dat=DateTime.MinValue;
                //加载还未扫描加载的内容
                foreach(var vv in dateTimes.Select(e=>e.Date))
                {
                    if(dat!=vv)
                    {
                        CheckAndLoad(vv);
                        dat = vv;
                    }
                }

                var vfdate = mFileMaps.First().Key;
                foreach(var vv in dateTimes)
                {
                    var val = dateTimes.PeekGet();
                    if(val<vfdate)
                    {
                        logFileTimes.Add(val);
                        dateTimes.Next();
                    }
                    else
                    {
                        break;
                    }
                }

                ////bool needbreak = false;
                foreach (var vv in mFileMaps)
                {
                    while (dateTimes.CanGet())
                    {
                        var val = dateTimes.PeekGet();
                        if(val<vv.Value.Time)
                        {
                            logFileTimes.Add(val);
                            dateTimes.Next();
                        }
                        else if (vv.Value.Time <= val && val < vv.Value.EndTime)
                        {
                            re = vv.Value.File1 != null ? vv.Value.File1 : (vv.Value.File2);
                            dic.Add(val, re);
                            dateTimes.Next();
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (!dateTimes.CanGet())
                    {
                        return;
                    }
                    //if (needbreak) break;
                }

                while (dateTimes.CanGet())
                {
                    var vtime = dateTimes.PeekGet();
                    if (vtime.Month == TimeKey)
                    {
                        logFileTimes.Add(vtime);
                        dateTimes.Next();
                    }
                }

                ////延迟加载处理,
                //while(dateTimes.CanGet())
                //{
                //    var vtime = dateTimes.PeekGet();
                //    if(vtime.Month== TimeKey)
                //    {
                //        if (CheckAndLoad(vtime))
                //        {
                //            FillDataFile(dateTimes, dic, logFileTimes);
                //        }
                //        else
                //        {
                //            logFileTimes.Add(vtime);
                //            dateTimes.Next();
                //        }
                //    }
                //    else
                //    {
                //        break;
                //    }
                //}
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public void FillDataFile(RecordList<DateTime> dateTimes, SortedDictionary<DateTime, IDataFile> dic, ArrayList<DateTime> logFileTimes)
        {
            IDataFile re = null;
            lock (this)
            {
                var vfdate = mFileMaps.First().Key;
                foreach (var vv in dateTimes)
                {
                    var val = dateTimes.PeekGet();
                    if (val < vfdate)
                    {
                        logFileTimes.Add(val);
                        dateTimes.Next();
                    }
                    else
                    {
                        break;
                    }
                }

                foreach (var vv in mFileMaps)
                {
                    while (dateTimes.CanGet())
                    {
                        var val = dateTimes.PeekGet();
                        if (vv.Value.Time <= val && val < vv.Value.EndTime)
                        {
                            re = vv.Value.File1 != null ? vv.Value.File1 : (vv.Value.File2);
                            dic.Add(val, re);
                            dateTimes.Next();
                        }
                        else
                        {
                            break;
                        }
                    }
                    if (!dateTimes.CanGet())
                    {
                        return;
                    }
                }

                //延迟加载处理,
                while (dateTimes.CanGet())
                {
                    var vtime = dateTimes.PeekGet();
                    if (vtime.Month == TimeKey)
                    {
                        if (CheckAndLoad(vtime))
                        {
                            FillDataFile(dateTimes, dic, logFileTimes);
                        }
                        else
                        {
                            logFileTimes.Add(vtime);
                            dateTimes.Next();
                        }
                    }
                    else
                    {
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public List<IDataFile> GetDataFiles(DateTime startTime,DateTime endTime)
        {
            lock (this)
            {
                List<IDataFile> infos = new List<IDataFile>();

                //延迟加载历史数据
                if(startTime.Date<mMinScanTime || startTime.Date> mMaxScanTime || endTime.Date<mMinScanTime || endTime.Date> mMaxScanTime)
                {
                    DateTime st = startTime.Date;
                    while(st<= endTime)
                    {
                        CheckAndLoad(st);
                        st = st.AddDays(1);
                    }
                }

                DateTime stime = startTime;
                foreach (var vv in mFileMaps)
                {
                    if ((startTime >= vv.Key && startTime < vv.Key + vv.Value.Duration) || (endTime >= vv.Key && endTime < vv.Key + vv.Value.Duration) || (vv.Key >= startTime && (vv.Key + vv.Value.Duration) <= endTime))
                    {
                        infos.Add(vv.Value.File2 != null ? vv.Value.File2 : vv.Value.File1);
                    }
                }
                return infos;
            }
        }

        private List<DateTime> mCachedTimes = new List<DateTime>();

        /// <summary>
        /// 
        /// </summary>
        public void ClearCachedTimes()
        {
            mCachedTimes.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private bool CheckAndLoad(DateTime time)
        {
            if(mCachedTimes.Contains(time.Date))
            {
                return false;
            }

            string spath = System.IO.Path.Combine(this.Parent.DataPath, time.Year.ToString(), time.Month.ToString(),time.Day.ToString());
            if (System.IO.Directory.Exists(spath))
            {
                ScanFile(spath);
            }
            mCachedTimes.Add(time.Date);

            if (!string.IsNullOrEmpty(this.Parent.BackPath))
            {
                spath = System.IO.Path.Combine(this.Parent.BackPath, time.Year.ToString(), time.Month.ToString(), time.Day.ToString());
                if (System.IO.Directory.Exists(spath))
                {
                    ScanFile(spath);
                }
            }

            if(time<mMinScanTime)
            {
                mMinScanTime = time;
            }
            if(time> mMaxScanTime)
            {
                mMaxScanTime = time;
            }

            return true;
        }


        private void ScanFile(string path)
        {
            System.IO.DirectoryInfo sdd = new System.IO.DirectoryInfo(path);
            if(sdd.Exists)
            {
                foreach(var vv in sdd.EnumerateFiles())
                {
                    if (vv.Extension == DataFileManager.DataFileExtends || vv.Extension == DataFileManager.HisDataFileExtends || vv.Extension == DataFileManager.ZipHisDataFileExtends || vv.Extension == DataFileManager.DataFile2Extends || vv.Extension == DataFileManager.ZipDataFile2Extends)
                    {
                        ParseFileName(vv);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fileName"></param>
        private void ParseFileName(System.IO.FileInfo file)
        {

            DateTime startTime = ParseFileToTime(file.Name, out int id, out int hhspan);

            if (file.Extension == DataFileManager.DataFileExtends)
            {
                Parent.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new DataFileInfo4() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName, FId = Parent.Database + id });
            }
            else if (file.Extension == DataFileManager.HisDataFileExtends || file.Extension == DataFileManager.ZipHisDataFileExtends)
            {
                Parent.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new HisDataFileInfo4() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName, FId = Parent.Database + id, IsZipFile = (file.Extension == DataFileManager.ZipHisDataFileExtends) });
            }
            else if (file.Extension == DataFileManager.DataFile2Extends || file.Extension == DataFileManager.ZipDataFile2Extends)
            {
                Parent.AddFile(startTime, new TimeSpan(hhspan, 0, 0), new DataFileInfo6() { Duration = new TimeSpan(hhspan, 0, 0), StartTime = startTime, FileName = file.FullName, FId = Parent.Database + id, IsZipFile = (file.Extension == DataFileManager.ZipDataFile2Extends) });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="id"></param>
        /// <param name="hhspan"></param>
        /// <returns></returns>
        private DateTime ParseFileToTime(string file, out int id, out int hhspan)
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


        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="span"></param>
        /// <returns></returns>
        public List<IDataFile> GetDataFiles(DateTime startTime,TimeSpan span)
        {
            return GetDataFiles(startTime, startTime + span);
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public struct DayFileItem
    {
        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public TimeSpan Duration { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public IDataFile File1 { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public IDataFile File2 { get; set; }
    }

}
