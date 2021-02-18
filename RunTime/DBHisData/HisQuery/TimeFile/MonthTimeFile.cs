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
        public void AddFile(DateTime startTime,TimeSpan duration, DataFileInfo4 file)
        {
            lock (this)
            {
                if (!mFileMaps.ContainsKey(startTime))
                {
                    var vdd = new DayFileItem() { Duration = duration, Time = startTime };
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
                }
                else
                {
                    var vv = mFileMaps[startTime];
                    vv.Duration = duration;
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
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public DataFileInfo4 GetDataFile(DateTime dateTime)
        {
            lock (this)
            {
                foreach (var vv in mFileMaps)
                {
                    if (vv.Key <= dateTime)
                    {
                        if (vv.Value.File1 != null && dateTime < (vv.Key + vv.Value.Duration))
                            return vv.Value.File1;
                        else if (vv.Value.File2 != null && dateTime < (vv.Key + vv.Value.Duration))
                            return vv.Value.File2;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public List<DataFileInfo4> GetDataFiles(DateTime startTime,DateTime endTime)
        {
            lock (this)
            {
                List<DataFileInfo4> infos = new List<DataFileInfo4>();

                DateTime stime = startTime;
                foreach (var vv in mFileMaps)
                {
                    if ((startTime >= vv.Key && startTime < vv.Key + vv.Value.Duration) || (endTime >= vv.Key && endTime < vv.Key + vv.Value.Duration) || (vv.Key >= startTime && (vv.Key + vv.Value.Duration) <= endTime))
                    {
                        infos.Add(vv.Value.File1 != null ? vv.Value.File1 : vv.Value.File2);
                    }
                }
                return infos;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="span"></param>
        /// <returns></returns>
        public List<DataFileInfo4> GetDataFiles(DateTime startTime,TimeSpan span)
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
        public DataFileInfo4 File1 { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DataFileInfo4 File2 { get; set; }
    }

}
