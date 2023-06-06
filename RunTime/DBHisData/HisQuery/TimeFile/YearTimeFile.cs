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
using System.Linq;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class YearTimeFile: TimeFileBase
    {

        #region ... Variables  ...

        private int mMaxMonth = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public string DataPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string BackPath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Database { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="duration"></param>
        /// <param name="file"></param>
        public void AddFile(DateTime startTime,TimeSpan duration,IDataFile file)
        {
            int mon1 = startTime.Month;
            var endTime = startTime.Add(duration);
            if (mon1 == endTime.Month)
            {
                var mm = AddMonth(mon1);
                mm.AddFile(startTime, duration, file);
            }
            else
            {
                var startTime2 = new DateTime(endTime.Year, endTime.Month, endTime.Day);

                var mm = AddMonth(mon1);
                mm.AddFile(startTime, startTime2-startTime, file);
                if ((endTime - startTime).Seconds <= 1)
                {
                    return;
                }
                else
                {
                    mm = AddMonth(endTime.Month);
                    mm.AddFile(startTime2, endTime - startTime2, file);
                }
            }
            mMaxMonth = Math.Max(mon1, mMaxMonth);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void CheckFileExist(string file, DateTime startTime)
        {
            if(this.ContainsKey(startTime.Month))
            {
                (this[startTime.Month] as MonthTimeFile).CheckFileExist(file, startTime);

                CheckAndLoad(startTime);
                if (this.ContainsKey(startTime.Month))
                {
                    (this[startTime.Month] as MonthTimeFile).CheckFileExist(file, startTime);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void UpdateLastDatetime()
        {
           if(this.ContainsKey(mMaxMonth))
            {
                (this[mMaxMonth] as MonthTimeFile).UpdateLastDatetime();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="month"></param>
        /// <param name="file"></param>
        public MonthTimeFile AddMonth(int month, MonthTimeFile file)
        {
            if (this.ContainsKey(month))
            {
                return this[month] as MonthTimeFile;
            }
            else
            {
                file.Parent = this;
                return this.AddTimefile(month, file) as MonthTimeFile;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="month"></param>
        /// <returns></returns>
        public MonthTimeFile AddMonth(int month)
        {
            MonthTimeFile mfile = new MonthTimeFile() { TimeKey = month };
            return AddMonth(month, mfile);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public IDataFile GetDataFile(DateTime dateTime)
        {
            if(this.ContainsKey(dateTime.Month))
            {
                
                return (this[dateTime.Month] as MonthTimeFile).GetDataFile(dateTime);
            }
            else
            {
                CheckAndLoad(dateTime);
                if (this.ContainsKey(dateTime.Month))
                {
                    return (this[dateTime.Month] as MonthTimeFile).GetDataFile(dateTime);
                }
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public void FillDataFile(Dictionary<int,RecordList<DateTime>> dateTimes, SortedDictionary<DateTime, IDataFile> dic, List<DateTime> logFileTimes)
        {
            foreach(var vtime in dateTimes)
            {
                if (this.ContainsKey(vtime.Key))
                {
                    (this[vtime.Key] as MonthTimeFile).FillDataFile(vtime.Value,dic, logFileTimes);
                }
                else
                {
                    CheckAndLoad(vtime.Value.First());
                    if (this.ContainsKey(vtime.Key))
                    {
                        (this[vtime.Key] as MonthTimeFile).FillDataFile(vtime.Value, dic, logFileTimes);
                    }
                    else
                    {
                        logFileTimes.AddRange(vtime.Value);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public void FillDataFile(Dictionary<int, RecordList<DateTime>> dateTimes, SortedDictionary<DateTime, IDataFile> dic, ArrayList<DateTime> logFileTimes)
        {
            foreach (var vtime in dateTimes)
            {
                if (this.ContainsKey(vtime.Key))
                {
                    (this[vtime.Key] as MonthTimeFile).FillDataFile(vtime.Value, dic, logFileTimes);
                }
                else
                {
                    CheckAndLoad(vtime.Value.First());
                    if (this.ContainsKey(vtime.Key))
                    {
                        (this[vtime.Key] as MonthTimeFile).FillDataFile(vtime.Value, dic, logFileTimes);
                    }
                    else
                    {
                        logFileTimes.AddRange(vtime.Value);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        public void ClearCach(DateTime time)
        {
            if (this.ContainsKey(time.Month))
            {
                (this[time.Month] as MonthTimeFile).ClearCachedTimes();
            }
        }

        private void CheckAndLoad(DateTime time)
        {
            string spath = System.IO.Path.Combine(this.DataPath, time.Year.ToString(), time.Month.ToString());
            if(System.IO.Directory.Exists(spath))
            {
                AddMonth(time.Month);
            }

            if (!string.IsNullOrEmpty(this.BackPath))
            {
                spath = System.IO.Path.Combine(this.BackPath, time.Year.ToString(), time.Month.ToString());
                if (System.IO.Directory.Exists(spath))
                {
                    AddMonth(time.Month);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="span"></param>
        /// <returns></returns>
        public List<IDataFile> GetDataFiles(DateTime startTime,TimeSpan span)
        {
            List<IDataFile> re = new List<IDataFile>();
            var nxtMonth = startTime.AddMonths(1);
            nxtMonth = new DateTime(nxtMonth.Year, nxtMonth.Month,1);
            if(nxtMonth>startTime+span)
            {
                int mon = startTime.Month;
                if(this.ContainsKey(mon))
                {
                    re.AddRange((this[mon] as MonthTimeFile).GetDataFiles(startTime, span));
                }
                else
                {
                    CheckAndLoad(startTime);
                    if (this.ContainsKey(mon))
                    {
                        re.AddRange((this[mon] as MonthTimeFile).GetDataFiles(startTime, span));
                    }
                }
            }
            else
            {
                int mon = startTime.Month;
                if (this.ContainsKey(mon))
                {
                    re.AddRange((this[mon] as MonthTimeFile).GetDataFiles(startTime, nxtMonth - startTime));
                }
                else
                {
                    CheckAndLoad(startTime);
                    if (this.ContainsKey(mon))
                    {
                        re.AddRange((this[mon] as MonthTimeFile).GetDataFiles(startTime, nxtMonth - startTime));
                    }
                }
                re.AddRange(GetDataFiles(nxtMonth, startTime+span - nxtMonth));
            }
            return re;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
