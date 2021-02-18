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

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="duration"></param>
        /// <param name="file"></param>
        public void AddFile(DateTime startTime,TimeSpan duration,DataFileInfo4 file)
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
        public DataFileInfo4 GetDataFile(DateTime dateTime)
        {
            if(this.ContainsKey(dateTime.Month))
            {
                return (this[dateTime.Month] as MonthTimeFile).GetDataFile(dateTime);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="span"></param>
        /// <returns></returns>
        public List<DataFileInfo4> GetDataFiles(DateTime startTime,TimeSpan span)
        {
            List<DataFileInfo4> re = new List<DataFileInfo4>();
            var nxtMonth = startTime.AddMonths(1);
            nxtMonth = new DateTime(nxtMonth.Year, nxtMonth.Month,1);
            if(nxtMonth>startTime+span)
            {
                int mon = startTime.Month;
                if(this.ContainsKey(mon))
                {
                    re.AddRange((this[mon] as MonthTimeFile).GetDataFiles(startTime, span));
                }
            }
            else
            {
                int mon = startTime.Month;
                if (this.ContainsKey(mon))
                {
                    re.AddRange((this[mon] as MonthTimeFile).GetDataFiles(startTime, nxtMonth - startTime));
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
