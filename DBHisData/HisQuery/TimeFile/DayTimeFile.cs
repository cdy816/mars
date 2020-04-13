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
    public class DayTimeFile : TimeFileBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public MonthTimeFile Parent { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="month"></param>
        /// <param name="file"></param>
        public HourTimeFile AddHour(int hour, HourTimeFile file)
        {
            file.Parent = this;
            return this.AddTimefile(hour, file) as HourTimeFile;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="month"></param>
        /// <returns></returns>
        public HourTimeFile AddHour(int hour)
        {
            HourTimeFile mfile = new HourTimeFile() { TimeKey = hour };
            return AddHour(hour, mfile);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        /// <returns></returns>
        public List<HourTimeFile> AddHours(int from=0, int to=23)
        {
            List<HourTimeFile> re = new List<HourTimeFile>();
            for (int i = from; i <= to; i++)
            {
                re.Add(AddHour(i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public override MinuteTimeFile GetFile(DateTime dateTime)
        {
            if (this.ContainsKey(dateTime.Hour))
            {
                return this[dateTime.Hour].GetFile(dateTime);
            }
            else
            {
                return null;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
