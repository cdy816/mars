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
    public class HourTimeFile : TimeFileBase
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
        public DayTimeFile Parent { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="month"></param>
        /// <param name="file"></param>
        public MinuteTimeFile AddMinute(int minute, MinuteTimeFile file)
        {
            file.Parent = this;
            return this.AddTimefile(minute, file) as MinuteTimeFile;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="month"></param>
        /// <returns></returns>
        public MinuteTimeFile AddMinute(int minute)
        {
            MinuteTimeFile mfile = new MinuteTimeFile() { TimeKey = minute };
            return AddMinute(minute, mfile);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="from"></param>
        /// <param name="to"></param>
        public List<MinuteTimeFile> AddMinutes(int from=0,int to=59)
        {
            List<MinuteTimeFile> re = new List<MinuteTimeFile>();
            for(int i=from;i<=to;i++)
            {
               re.Add(AddMinute(i));
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
            if (this.ContainsKey(dateTime.Minute))
            {
                return this[dateTime.Minute].GetFile(dateTime);
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
