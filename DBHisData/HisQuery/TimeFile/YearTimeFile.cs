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
        /// <param name="month"></param>
        /// <param name="file"></param>
        public MonthTimeFile AddMonth(int month, MonthTimeFile file)
        {
            file.Parent = this;
            return this.AddTimefile(month, file) as MonthTimeFile;
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
        public override MinuteTimeFile GetFile(DateTime dateTime)
        {
            if (this.ContainsKey(dateTime.Month))
            {
                return this[dateTime.Month].GetFile(dateTime);
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
