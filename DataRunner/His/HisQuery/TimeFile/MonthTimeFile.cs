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
        /// <param name="month"></param>
        /// <param name="file"></param>
        public DayTimeFile AddDay(int month, DayTimeFile file)
        {
            file.Parent = this;
            return this.AddTimefile(month, file) as DayTimeFile;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="month"></param>
        /// <returns></returns>
        public DayTimeFile AddDay(int month)
        {
            DayTimeFile mfile = new DayTimeFile() { TimeKey = month };
            return AddDay(month, mfile);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public override MinuteTimeFile GetFile(DateTime dateTime)
        {
            if (this.ContainsKey(dateTime.Day))
            {
                return this[dateTime.Day].GetFile(dateTime);
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
