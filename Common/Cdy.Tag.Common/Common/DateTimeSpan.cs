//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/10/12 10:06:58.
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
    public class DateTimeSpan
    {
        /// <summary>
        /// 
        /// </summary>
        public static DateTimeSpan Empty = new DateTimeSpan() { Start = DateTime.MinValue, End = DateTime.MinValue };

        /// <summary>
        /// 
        /// </summary>
        public DateTime Start { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime End { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsEmpty()
        {
            return Start == DateTime.MinValue && End == DateTime.MinValue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsZore()
        {
            return (End - Start).TotalSeconds == 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        public DateTimeSpan Cross(DateTimeSpan target)
        {
            DateTime stime = Max(target.Start, this.Start);
            DateTime etime = Min(target.End, this.End);
            if (etime < stime)
            {
                return Empty;
            }
            else
            {
                return new DateTimeSpan() { Start = stime, End = etime };
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool Contains(DateTime time)
        {
            return time >= Start & time < End;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time1"></param>
        /// <param name="time2"></param>
        /// <returns></returns>
        public DateTime Min(DateTime time1, DateTime time2)
        {
            return time1 <= time2 ? time1 : time2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time1"></param>
        /// <param name="time2"></param>
        /// <returns></returns>
        public DateTime Max(DateTime time1, DateTime time2)
        {
            return time1 >= time2 ? time1 : time2;
        }

    }
}
