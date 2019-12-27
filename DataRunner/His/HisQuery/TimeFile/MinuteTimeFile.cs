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
using System.Linq;

namespace Cdy.Tag
{
    public class MinuteTimeFile : TimeFileBase
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private string mDataFile = string.Empty;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, long> mSecondOffset = new Dictionary<int, long>();

        private List<int> mOrderSecondOffset = new List<int>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public HourTimeFile Parent { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string DataFile
        {
            get
            {
                return mDataFile;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int, long> SecondOffset
        {
            get
            {
                return mSecondOffset;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public void GetTimeSpan(out DateTime start,out DateTime end)
        {
            int mm = this.TimeKey;
            int hh = Parent.TimeKey;
            int dd = Parent.Parent.TimeKey;
            int MM = Parent.Parent.Parent.TimeKey;
            int yy = Parent.Parent.Parent.Parent.TimeKey;
            start = new DateTime(yy, MM, dd, hh, mm, 0);
            end = new DateTime(yy, MM, dd, hh, mm, 59);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="minTime"></param>
        /// <param name="maxTime"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        public void GetTimeSpan(DateTime minTime,DateTime maxTime,out DateTime start,out DateTime end )
        {
            DateTime sstart, send;
            GetTimeSpan(out sstart, out send);
            start = minTime > sstart ? minTime : sstart;
            end = maxTime > send ? send : maxTime;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool Contains(DateTime time)
        {
            DateTime start, end;
            GetTimeSpan(out start, out end);
            return time >= start && time < end.AddSeconds(1);
        }

        /// <summary>
        /// 
        /// </summary>
        private void Scan()
        {
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenFile(mDataFile);
                long offset = 0;
                DateTime time;
                do
                {
                    time = ss.ReadDateTime(16);
                    mSecondOffset.Add(time.Second, offset);
                    offset = ss.ReadLong(offset + 8);
                }
                while (offset != 0);
            }
            mOrderSecondOffset = mSecondOffset.Keys.OrderBy(e=>e).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        public void AddFile(string sfile)
        {
            mDataFile = sfile;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="dateTime"></param>
        /// <returns></returns>
        public override MinuteTimeFile GetFile(DateTime dateTime)
        {
            return this;
        }

        /// <summary>
        /// 获取某个时间的数据在数据文件中偏移起始地址
        /// </summary>
        /// <param name="second"></param>
        /// <returns>-1表示未找到</returns>
        public long GetOffset(int second)
        {
            long re = -1;
            for (int i = 0; i < mOrderSecondOffset.Count; i++)
            {
                int itmp = mOrderSecondOffset[i];
                if (second < itmp)
                {
                    return re;
                }
                else if (second == itmp)
                {
                    re = mSecondOffset[mOrderSecondOffset[i]];
                    break;
                }
                else
                {
                    re = mSecondOffset[mOrderSecondOffset[i]];
                }
            }
            return re;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

}
