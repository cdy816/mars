//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/2/16 12:55:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class StatisticsFileHelper
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static StatisticsFileHelper Helper = new StatisticsFileHelper();

        public const string DayStatisticsFileExtends = ".stad";

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public string Database { get; set; }

        public int TagCountOneFile { get; set; } = 100000;


        public DataFileManager Manager { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetPrimaryHisDataPath()
        {
            return Manager.GetPrimaryHisDataPath();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetBackUpHisDataPath()
        {
            return Manager.GetBackHisDataPath();
        }

        /// <summary>
        /// 获取统计文件名称
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        private string GetStatisticsFileName(int id,DateTime time)
        {
            return Database + (id/ TagCountOneFile).ToString("X3") + time.ToString("yyyyMMdd") + DayStatisticsFileExtends;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        public NumberStatisticsQueryResult  Read(int id,DateTime startTime,DateTime endTime)
        {
            NumberStatisticsQueryResult result;

            var valcount = (int)Math.Ceiling((endTime - startTime).TotalHours) + 1;
            result = new NumberStatisticsQueryResult(valcount);

            Dictionary<DateTime, List<DateTime>> mFileMap = new Dictionary<DateTime, List<DateTime>>();

            DateTime stime = new DateTime(startTime.Year, startTime.Month, startTime.Day, startTime.Hour, 0, 0);
            while(stime<=endTime)
            {
                var vdd = stime.Date;
                if(!mFileMap.ContainsKey(vdd))
                {
                    mFileMap.Add(vdd, new List<DateTime>() { stime });
                }
                else
                {
                    mFileMap[vdd].Add(stime);
                }
                stime = stime.AddHours(1);
            }

            if(stime>endTime)
            {
                stime = new DateTime(stime.Year, stime.Month, stime.Day, stime.Hour, 0, 0);
                var etime = new DateTime(endTime.Year, endTime.Month, endTime.Day, endTime.Hour, 0, 0);
                if(stime == endTime)
                {
                    var vdd = stime.Date;
                    if (!mFileMap.ContainsKey(vdd))
                    {
                        mFileMap.Add(vdd, new List<DateTime>() { stime });
                    }
                    else
                    {
                        mFileMap[vdd].Add(stime);
                    }
                }
            }

            foreach(var vv in mFileMap)
            {
                Read(id, vv.Key, vv.Value,result);
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult Read(int id, IEnumerable<DateTime> times)
        {
            NumberStatisticsQueryResult result;

            var valcount = times.Count();
            result = new NumberStatisticsQueryResult(valcount);

            Dictionary<DateTime, List<DateTime>> mFileMap = new Dictionary<DateTime, List<DateTime>>();

            foreach(var stime in times)
            {
                var vdd = stime.Date;
                if (!mFileMap.ContainsKey(vdd))
                {
                    mFileMap.Add(vdd, new List<DateTime>() { stime });
                }
                else
                {
                    mFileMap[vdd].Add(stime);
                }
            }

            foreach (var vv in mFileMap)
            {
                Read(id, vv.Key, vv.Value, result);
            }

            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="times"></param>
        /// <param name="result"></param>
        private void Read(int id,DateTime time,List<DateTime> times,NumberStatisticsQueryResult result)
        {
            string sfile = GetStatisticsFileName(id, time);
            string ss = System.IO.Path.Combine(GetPrimaryHisDataPath(), sfile);
            if(System.IO.File.Exists(ss))
            {
                Read(ss, id, times, result);
            }
            else
            {
                ss = System.IO.Path.Combine(GetBackUpHisDataPath(), sfile);
                if(System.IO.File.Exists(ss))
                {
                    Read(ss, id, times, result);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="result"></param>
        private void Read(string file, int id, List<DateTime> times, NumberStatisticsQueryResult result)
        {
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenForReadOnly(file);
                int icount = id % TagCountOneFile;
                long laddr = ss.ReadLong(icount * 8 + 72+4);

                laddr += TagCountOneFile * 8 + 8 + 72;

                var res =  ss.Read(laddr, 48 * 24);

                foreach(var vv in times)
                {
                    long lltmp = vv.Hour * 48;
                    
                    
                    var cmd = res.ReadByte(lltmp);

                    var avgvalue = res.ReadDouble(lltmp + 8);
                    var maxtime = res.ReadDateTime();
                    var maxvalue = res.ReadDouble();
                    var mintime = res.ReadDateTime();
                    var minvalue = res.ReadDouble();

                    if(cmd>0)
                    result.AddValue(vv, avgvalue, maxvalue, maxtime, minvalue, mintime);
                }

            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
