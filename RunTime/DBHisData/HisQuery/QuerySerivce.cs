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
using System.Diagnostics;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class QuerySerivce: IHisQuery
    {
        IHisQueryFromMemory mMemoryService;

        StatisticsFileHelper statisticsHelper;

        /// <summary>
        /// 
        /// </summary>
        public QuerySerivce()
        {
            mMemoryService = ServiceLocator.Locator.Resolve<IHisQueryFromMemory>() as IHisQueryFromMemory;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        public QuerySerivce(string databaseName) : this()
        {
            Database = databaseName;
            statisticsHelper = new StatisticsFileHelper() { Database = databaseName, Manager = GetFileManager() };
        }

        public string Database { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private DataFileManager GetFileManager()
        {
            return HisQueryManager.Instance.GetFileManager(Database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private bool IsCanQueryFromMemory()
        {
            return mMemoryService != null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            ReadValueByUTCTime<T>(id, times.Select(e => e.ToUniversalTime()), type, result);
            result.ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValueByUTCTime<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            List<DateTime> ltmp = new List<DateTime>();
            List<DateTime> mMemoryTimes = new List<DateTime>();
            //判断数据是否在内存中
            if (IsCanQueryFromMemory())
            {
                foreach(var vv in times)
                {
                    if(!mMemoryService.CheckTime(id,vv))
                    {
                        ltmp.Add(vv);
                    }
                    else
                    {
                        mMemoryTimes.Add(vv);
                    }
                }
            }
            else
            {
                ltmp.AddRange(times);
            }

            List<DateTime> mLogTimes = new List<DateTime>();
            var vfiles = GetFileManager().GetDataFiles(ltmp, mLogTimes, id);

            DataFileInfo4 mPreFile = null;

            List<DateTime> mtime = new List<DateTime>();

            //从历史文件中读取数据
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        if (mPreFile is HisDataFileInfo4)
                        {
                            (mPreFile as HisDataFileInfo4).Read(id, mtime, type, result);
                        }
                        else
                            mPreFile.Read<T>(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add(default(T), vv.Key, (byte)QualityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        if (mPreFile is HisDataFileInfo4) (mPreFile as HisDataFileInfo4).Read(id, mtime, type, result);
                        else
                            mPreFile.Read<T>(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
                else
                {
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                if (mPreFile is HisDataFileInfo4) (mPreFile as HisDataFileInfo4).Read(id, mtime, type, result);
                else
                    mPreFile.Read<T>(id, mtime, type, result);
            }

            if (IsCanQueryFromMemory())
            {
                //从内存中读取数据
                ReadFromMemory(id, mMemoryTimes, type, result);

                if(mLogTimes.Count>0)
                {
                    foreach(var vv in mLogTimes)
                    {
                        result.Add(default(T), vv, (byte)QualityConst.Null);
                    }
                }

            }
            else
            {
                //从日志文件中读取数据
                ReadLogFile(id, mLogTimes, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadFromMemory<T>(int id,List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            mMemoryService?.ReadValue(id, times, type, result);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="mLogTimes"></param>
        /// <param name="result"></param>
        private void ReadLogFile<T>(int id,List<DateTime> mLogTimes, QueryValueMatchType type, HisQueryResult<T> result)
        {
            if (mLogTimes.Count > 0)
            {
                List<DateTime> mtime = new List<DateTime>();
                var lfiles = GetFileManager().GetLogDataFiles(mLogTimes);

                LogFileInfo mPlFile = null;

                foreach (var vv in lfiles)
                {
                    if (vv.Value == null)
                    {
                        if (mPlFile != null)
                        {
                            mPlFile.Read<T>(id, mtime, type, result);
                            mPlFile = null;
                            mtime.Clear();
                        }
                        result.Add(default(T), vv.Key, (byte)QualityConst.Null);
                    }
                    else if (vv.Value != mPlFile)
                    {
                        if (mPlFile != null)
                        {
                            mPlFile.Read<T>(id, mtime, type, result);
                        }
                        mPlFile = vv.Value;
                        mtime.Clear();
                        mtime.Add(vv.Key);
                    }
                    else
                    {
                        mtime.Add(vv.Key);
                    }
                }
                if (mPlFile != null)
                {
                    mPlFile.Read<T>(id, mtime, type, result);
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadLogFileAllValue<T>(int id,DateTime startTime,DateTime endTime,HisQueryResult<T> result)
        {
            var vfiles = GetFileManager().GetLogDataFiles(startTime,endTime);
            vfiles.ForEach(e => {
                DateTime sstart = e.StartTime > startTime ? e.StartTime : startTime;
                DateTime eend = e.EndTime > endTime ? endTime : endTime;
                e.ReadAllValue<T>(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="starttime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadAllValueFromMemory<T>(int id,DateTime starttime,DateTime endTime,HisQueryResult<T> result)
        {
            mMemoryService?.ReadAllValue(id, starttime, endTime, result);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            ReadAllValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), result);
            result.ConvertUTCTimeToLocal();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            try
            {

                DateTime etime = endTime,stime = startTime;
                DateTime memoryTime = DateTime.MaxValue;
                if(IsCanQueryFromMemory())
                {
                    memoryTime = mMemoryService.GetStartMemoryTime(id);
                }

                if(startTime>=memoryTime)
                {
                    ReadAllValueFromMemory(id, startTime, endTime, result);
                }
                else
                {
                    var fileMananger = GetFileManager();

                    ////优先从日志中读取历史记录
                    //memoryTime = fileMananger.LastLogTime > memoryTime ? fileMananger.LastLogTime : memoryTime;

                    if (endTime>memoryTime)
                    {
                        etime = memoryTime;
                    }

                    Tuple<DateTime, DateTime> mLogFileTimes;
                    var vfiles = fileMananger.GetDataFiles(stime, etime, out mLogFileTimes, id);
                    //ltmp0 = sw.ElapsedMilliseconds;
                    //从历史记录中读取数据
                    foreach(var e in vfiles)
                    {
                        DateTime sstart = e.StartTime > startTime ? e.StartTime : startTime;
                        DateTime eend = e.EndTime > endTime ? endTime : e.EndTime;
                        if (e is HisDataFileInfo4)
                        {
                            (e as HisDataFileInfo4).ReadAllValue(id, startTime, endTime, result);
                        }
                        else { e.ReadAllValue(id, sstart, eend, result); }
                    }

                    //从日志文件中读取数据
                    if (mLogFileTimes.Item1 < mLogFileTimes.Item2)
                    {
                        ReadLogFileAllValue(id, mLogFileTimes.Item1, mLogFileTimes.Item2, result);
                    }

                    //从内存中读取数据
                    if(endTime>memoryTime)
                    {
                        ReadAllValueFromMemory(id, memoryTime, endTime, result);
                    }
                    //ltmp2 = sw.ElapsedMilliseconds;
                }

                //sw.Stop();

                //Debug.Print("ReadAllValueByUTCTime "+ ltmp0 +" , " +(ltmp1-ltmp0)+" , "+(ltmp2-ltmp1));
                
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("QueryService", ex.StackTrace);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadAllValue<T>(int id, DateTime startTime, DateTime endTime)
        {
            return ReadAllValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime()).ConvertUTCTimeToLocal();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadAllValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime)
        {
            int valueCount = (int)(endTime - startTime).TotalSeconds;
            var result = new HisQueryResult<T>(valueCount);
            ReadAllValueByUTCTime(id, startTime, endTime, result);
            return result as HisQueryResult<T>;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadValue<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type)
        {
            return ReadValueByUTCTime<T>(id, times.Select(e => e.ToUniversalTime()), type).ConvertUTCTimeToLocal();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> ReadValueByUTCTime<T>(int id, IEnumerable<DateTime> times, QueryValueMatchType type)
        {
            int valueCount = times.Count();

            var result = new HisQueryResult<T>(valueCount);
            ReadValueByUTCTime(id, times, type, result);
            return result; 
        }

        /// <summary>
        /// 读取某个时间段内，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatistics(int id, DateTime startTime, DateTime endTime)
        {
            return ReadNumberStatisticsByUTCTime(id, startTime.ToUniversalTime(), endTime.ToUniversalTime()).ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 读取某个时间段（UTC时间）内，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatisticsByUTCTime(int id, DateTime startTime, DateTime endTime)
        {
            return statisticsHelper.Read(id, startTime, endTime);
        }

        /// <summary>
        /// 读取指定时间点的，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatistics(int id, IEnumerable<DateTime> times)
        {
            return ReadNumberStatisticsByUTCTime(id, times.Select(e => e.ToUniversalTime())).ConvertUTCTimeToLocal();
        }

        /// <summary>
        /// 读取指定时间点（UTC时间）的，值类型变量的统计信息
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public NumberStatisticsQueryResult ReadNumberStatisticsByUTCTime(int id, IEnumerable<DateTime> times)
        {
            return statisticsHelper.Read(id, times);
        }



        #region Number Tag Value Statistics

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Tuple<DateTime,object> FindNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            return FindNumberTagValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para,para2, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Dictionary<DateTime,object> FindNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            return FindNumberTagValuesByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para,para2, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public double FindNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            return FindNumberTagValueDurationByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para,para2, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public double FindNumberTagMaxMinValue<T>(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, out IEnumerable<DateTime> time)
        {
            return FindNumberTagMaxMinValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), type,out time);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public double FindNumberTagAvgValue<T>(int id, DateTime startTime, DateTime endTime)
        {
            return FindNumberTagAvgValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime());
        }


        /// <summary>
        /// 查找数字型变量的的等于指定值的时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="para2"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Tuple<DateTime,object> FindNumberTagValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para,double para2,NumberStatisticsType type)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            double dmin = para - para2;
            double dmax = para + para2;

            for (int i = 0; i < vals.Count; i++)
            {
                var tmp = vals.GetValue(i, out DateTime time, out byte qu);
                var val = Convert.ToDouble(tmp);
                if (IsGoodQuality(qu))
                {
                    switch(type)
                    {
                        case NumberStatisticsType.EqualsValue:
                            if(val == para || (val >= dmin && val <= dmax))
                            {
                                return new Tuple<DateTime, object>(time,tmp);
                            }
                            break;
                        case NumberStatisticsType.GreatValue:
                            if (val > para)
                            {
                                return new Tuple<DateTime, object>(time, tmp);
                            }
                            break;
                        case NumberStatisticsType.LowValue:
                            if (val < para)
                            {
                                return new Tuple<DateTime, object>(time, tmp);
                            }
                            break;
                    }
                }
            }
            return new Tuple<DateTime, object>(DateTime.MinValue,default(T));
        }

        /// <summary>
        /// 查找数字型变量的的等于指定值的时间集合
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="type"></param>
        /// <returns></returns>

        public Dictionary<DateTime,object> FindNumberTagValuesByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            Dictionary<DateTime,object> re = new Dictionary<DateTime, object>();
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);

            double dmin = para - para2;
            double dmax = para + para2;

            for (int i = 0; i < vals.Count; i++)
            {
                T tmp = vals.GetValue(i, out DateTime time, out byte qu);
                var val = Convert.ToDouble(tmp);
                if (IsGoodQuality(qu))
                {
                    switch (type)
                    {
                        case NumberStatisticsType.EqualsValue:
                            if (val == para || (val>= dmin && val<=dmax))
                            {
                                re.Add(time,tmp);
                            }
                            break;
                        case NumberStatisticsType.GreatValue:
                            if (val > para)
                            {
                                re.Add(time,tmp);
                            }
                            break;
                        case NumberStatisticsType.LowValue:
                            if (val < para)
                            {
                                re.Add(time,tmp);
                            }
                            break;
                    }
                   
                }
            }
            return re;
        }

        /// <summary>
        /// 查找数字型变量的的等于指定值得保持时间
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <param name="para2"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public double FindNumberTagValueDurationByUTCTime<T>(int id, DateTime startTime, DateTime endTime, double para,double para2, NumberStatisticsType type)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            double tim = 0;
            bool ishase = false;
            DateTime statetime = DateTime.MinValue;
            double dmin = para - para2;
            double dmax = para + para2;
            for (int i = 0; i < vals.Count; i++)
            {
                var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));

                switch (type)
                {
                    case NumberStatisticsType.EqualsValue:
                        if (IsGoodQuality(qu) && (val == para|| (val >= dmin && val <= dmax)))
                        {
                            if (!ishase)
                            {
                                ishase = true;
                                statetime = time;
                            }
                        }
                        else
                        {
                            if (ishase)
                            {
                                ishase = false;
                                tim += (time - statetime).TotalSeconds;
                            }
                        }
                        break;
                    case NumberStatisticsType.GreatValue:
                        if (IsGoodQuality(qu) && val > para)
                        {
                            if (!ishase)
                            {
                                ishase = true;
                                statetime = time;
                            }
                        }
                        else
                        {
                            if (ishase)
                            {
                                ishase = false;
                                tim += (time - statetime).TotalSeconds;
                            }
                        }
                        break;
                    case NumberStatisticsType.LowValue:
                        if (IsGoodQuality(qu) && val < para)
                        {
                            if (!ishase)
                            {
                                ishase = true;
                                statetime = time;
                            }
                        }
                        else
                        {
                            if (ishase)
                            {
                                ishase = false;
                                tim += (time - statetime).TotalSeconds;
                            }
                        }
                        break;
                }

                
            }

            if (ishase)
            {
                vals.GetValue(vals.Count - 1, out DateTime time, out byte qua);
                tim += (time - statetime).TotalSeconds;
            }
            return tim;
        }

        /// <summary>
        /// 查找最大、最小值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public double FindNumberTagMaxMinValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime,NumberStatisticsType type,out IEnumerable<DateTime> time)
        {
            List<DateTime> ret = new List<DateTime>();
            var hh = (endTime - startTime).TotalHours;
            DateTime retime = DateTime.MinValue;
            if (hh > 2 && (type == NumberStatisticsType.Max || type == NumberStatisticsType.Min))
            {
                //从统计数据文件中读取值，加快统计数据的读取
                var etime1 = new DateTime(startTime.Year, startTime.Month, startTime.Day, startTime.Hour + 1, 0, 0);
                var etime2 = new DateTime(endTime.Year, endTime.Month, endTime.Day, endTime.Hour, 0, 0);
                var val1 = ReadAllValueByUTCTime<T>(id, startTime, etime1);
                var val3 = ReadAllValueByUTCTime<T>(id, etime2, endTime);

                var vvs = ReadNumberStatisticsByUTCTime(id, etime1, etime2);

                switch (type)
                {
                    case NumberStatisticsType.Max:
                        double re = StatisticsValue<T>(val1, type,out List<DateTime> time1);
                        ret = time1;
                        var mv2 = StatisticsValue<T>(val3, type,out List<DateTime> tim2);
                       
                        if(re == mv2)
                        {
                            ret.AddRange(tim2);
                        }
                        else
                        {
                            re = Math.Max(re, mv2);
                            if(re==mv2)
                            {
                                ret = tim2;
                            }
                        }

                        if (vvs != null)
                        {
                            foreach (var vv in vvs.ListAllValue())
                            {
                                if (re == vv.MaxValue)
                                {
                                    ret.Add(vv.MaxTime);
                                }
                                else
                                {
                                    re = Math.Max(re, vv.MaxValue);
                                    if (re == vv.MaxValue)
                                    {
                                        ret = new List<DateTime>() { vv.MaxTime };
                                    }
                                }
                            }
                        }
                        time = ret;
                        return re;
                    case NumberStatisticsType.Min:
                        re = StatisticsValue<T>(val1, type, out List<DateTime> time3);
                        ret = time3;
                        mv2 = StatisticsValue<T>(val3, type, out List<DateTime> tim4);

                        if (re == mv2)
                        {
                            ret.AddRange(tim4);
                        }
                        else
                        {
                            re = Math.Min(re, mv2);
                            if (re == mv2)
                            {
                                ret = tim4;
                            }
                        }

                        if (vvs != null)
                        {
                            foreach (var vv in vvs.ListAllValue())
                            {
                                if (re == vv.MinValue)
                                {
                                    ret.Add(vv.MinTime);
                                }
                                else
                                {
                                    re = Math.Min(re, vv.MinValue);
                                    if (re == vv.MinValue)
                                    {
                                        ret = new List<DateTime>() { vv.MinTime };
                                    }
                                }
                            }
                        }
                        time = ret;
                        return re;
                    default:
                        time = ret;
                        return 0;
                }

            }
            else
            {
                var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
                var re = StatisticsValue(vals, type,out ret);
                time = ret;
                return re;
            }
        }

        /// <summary>
        /// 获取一段时间内的平均值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public double FindNumberTagAvgValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime)
        {
            List<DateTime> ret = new List<DateTime>();
            var hh = (endTime - startTime).TotalHours;
            DateTime retime = DateTime.MinValue;
            if (hh > 2)
            {
                //从统计数据文件中读取值，加快统计数据的读取
                var etime1 = new DateTime(startTime.Year, startTime.Month, startTime.Day, startTime.Hour + 1, 0, 0);
                var etime2 = new DateTime(endTime.Year, endTime.Month, endTime.Day, endTime.Hour, 0, 0);
                var val1 = ReadAllValueByUTCTime<T>(id, startTime, etime1);
                var val3 = ReadAllValueByUTCTime<T>(id, etime2, endTime);

                var vvs = ReadNumberStatisticsByUTCTime(id, etime1, etime2);

                double re = 0;
                var ttime = (endTime - startTime).TotalSeconds;
                double mv1 = StatisticsValue<T>(val1, NumberStatisticsType.Avg, out List<DateTime> time);
                if (mv1 != double.MinValue)
                {
                    re += mv1 * ((etime1 - startTime).TotalSeconds);
                }
                else
                {
                    ttime -= (etime1 - startTime).TotalSeconds;
                }
                double mv2 = StatisticsValue<T>(val3, NumberStatisticsType.Avg, out List<DateTime> time2);

                if (mv2 != double.MinValue)
                {
                    re += mv2 * ((endTime - etime2).TotalSeconds);
                }
                else
                {
                    ttime -= (endTime - etime2).TotalSeconds;
                }

                if (vvs != null)
                {
                    foreach (var vv in vvs.ListAllValue())
                    {
                        if (vv.AvgValue != double.MinValue)
                        {
                            re += vv.AvgValue * 3600;
                        }
                        else
                        {
                            ttime -= 3600;
                        }

                    }
                }
                if (ttime > 0)
                {
                    re = re / ttime;
                    return re;
                }
                else
                {
                    return double.MinValue;
                }

            }
            else
            {
                var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
                var re = StatisticsValue(vals,NumberStatisticsType.Avg, out ret);
                return re;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="vals"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private double StatisticsValue<T>(HisQueryResult<T> vals, NumberStatisticsType type, out List<DateTime> datetime)
        {
            double re = 0;
            List<DateTime> ret = new List<DateTime>();
            switch (type)
            {
                case NumberStatisticsType.Max:
                    re = double.MinValue;
                    for (int i = 0; i < vals.Count; i++)
                    {
                        var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));
                        if (IsGoodQuality(qu))
                        {
                            re = Math.Max(val, re);
                            if (re == val)
                            {
                                ret.Add(time);
                            }
                        }
                    }
                    break;
                case NumberStatisticsType.Min:
                    re = double.MaxValue;
                    for (int i = 0; i < vals.Count; i++)
                    {
                        var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));
                        if (IsGoodQuality(qu))
                        {
                            re = Math.Min(val, re);
                            if (re == val)
                            {
                                ret.Add(time);
                            }
                        }
                    }
                    break;
                case NumberStatisticsType.Avg:
                    re = double.MinValue;
                    int count = 0;
                    for (int i = 0; i < vals.Count; i++)
                    {
                        var val = Convert.ToDouble(vals.GetValue(i, out DateTime time, out byte qu));
                        if (IsGoodQuality(qu))
                        {
                            re += val;
                            count++;
                        }
                    }
                    if (count > 0)
                        re = re / count;
                    break;


            }
            datetime = ret;
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="qu"></param>
        /// <returns></returns>
        bool IsGoodQuality(byte qu)
        {
            return qu == 0 || qu == 100;
        }

        #endregion


        #region No Number Tag Statistics

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public double FindNoNumberTagValueDuration<T>(int id, DateTime startTime, DateTime endTime,  object para)
        {
            return FindNoNumberTagValueDurationByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(),  para);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public DateTime FindNoNumberTagValue<T>(int id, DateTime startTime, DateTime endTime, object para)
        {
            return FindNoNumberTagValueByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public List<DateTime> FindNoNumberTagValues<T>(int id, DateTime startTime, DateTime endTime, object para)
        {
            return FindNoNumberTagValuesByUTCTime<T>(id, startTime.ToUniversalTime(), endTime.ToUniversalTime(), para);
        }

        /// <summary>
        /// 查找指定的值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public DateTime FindNoNumberTagValueByUTCTime<T>(int id, DateTime startTime, DateTime endTime, object para)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            for (int i = 0; i < vals.Count; i++)
            {
                if ((object)vals.GetValue(i,out DateTime time,out byte qu) == para)
                {
                    if(IsGoodQuality(qu))
                    return time;
                }
            }
            return DateTime.MinValue;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public List<DateTime> FindNoNumberTagValuesByUTCTime<T>(int id, DateTime startTime, DateTime endTime,  object para)
        {
            List<DateTime> re = new List<DateTime>();
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            for (int i = 0; i < vals.Count; i++)
            {
                if ((object)vals.GetValue(i, out DateTime time, out byte qu) == para)
                {
                    if (IsGoodQuality(qu))
                    {
                        re.Add(time);
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public double FindNoNumberTagValueDurationByUTCTime<T>(int id, DateTime startTime, DateTime endTime,  object para)
        {
            var vals = ReadAllValueByUTCTime<T>(id, startTime, endTime);
            double tim = 0;
            bool ishase = false;
            DateTime statetime = DateTime.MinValue;

            for (int i = 0; i < vals.Count; i++)
            {

                if ((object)vals.GetValue(i, out DateTime time, out byte qua) == para)
                {
                    if (!ishase)
                    {
                        ishase = true;
                        statetime = time;
                    }
                }
                else
                {
                    if (ishase)
                    {
                        ishase = false;
                        tim += (time - statetime).TotalSeconds;
                    }
                }
            }

            if (ishase)
            {
                vals.GetValue(vals.Count - 1, out DateTime time, out byte qua);
                tim += (time - statetime).TotalSeconds;
            }
            return tim;
        }



        #endregion
    }
}
