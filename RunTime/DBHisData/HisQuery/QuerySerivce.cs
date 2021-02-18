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

            //从日志文件中读取数据
            ReadLogFile(id, mLogTimes, type, result);

            //从内存中读取数据
            ReadFromMemory(id, mMemoryTimes, type, result);
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
    }
}
