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
    public class QuerySerivce: IHisQuery
    {


        /// <summary>
        /// 
        /// </summary>
        public QuerySerivce()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        public QuerySerivce(string databaseName)
        {
            Database = databaseName;
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
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue<T>(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            List<DateTime> mLogTimes = new List<DateTime>();
            var vfiles = GetFileManager().GetDataFiles(times, mLogTimes, id);

            DataFileInfo mPreFile = null;

            List<DateTime> mtime = new List<DateTime>();

            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.Read<T>(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add(false, vv.Key, (byte)QualityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
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
                mPreFile.Read<T>(id, mtime, type, result);
            }

            ReadLogFile(id, mLogTimes, type, result);
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
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue<T>(int id, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            Tuple<DateTime, DateTime> mLogFileTimes;
            var vfiles = GetFileManager().GetDataFiles(startTime, endTime, out mLogFileTimes, id);
            vfiles.ForEach(e => {
                DateTime sstart = e.StartTime > startTime ? e.StartTime : startTime;
                DateTime eend = e.EndTime > endTime ? endTime : endTime;
                e.ReadAllValue(id, sstart, eend, result);
            });

            if (mLogFileTimes.Item1 != DateTime.MinValue)
            {
                ReadLogFileAllValue(id, mLogFileTimes.Item1, mLogFileTimes.Item2, result);
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
            int valueCount = (int)(endTime - startTime).TotalSeconds;
            var result = new HisQueryResult<T>(valueCount);
            ReadAllValue(id, startTime, endTime, result);
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
        public HisQueryResult<T> ReadValue<T>(int id, List<DateTime> times, QueryValueMatchType type)
        {
            int valueCount = times.Count;

            var result = new HisQueryResult<T>(valueCount);
            ReadValue(id, times, type, result);
            return result; 
        }
    }
}
