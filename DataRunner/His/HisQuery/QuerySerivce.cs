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
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id,List<DateTime> times,QueryValueMatchType type,HisQueryResult<bool> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach(var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadBool(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add(false, vv.Key, (byte)QulityConst.Null);
                }
                else if(vv.Value!=mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadBool(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if(mPreFile!=null)
            {
                mPreFile.ReadBool(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadByte(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add((byte)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadByte(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadByte(id, mtime, type, result);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<short> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadShort(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add((short)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadShort(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadShort(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadUShort(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add((ushort)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadUShort(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadUShort(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<int> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadInt(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add((int)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadInt(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadInt(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadUInt(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add((uint)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadUInt(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadUInt(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadULong(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add((ulong)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadULong(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadULong(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<long> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadLong(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }

                    result.Add((long)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadLong(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadLong(id, mtime, type, result);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<float> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadFloat(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }

                    result.Add((float)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadFloat(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadFloat(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<double> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadDouble(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }

                    result.Add((double)0, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadDouble(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadDouble(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadDateTime(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add(DateTime.Now, vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadDateTime(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadDateTime(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public void ReadValue(int id, List<DateTime> times, QueryValueMatchType type, HisQueryResult<string> result)
        {
            var vfiles = GetFileManager().GetFiles(times);
            MinuteTimeFile mPreFile = null;
            List<DateTime> mtime = new List<DateTime>();
            foreach (var vv in vfiles)
            {
                if (vv.Value == null)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadString(id, mtime, type, result);
                        mPreFile = null;
                        mtime.Clear();
                    }
                    result.Add("", vv.Key, (byte)QulityConst.Null);
                }
                else if (vv.Value != mPreFile)
                {
                    if (mPreFile != null)
                    {
                        mPreFile.ReadString(id, mtime, type, result);
                    }
                    mPreFile = vv.Value;
                    mtime.Clear();
                    mtime.Add(vv.Key);
                }
            }
            if (mPreFile != null)
            {
                mPreFile.ReadString(id, mtime, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id,DateTime startTime,DateTime endTime,HisQueryResult<bool> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart,out eend);    
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<byte> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<short> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<ushort> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<int> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<uint> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<long> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<ulong> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<float> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<double> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<DateTime> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public void ReadAllValue(int id, DateTime startTime, DateTime endTime, HisQueryResult<string> result)
        {
            var vfiles = GetFileManager().GetFiles(startTime, endTime);
            vfiles.ForEach(e => {
                DateTime sstart, eend;
                e.GetTimeSpan(startTime, endTime, out sstart, out eend);
                e.ReadAllValue(id, sstart, eend, result);
            });
        }


    }


    public static class MinuteTimeFileExtend
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static DataFileSeriserbase GetFileSeriser(this MinuteTimeFile file)
        {
            return DataFileSeriserManager.manager.GetDefaultFileSersie();
        }

        #region 读取所有值
        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="file"></param>
        private static void GeneratorTime(Dictionary<long, Tuple<DateTime, DateTime>> times, DateTime start, DateTime end, MinuteTimeFile file)
        {
            file.GetTimeSpan(out start, out end);

            int icount = 0;
            long laddr = 0;
            foreach (var vv in file.SecondOffset)
            {
                var vend = start.AddSeconds(vv.Key);
                if (icount > 0)
                {
                    times.Add(vv.Value, new Tuple<DateTime, DateTime>(start, vend));
                }
                laddr = vv.Value;
                start = vend;
            }

            if (start < end)
            {
                times.Add(laddr, new Tuple<DateTime, DateTime>(start, end));
            }
        }

        /// <summary>
        /// 读取某时间段内的所有bool值
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<bool> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 读取某时间段内的所有byte值
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<byte> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<short> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<ushort> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<int> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<uint> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>

        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<long> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<ulong> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<float> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<double> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<string> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this MinuteTimeFile file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<DateTime> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            GeneratorTime(moffs, startTime, endTime, file);

            foreach (var vf in moffs)
            {
                vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            }
        }

        #endregion

        #region 读取指定时刻值

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static bool? ReadBool(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadBool(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public static HisQueryResult<bool> ReadBool(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<bool> re = new HisQueryResult<bool>(times.Count);
            ReadBool(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadBool(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadBool(vf.Key, tid, vf.Value, type, result);
            }
        }

     

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static byte? ReadByte(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadByte(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<byte> ReadByte(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<byte> re = new HisQueryResult<byte>(times.Count);
            ReadByte(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadByte(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadByte(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static short? ReadShort(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadShort(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<short> ReadShort(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<short> re = new HisQueryResult<short>(times.Count);
            ReadShort(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadShort(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<short> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }
            foreach (var vf in moffs)
            {
                vff.ReadShort(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static ushort? ReadUShort(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadUShort(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public static HisQueryResult<ushort> ReadUShort(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<ushort> re = new HisQueryResult<ushort>(times.Count);
            ReadUShort(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadUShort(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadUShort(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static int? ReadInt(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadInt(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<int> ReadInt(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<int> re = new HisQueryResult<int>(times.Count);
            ReadInt(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadInt(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<int> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadInt(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static uint? ReadUInt(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadUInt(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<uint> ReadUInt(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<uint> re = new HisQueryResult<uint>(times.Count);
            ReadUInt(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadUInt(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }
            foreach (var vf in moffs)
            {
                vff.ReadUInt(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static long? ReadLong(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadLong(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<long> ReadLong(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {

            HisQueryResult<long> re = new HisQueryResult<long>(times.Count);
            ReadLong(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadLong(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<long> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadLong(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static ulong? ReadULong(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadULong(offset, tid, time, type);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<ulong> ReadULong(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<ulong> re = new HisQueryResult<ulong>(times.Count);
            ReadULong(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadULong(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadULong(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static float? ReadFloat(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadFloat(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<float> ReadFloat(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<float> re = new HisQueryResult<float>(times.Count);
            ReadFloat(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadFloat(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<float> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadFloat(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static double? ReadDouble(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadDouble(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<double> ReadDouble(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<double> re = new HisQueryResult<double>(times.Count);
            ReadDouble(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadDouble(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<double> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }

            foreach (var vf in moffs)
            {
                vff.ReadDouble(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static DateTime? ReadDateTime(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadDateTime(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<DateTime> ReadDateTime(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<DateTime> re = new HisQueryResult<DateTime>(times.Count);
            ReadDateTime(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadDateTime(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }
            foreach (var vf in moffs)
            {
                vff.ReadDateTime(vf.Key, tid, vf.Value, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public static string ReadString(this MinuteTimeFile file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetOffset(time.Second);
            return vff.ReadString(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public static HisQueryResult<string> ReadString(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<string> re = new HisQueryResult<string>(times.Count);
            ReadString(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void ReadString(this MinuteTimeFile file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<string> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetOffset(vv.Second);
                if (moffs.ContainsKey(ff))
                {
                    moffs[ff].Add(vv);
                }
                else
                {
                    moffs.Add(ff, new List<DateTime>() { vv });
                }
            }
            foreach (var vf in moffs)
            {
                vff.ReadString(vf.Key, tid, vf.Value, type, result);
            }
        }
        #endregion

        #region DataFileSeriser Read

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static bool? ReadBool(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToBoolValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static HisQueryResult<bool> ReadBool(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<bool> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToBoolValue(vv.Key, vv.Value, timetick, type, res);
            }
            return res;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static byte? ReadByte(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToByteValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadByte(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<byte> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToByteValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static short? ReadShort(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToShortValue(data, dataTime, timetick, type);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadShort(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<short> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToShortValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static ushort? ReadUShort(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToUShortValue(data, dataTime, timetick, type);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadUShort(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<ushort> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToUShortValue(vv.Key, vv.Value, timetick, type, res);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static int? ReadInt(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToIntValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadInt(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<int> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToIntValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static uint? ReadUInt(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToUIntValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadUInt(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<uint> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToUIntValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static long? ReadLong(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToLongValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <param name="type"></param>
        /// <param name="res"></param>
        public static void ReadLong(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<long> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToLongValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static ulong? ReadULong(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToULongValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadULong(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<ulong> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToULongValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static string ReadString(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToStringValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadString(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<string> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToStringValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static DateTime? ReadDateTime(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToDateTimeValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadDateTime(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<DateTime> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToDateTimeValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static float? ReadFloat(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToFloatValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadFloat(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<float> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToFloatValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <returns></returns>
        public static double? ReadDouble(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick);
            return DeCompressDataBlockToDoubleValue(data, dataTime, timetick, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <returns></returns>
        public static void ReadDouble(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<double> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockToDoubleValue(vv.Key, vv.Value, timetick, type, res);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<bool> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<byte> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<ushort> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<short> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<uint> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<int> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<ulong> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<long> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<string> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<DateTime> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<float> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime,DateTime endTime, HisQueryResult<double> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
        }
        #endregion

        #region DeCompressData

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static bool? DeCompressDataBlockToBoolValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressBoolValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToBoolValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressBoolValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static byte? DeCompressDataBlockToByteValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressByteValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToByteValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressByteValue(memory, 1, datatime, timeTick, type, result);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static short? DeCompressDataBlockToShortValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressShortValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToShortValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressShortValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static ushort? DeCompressDataBlockToUShortValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressUShortValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToUShortValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressUShortValue(memory, 1, datatime, timeTick, type, result);
            }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static int? DeCompressDataBlockToIntValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressIntValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToIntValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<int> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressIntValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static uint? DeCompressDataBlockToUIntValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressUIntValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToUIntValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressUIntValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static long? DeCompressDataBlockToLongValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressLongValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        private static void DeCompressDataBlockToLongValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<long> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressLongValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static ulong? DeCompressDataBlockToULongValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressULongValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        private static void DeCompressDataBlockToULongValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressULongValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static float? DeCompressDataBlockToFloatValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressFloatValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToFloatValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<float> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressFloatValue(memory, 1, datatime, timeTick, type, result);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static Double? DeCompressDataBlockToDoubleValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressDoubleValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToDoubleValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressDoubleValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static string DeCompressDataBlockToStringValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressStringValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToStringValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressStringValue(memory, 1, datatime, timeTick, type, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        private static DateTime? DeCompressDataBlockToDateTimeValue(MemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressDateTimeValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockToDateTimeValue(MemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressDateTimeValue(memory, 1, datatime, timeTick, type, result);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory,DateTime startTime,DateTime endTime, int timeTick,HisQueryResult<bool> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockAllValue(MemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            MemoryBlock target = new MemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
            }
        }
        #endregion

        #region 读取数据区域头数据
        /// <summary>
        /// 检测数据头部指针区域数据是否被缓存
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="fileDuration"></param>
        /// <param name="blockDuration"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static Dictionary<int, long> CheckBlockHeadCach(this DataFileSeriserbase datafile, long offset, out int fileDuration, out int blockDuration, out int timetick)
        {
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { + tagid1+tagid2+...+tagidn }+ {tag1 block point1(8) + block size(4) + tag1 block point2(8)+ block size(4) + tag1 block point3(8)+ block size(4)+...+tag1 block pointn(8)+ block size(4) + tag2 block point1 (8)+ block size(4)+ tag2 block point2(8)+ block size(4)+....}
            var dataoffset = offset + 24;
            //读取变量个数
            int count = datafile.ReadInit(dataoffset);
            dataoffset += 4;

            //读取校验和
            long idsum = datafile.ReadLong(dataoffset);
            dataoffset += 8;

            //读取单个文件的时长
            fileDuration = datafile.ReadInit(dataoffset);
            dataoffset += 4;
            //读取数据块时长
            blockDuration = datafile.ReadInit(dataoffset);
            dataoffset += 4;
            //读取时钟周期
            timetick = datafile.ReadInit(dataoffset);
            dataoffset += 4;

            lock (TagHeadOffsetManager.manager)
            {
                if (!TagHeadOffsetManager.manager.Contains(idsum, count))
                {
                    //Tag id 列表经过压缩，内容格式为:DataSize + Data
                    var dsize = datafile.ReadInit(dataoffset);
                    dataoffset += 4;
                    using (var dd = datafile.Read(dataoffset, dsize))
                    {
                        VarintCodeMemory vcm = new VarintCodeMemory(dd.StartMemory);
                        var ltmp = vcm.ToIntList();
                        var dtmp = new Dictionary<int, long>();
                        if (ltmp.Count > 0)
                        {
                            int preid = ltmp[0];
                            dtmp.Add(preid,0);
                            for (int i = 0; i < ltmp.Count; i++)
                            {
                                var id = ltmp[i] + preid;
                                dtmp.Add(id, i);
                                preid = id;
                            }
                        }
                        TagHeadOffsetManager.manager.Add(idsum, count, dtmp);
                        return dtmp;
                    }
                }
                else
                {
                    return TagHeadOffsetManager.manager.Get(idsum, count);
                }
            }
        }

        /// <summary>
        /// 读取数据区域的数据头数据
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="fileDuration"></param>
        /// <param name="blockDuration"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static long ReadTargetBlockAddress(this DataFileSeriserbase datafile, int tid, long offset, out int fileDuration, out int blockDuration, out int timetick)
        {
            var hfile = datafile.CheckBlockHeadCach(offset, out fileDuration, out blockDuration, out timetick);
            if (hfile.ContainsKey(tid))
            {
                return hfile[tid];
            }
            return -1;
        }

        /// <summary>
        /// 读取数据区域的数据头数据
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="fileDuration"></param>
        /// <param name="blockDuration"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static List<long> ReadTargetBlockAddress(this DataFileSeriserbase datafile, List<int> tid, long offset, out int fileDuration, out int blockDuration, out int timetick)
        {
            var hfile = datafile.CheckBlockHeadCach(offset, out fileDuration, out blockDuration, out timetick);
            List<long> re = new List<long>();
            foreach (var vv in tid)
            {
                if (hfile.ContainsKey(vv))
                {
                    re.Add(hfile[vv]);
                }
                else
                {
                    re.Add(-1);
                }
            }
            return re;
        }
        #endregion

        #region 读取数据块

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="dataTime"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static MemoryBlock ReadTagDataBlock(this DataFileSeriserbase datafile, int tid, long offset, DateTime dataTime, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            var blockpointer = datafile.ReadTargetBlockAddress(tid, offset, out fileDuration, out blockDuration, out timetick);
            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(16);
            var ttmp = (dataTime - startTime).TotalMinutes;
            int dindex = (int)(ttmp / blockDuration);
            if (ttmp % blockDuration > 0)
            {
                dindex++;
            }

            if (dindex > blockcount)
            {
                throw new Exception("DataPointer index is out of total block number");
            }

            var dataPointer = datafile.ReadLong(blockpointer + dindex * 12);
            var datasize = datafile.ReadInit(blockpointer + dindex * 12 + 8);

            return datafile.Read(dataPointer, datasize);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="dataTimes"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static Dictionary<DateTime, MemoryBlock> ReadTagDataBlock(this DataFileSeriserbase datafile, int tid, long offset, List<DateTime> dataTimes, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            var blockpointer = datafile.ReadTargetBlockAddress(tid, offset, out fileDuration, out blockDuration, out timetick);
            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(16);

            Dictionary<long, MemoryBlock> rtmp = new Dictionary<long, MemoryBlock>();

            Dictionary<DateTime, MemoryBlock> re = new Dictionary<DateTime, MemoryBlock>();

            foreach (var vdd in dataTimes)
            {
                var ttmp = (vdd - startTime).TotalMinutes;
                int dindex = (int)(ttmp / blockDuration);
                if (ttmp % blockDuration > 0)
                {
                    dindex++;
                }

                if (dindex > blockcount)
                {
                    throw new Exception("DataPointer index is out of total block number");
                }

                var dataPointer = datafile.ReadLong(blockpointer + dindex * 12);
                var datasize = datafile.ReadInit(blockpointer + dindex * 12 + 8);

                if (!rtmp.ContainsKey(dataPointer))
                {
                    var rmm = datafile.Read(dataPointer, datasize);
                    re.Add(vdd, rmm);
                    rtmp.Add(dataPointer, rmm);
                }
                else
                {
                    re.Add(vdd, rtmp[dataPointer]);
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="dataTimes"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static Dictionary<MemoryBlock, List<DateTime>> ReadTagDataBlock2(this DataFileSeriserbase datafile, int tid, long offset, List<DateTime> dataTimes, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            var blockpointer = datafile.ReadTargetBlockAddress(tid, offset, out fileDuration, out blockDuration, out timetick);
            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(16);

            Dictionary<long, MemoryBlock> rtmp = new Dictionary<long, MemoryBlock>();

            Dictionary<MemoryBlock, List<DateTime>> re = new Dictionary<MemoryBlock, List<DateTime>>();

            foreach (var vdd in dataTimes)
            {
                var ttmp = (vdd - startTime).TotalMinutes;
                int dindex = (int)(ttmp / blockDuration);
                if (ttmp % blockDuration > 0)
                {
                    dindex++;
                }

                if (dindex > blockcount)
                {
                    throw new Exception("DataPointer index is out of total block number");
                }

                var dataPointer = datafile.ReadLong(blockpointer + dindex * 12);
                var datasize = datafile.ReadInit(blockpointer + dindex * 12 + 8);

                if (!rtmp.ContainsKey(dataPointer))
                {
                    var rmm = datafile.Read(dataPointer, datasize);
                    if (!re.ContainsKey(rmm))
                    {
                        re.Add(rmm, new List<DateTime>() { vdd });
                    }
                    else
                    {
                        re[rmm].Add(vdd);
                    }
                    rtmp.Add(dataPointer, rmm);
                }
                else
                {
                    var rmm = rtmp[dataPointer];
                    if (!re.ContainsKey(rmm))
                    {
                        re.Add(rmm, new List<DateTime>() { vdd });
                    }
                    else
                    {
                        re[rmm].Add(vdd);
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static Dictionary<MemoryBlock, Tuple<DateTime, DateTime>> ReadTagDataBlock2(this DataFileSeriserbase datafile, int tid, long offset, DateTime start,DateTime end, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            var blockpointer = datafile.ReadTargetBlockAddress(tid, offset, out fileDuration, out blockDuration, out timetick);
            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(16);

            Dictionary<MemoryBlock, Tuple<DateTime,DateTime>> re = new Dictionary<MemoryBlock, Tuple<DateTime, DateTime>>();

            DateTime sstart = start;
            DateTime send = end;

            while(sstart<end)
            {
                var ttmp = (sstart - startTime).TotalMinutes;
                send = sstart.AddMinutes(blockDuration);
                if(send>end)
                {
                    send = end;
                }
                int dindex = (int)(ttmp / blockDuration);
                if (ttmp % blockDuration > 0)
                {
                    dindex++;
                }

                if (dindex > blockcount)
                {
                    throw new Exception("DataPointer index is out of total block number");
                }

                var dataPointer = datafile.ReadLong(blockpointer + dindex * 12);
                var datasize = datafile.ReadInit(blockpointer + dindex * 12 + 8);

                var rmm = datafile.Read(dataPointer, datasize);
                if (!re.ContainsKey(rmm))
                {
                    re.Add(rmm, new Tuple<DateTime, DateTime>(sstart, end));
                }

                sstart = send;
            }
            return re;
        }
        #endregion
    }
}
