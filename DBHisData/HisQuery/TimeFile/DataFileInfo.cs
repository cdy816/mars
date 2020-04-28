//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/16 13:33:36.
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
    public class DataFileInfo
    {

        #region ... Variables  ...

        private SortedDictionary<DateTime, Tuple<TimeSpan, long>> mTimeOffsets = new SortedDictionary<DateTime, Tuple<TimeSpan, long>>();

        private bool mInited = false;

        private object mLockObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string FileName { get; set; }

        /// <summary>
        /// 开始时间
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime { 
            get 
            {
                return StartTime + Duration;
            }
            set
            {
                Duration = value - StartTime;
            } 
        }

        /// <summary>
        /// 时间长度
        /// </summary>
        public TimeSpan Duration { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public void Scan()
        {
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenFile(FileName);
                long offset = DataFileManager.FileHeadSize;
                DateTime time;

                do
                {
                    time = ss.ReadDateTime(offset + 16);
                    long oset = offset;
                    offset = ss.ReadLong(offset + 8);

                    if (offset != 0)
                    {
                        var dt2 = ss.ReadDateTime(offset + 16);
                        mTimeOffsets.Add(time, new Tuple<TimeSpan, long>(dt2 - time, oset));
                    }
                    else
                    {
                        var tspan = StartTime + Duration - time;
                        if(tspan.TotalMilliseconds>0)
                        mTimeOffsets.Add(time, new Tuple<TimeSpan, long>(StartTime + Duration - time, oset));
                    }
                }
                while (offset != 0);
                mInited = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public long GetFileOffsets(DateTime time)
        {
            lock (mLockObj)
                if (!mInited) Scan();

            foreach (var vv in mTimeOffsets)
            {
                if (vv.Key <= time && time < (vv.Key + vv.Value.Item1))
                {
                    return vv.Value.Item2;
                }
            }
            return -1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public Dictionary<DateTime, Tuple<TimeSpan, long>> GetFileOffsets(DateTime startTime, DateTime endTime)
        {
            lock (mLockObj)
                if (!mInited) Scan();
            Dictionary<DateTime, Tuple<TimeSpan, long>> re = new Dictionary<DateTime, Tuple<TimeSpan, long>>();

            foreach (var vv in mTimeOffsets)
            {
                if (vv.Key >= startTime && vv.Key < endTime)
                {
                    re.Add(vv.Key, vv.Value);
                }
            }

            return re;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class DataFileInfoExtend
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static DataFileSeriserbase GetFileSeriser(this DataFileInfo file)
        {
            var re = DataFileSeriserManager.manager.GetDefaultFileSersie();
            re.FileName = file.FileName;
            re.OpenFile(file.FileName);
            return re;
        }

        #region 读取所有值
        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="file"></param>
        private static void GeneratorTime(Dictionary<long, Tuple<DateTime, DateTime>> times, DateTime start, DateTime end, DataFileInfo file)
        {
            //file.GetTimeSpan(out start, out end);

            //int icount = 0;
            //long laddr = 0;
            //foreach (var vv in file.SecondOffset)
            //{
            //    var vend = start.AddSeconds(vv.Key);
            //    if (icount > 0)
            //    {
            //        times.Add(vv.Value.Item1, new Tuple<DateTime, DateTime>(start, vend));
            //    }
            //    laddr = vv.Value.Item1;
            //    start = vend;
            //}

            //if (start < end)
            //{
            //    times.Add(laddr, new Tuple<DateTime, DateTime>(start, end));
            //}
        }

        /// <summary>
        /// 读取某时间段内的所有bool值
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<bool> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<byte> result)
        {
            Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();
            using (var vff = file.GetFileSeriser())
            {
                GeneratorTime(moffs, startTime, endTime, file);

                foreach (var vf in moffs)
                {
                    vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
                }
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<short> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<ushort> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<int> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<uint> result)
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

        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<long> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<ulong> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<float> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<double> result)
        {
            var vff = file.GetFileSeriser();

            var offset = file.GetFileOffsets(startTime,endTime);

            foreach (var vv in offset)
            {
                DateTime stime = vv.Key > startTime ? vv.Key : startTime;
                DateTime etime = vv.Key + vv.Value.Item1 > endTime ? endTime : vv.Key + vv.Value.Item1;
                vff.ReadAllValue(vv.Value.Item2, tid, stime, etime, result);
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<string> result)
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
        public static void ReadAllValue(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<DateTime> result)
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
        public static bool? ReadBool(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
            return vff.ReadBool(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public static HisQueryResult<bool> ReadBool(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadBool(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static byte? ReadByte(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<byte> ReadByte(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadByte(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static short? ReadShort(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<short> ReadShort(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadShort(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<short> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static ushort? ReadUShort(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
            return vff.ReadUShort(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public static HisQueryResult<ushort> ReadUShort(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadUShort(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static int? ReadInt(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<int> ReadInt(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadInt(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<int> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static uint? ReadUInt(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<uint> ReadUInt(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadUInt(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static long? ReadLong(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<long> ReadLong(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadLong(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<long> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static ulong? ReadULong(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<ulong> ReadULong(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadULong(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static float? ReadFloat(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<float> ReadFloat(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadFloat(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<float> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static double? ReadDouble(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<double> ReadDouble(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadDouble(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<double> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
                if (ff <= 0) continue;
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
        public static DateTime? ReadDateTime(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
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
        public static HisQueryResult<DateTime> ReadDateTime(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadDateTime(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static string ReadString(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            var vff = file.GetFileSeriser();
            var offset = file.GetFileOffsets(time);
            return vff.ReadString(offset, tid, time, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <returns></returns>
        public static HisQueryResult<string> ReadString(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void ReadString(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<string> result)
        {
            var vff = file.GetFileSeriser();
            Dictionary<long, List<DateTime>> moffs = new Dictionary<long, List<DateTime>>();
            foreach (var vv in times)
            {
                var ff = file.GetFileOffsets(vv);
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
        public static void ReadAllValue(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<double> result)
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
        private static bool? DeCompressDataBlockToBoolValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToBoolValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static byte? DeCompressDataBlockToByteValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToByteValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static short? DeCompressDataBlockToShortValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToShortValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static ushort? DeCompressDataBlockToUShortValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToUShortValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static int? DeCompressDataBlockToIntValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToIntValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<int> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static uint? DeCompressDataBlockToUIntValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToUIntValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static long? DeCompressDataBlockToLongValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressLongValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        private static void DeCompressDataBlockToLongValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<long> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static ulong? DeCompressDataBlockToULongValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressULongValue(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        private static void DeCompressDataBlockToULongValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static float? DeCompressDataBlockToFloatValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToFloatValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<float> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static Double? DeCompressDataBlockToDoubleValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToDoubleValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static string DeCompressDataBlockToStringValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToStringValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static DateTime? DeCompressDataBlockToDateTimeValue(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockToDateTimeValue(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        private static void DeCompressDataBlockAllValue(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
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
        public static Dictionary<int, int> CheckBlockHeadCach(this DataFileSeriserbase datafile, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick,out long blockPointer)
        {
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { + tagid1+tagid2+...+tagidn }+ {[tag1 block point1(8) + tag2 block point1+ tag3 block point1+...] + [tag1 block point2(8) + tag2 block point2+ tag3 block point2+...]....}
            var dataoffset = offset + 24;
            //读取变量个数
            int count = datafile.ReadInt(dataoffset);
            dataoffset += 4;

            tagCount = count;

            //读取校验和
            long idsum = datafile.ReadLong(dataoffset);
            dataoffset += 8;

            //读取单个文件的时长
            fileDuration = datafile.ReadInt(dataoffset);
            dataoffset += 4;
            //读取数据块时长
            blockDuration = datafile.ReadInt(dataoffset);
            dataoffset += 4;
            //读取时钟周期
            timetick = datafile.ReadInt(dataoffset);
            dataoffset += 4;

            lock (TagHeadOffsetManager.manager)
            {
                if (!TagHeadOffsetManager.manager.Contains(idsum, count))
                {
                    //Tag id 列表经过压缩，内容格式为:DataSize + Data
                    var dsize = datafile.ReadInt(dataoffset);

                    if (dsize <= 0)
                    {
                        tagCount = 0;
                        fileDuration = 0;
                        blockDuration = 0;
                        timetick = 0;
                        blockPointer = 0;
                        return new Dictionary<int, int>();
                    }

                    dataoffset += 4;

                    blockPointer = dataoffset + dsize - offset;

                    using (var dd = datafile.Read(dataoffset, dsize))
                    {
                        MarshalVarintCodeMemory vcm = new MarshalVarintCodeMemory(dd.StartMemory, dsize);
                        var ltmp = vcm.ToIntList();

                        var dtmp = new Dictionary<int, int>();
                        if (ltmp.Count > 0)
                        {
                            int preid = ltmp[0];
                            dtmp.Add(preid, 0);
                            for (int i = 1; i < ltmp.Count; i++)
                            {
                                var id = ltmp[i] + preid;
                                dtmp.Add(id, i);
                                preid = id;
                            }
                        }
                        TagHeadOffsetManager.manager.Add(idsum, count, dtmp,blockPointer);

                        return dtmp;
                    }
                }
                else
                {
                    var re = TagHeadOffsetManager.manager.Get(idsum, count);
                    blockPointer = re.Item2;
                    return re.Item1;
                }
            }
        }

        /// <summary>
        /// 读取某个变量在头部文件种的序号
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="fileDuration"></param>
        /// <param name="blockDuration"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static int ReadTagIndexInDataPointer(this DataFileSeriserbase datafile, int tid, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick,out long blockpointer)
        {
            var hfile = datafile.CheckBlockHeadCach(offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer);
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
        public static List<long> ReadTargetBlockAddress(this DataFileSeriserbase datafile, List<int> tid, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockpointer)
        {
            var hfile = datafile.CheckBlockHeadCach(offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer);
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
        public static MarshalMemoryBlock ReadTagDataBlock(this DataFileSeriserbase datafile, int tid, long offset, DateTime dataTime, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;

            long blockpointer = 0;

            var blockIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer);
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

            var dataPointer = datafile.ReadLong(blockIndex * 8 + dindex * tagCount * 8); //读取DataBlock的地址

            var datasize = datafile.ReadInt(dataPointer); //读取DataBlock 的大小

            return datafile.Read(dataPointer + 4, datasize);
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
        public static Dictionary<DateTime, MarshalMemoryBlock> ReadTagDataBlock(this DataFileSeriserbase datafile, int tid, long offset, List<DateTime> dataTimes, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            long blockpointer = 0;
            var blockIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer);
            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(16);

            Dictionary<long, MarshalMemoryBlock> rtmp = new Dictionary<long, MarshalMemoryBlock>();

            Dictionary<DateTime, MarshalMemoryBlock> re = new Dictionary<DateTime, MarshalMemoryBlock>();

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

                var dataPointer = datafile.ReadLong(blockIndex * 8 + dindex * tagCount * 8); //读取DataBlock的地址

                var datasize = datafile.ReadInt(dataPointer); //读取DataBlock 的大小

                if (!rtmp.ContainsKey(dataPointer))
                {
                    var rmm = datafile.Read(dataPointer + 4, datasize);
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
        public static Dictionary<MarshalMemoryBlock, List<DateTime>> ReadTagDataBlock2(this DataFileSeriserbase datafile, int tid, long offset, List<DateTime> dataTimes, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            long blockpointer = 0;

            var tagIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer);
            
            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(0);

            Dictionary<long, MarshalMemoryBlock> rtmp = new Dictionary<long, MarshalMemoryBlock>();

            Dictionary<MarshalMemoryBlock, List<DateTime>> re = new Dictionary<MarshalMemoryBlock, List<DateTime>>();

            foreach (var vdd in dataTimes)
            {
                var ttmp = (vdd - startTime).TotalMinutes;
                int blockindex = (int)(ttmp / blockDuration);
                //if (ttmp % blockDuration > 0)
                //{
                //    blockindex++;
                //}

                if (blockindex > blockcount)
                {
                    throw new Exception("DataPointer index is out of total block number");
                }


                var dataPointer = datafile.ReadLong(offset + blockpointer + tagIndex * 8 + blockindex * tagCount * 8); //读取DataBlock的地址

                if (dataPointer > 0)
                {
                    var datasize = datafile.ReadInt(dataPointer); //读取DataBlock 的大小
                    if (datasize > 0)
                    {
                        //var rmm = datafile.Read(dataPointer + 4, (int)datasize);
                        //if (!re.ContainsKey(rmm))
                        //{
                        //    re.Add(rmm, new Tuple<DateTime, DateTime>(sstart, end));
                        //}

                        if (!rtmp.ContainsKey(dataPointer))
                        {
                            var rmm = datafile.Read(dataPointer + 4, datasize);
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
                    
                }
                
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="startTime"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="start"></param>
        /// <param name="end"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static Dictionary<MarshalMemoryBlock, Tuple<DateTime, DateTime>> ReadTagDataBlock2(this DataFileSeriserbase datafile,int tid, long offset, DateTime start, DateTime end, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            long blockpointer = 0;
            var tagIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer);
            int blockcount = fileDuration * 60 / blockDuration;

            //读取文件开始时间
            var startTime = datafile.ReadDateTime(0);

            Dictionary<MarshalMemoryBlock, Tuple<DateTime, DateTime>> re = new Dictionary<MarshalMemoryBlock, Tuple<DateTime, DateTime>>();

            DateTime sstart = start;
            DateTime send = end;

            while (sstart < end)
            {
                var ttmp = (sstart - startTime).TotalMinutes;
                send = (sstart  - new TimeSpan(0, 0, 0, sstart.Second, sstart.Millisecond)).AddMinutes(blockDuration);
                if (send > end)
                {
                    send = end;
                }
                int blockindex = (int)(ttmp / blockDuration);
                //if (ttmp % blockDuration > 0)
                //{
                //    dindex++;
                //}

                if (blockindex > blockcount)
                {
                    throw new Exception("DataPointer index is out of total block number");
                }

                var dataPointer = datafile.ReadLong(offset+ blockpointer + tagIndex * 8 + blockindex * tagCount * 8); //读取DataBlock的地址

                if (dataPointer > 0)
                {
                    var datasize = datafile.ReadInt(dataPointer); //读取DataBlock 的大小
                    if (datasize > 0)
                    {
                        var rmm = datafile.Read(dataPointer+4, (int)datasize);
                        if (!re.ContainsKey(rmm))
                        {
                            re.Add(rmm, new Tuple<DateTime, DateTime>(sstart, end));
                        }
                    }
                }
                sstart = send;
            }
            return re;
        }
        #endregion
    }
}
