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
using System.Net.Http.Headers;
using System.Numerics;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DataFileInfo
    {

        #region ... Variables  ...

        private SortedDictionary<DateTime, Tuple<TimeSpan, long,DateTime>> mTimeOffsets = new SortedDictionary<DateTime, Tuple<TimeSpan, long, DateTime>>();

        private bool mInited = false;

        private static object mLockObj = new object();

        private DateTime mLastTime;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string FId { get; set; }

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
        public void UpdateLastDatetime()
        {

            lock (mLockObj)
            {
                mTimeOffsets.Clear();
                Scan();

                if (DataFileManager.CurrentDateTime.ContainsKey(FId))
                {
                    DataFileManager.CurrentDateTime[FId] = mLastTime;
                }
                else
                {
                    DataFileManager.CurrentDateTime.Add(FId, mLastTime);
                }
            }

            

        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public void Scan()
        {
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenForReadOnly(FileName);
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
                        mTimeOffsets.Add(time, new Tuple<TimeSpan, long,DateTime>(dt2 - time, oset,dt2));
                        mLastTime = dt2;
                    }
                    else
                    {
                        var tspan = StartTime + Duration - time;
                        if(tspan.TotalMilliseconds>0)
                        mTimeOffsets.Add(time, new Tuple<TimeSpan, long, DateTime>(tspan, oset,time+tspan));
                        mLastTime = time + tspan;
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
        public Dictionary<DateTime, Tuple<TimeSpan, long,DateTime>> GetFileOffsets(DateTime startTime, DateTime endTime)
        {
            lock (mLockObj)
                if (!mInited) Scan();
            Dictionary<DateTime, Tuple<TimeSpan, long, DateTime>> re = new Dictionary<DateTime, Tuple<TimeSpan, long, DateTime>>();
            foreach (var vv in mTimeOffsets)
            {
                if (vv.Key >= startTime && vv.Key < endTime)
                {
                    re.Add(vv.Key, vv.Value);
                }
            }

            return re;
        }



        /// <summary>
        /// 获取某个时间段内，不包括数据的时间段集合
        /// </summary>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public List<Tuple<DateTime,DateTime>> GetNoValueOffsets(DateTime startTime,DateTime endTime)
        {
            List<Tuple<DateTime, DateTime>> re = new List<Tuple<DateTime, DateTime>>();
            DateTime stime = startTime;
            List<DateTimeSpan> dtmp = new List<DateTimeSpan>();
            DateTimeSpan ddt = new DateTimeSpan() { Start = startTime, End = endTime };
            foreach(var vv in mTimeOffsets)
            {
                DateTimeSpan dts = new DateTimeSpan() { Start = vv.Key, End = vv.Value.Item3 };
                dts = dts.Cross(ddt);
                if (!dts.IsEmpty())
                    dtmp.Add(dts);
            }

            foreach (var vv in dtmp)
            {
                if (vv.Start > stime)
                {
                    re.Add(new Tuple<DateTime, DateTime>(stime, vv.Start));
                }
                stime = vv.End;
            }
            return re;
        }

        /// <summary>
        /// 判断某个时间点是否有数据
        /// </summary>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool HasValue(DateTime time)
        {
            foreach (var vv in mTimeOffsets)
            {
                if (vv.Key <= time && time < vv.Value.Item3)
                {
                    return true;
                }
            }
            return false;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class DateTimeSpan
    {
        /// <summary>
        /// 
        /// </summary>
        public DateTimeSpan Empty = new DateTimeSpan() { Start = DateTime.MinValue, End = DateTime.MinValue };

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
            if(etime<stime)
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
        /// <param name="time1"></param>
        /// <param name="time2"></param>
        /// <returns></returns>
        public DateTime Min(DateTime time1,DateTime time2)
        {
            return time1 <= time2 ? time1 : time2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="time1"></param>
        /// <param name="time2"></param>
        /// <returns></returns>
        public DateTime Max(DateTime time1,DateTime time2)
        {
            return time1 >= time2 ? time1 : time2;
        }

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
            re.OpenForReadOnly(file.FileName);
            return re;
        }

        #region 读取所有值
        

        /// <summary>
        /// 读取某时间段内的所有bool值
        /// </summary>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        public static void ReadAllValue<T>(this DataFileInfo file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            var vff = file.GetFileSeriser();
            //Dictionary<long, Tuple<DateTime, DateTime>> moffs = new Dictionary<long, Tuple<DateTime, DateTime>>();

            var offset = file.GetFileOffsets(startTime, endTime);

            foreach (var vv in offset)
            {
                DateTime stime = vv.Key > startTime ? vv.Key : startTime;
                DateTime etime = vv.Key + vv.Value.Item1 > endTime ? endTime : vv.Key + vv.Value.Item1;
                vff.ReadAllValue(vv.Value.Item2, tid, stime, etime, result);
            }
            vff.Close();
            //GeneratorTime(moffs, startTime, endTime, file);

            //foreach (var vf in moffs)
            //{
            //    vff.ReadAllValue(vf.Key, tid, vf.Value.Item1, vf.Value.Item2, result);
            //}
        }

        

        #endregion

        #region 读取指定时刻值

        

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="time"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object Read<T>(this DataFileInfo file, int tid, DateTime time, QueryValueMatchType type)
        {
            using (var vff = file.GetFileSeriser())
            {
                var offset = file.GetFileOffsets(time);
                return vff.Read<T>(offset, tid, time, type);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static HisQueryResult<T> Read<T>(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type)
        {
            HisQueryResult<T> re = new HisQueryResult<T>(times.Count);
            Read<T>(file, tid, times, type, re);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="file"></param>
        /// <param name="tid"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        public static void Read<T>(this DataFileInfo file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
        {
            using (var vff = file.GetFileSeriser())
            {
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
                    if(vf.Key>-1)
                    vff.Read<T>(vf.Key, tid, vf.Value, type, result);
                    else
                    {
                        foreach(var vv in vf.Value)
                        {
                            result.Add(default(T), vv, (byte)QualityConst.Null);
                        }
                    }
                }
            }
        }
        #endregion

        #region DataFileSeriser Read

        

        public static void Read<T>(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<T> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockValue<T>(vv.Key, vv.Value, timetick, type, res);
            }
            foreach (var vv in data)
            {
                vv.Key.Dispose();
            }
            data.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTime"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static object Read<T>(this DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            using (var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick))
            {
                return DeCompressDataBlockValue<T>(data, dataTime, timetick, type);
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
        public static void ReadAllValue<T>(this DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, startTime, endTime, out timetick);
            foreach (var vv in data)
            {
                DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result);
            }
            // System.Threading.Tasks.Parallel.ForEach(data, (vv) => { DeCompressDataBlockAllValue(vv.Key, vv.Value.Item1, vv.Value.Item2, timetick, result); });

            foreach (var vv in data)
            {
                vv.Key.Dispose();

            }
            data.Clear();
        }

        
        #endregion

        #region DeCompressData

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private static object DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        /// <param name="datatime"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private static void DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<T> result)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type, result);
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
        private static void DeCompressDataBlockAllValue<T>(MarshalMemoryBlock memory, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
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
        public static Dictionary<int, int> CheckBlockHeadCach(this DataFileSeriserbase datafile, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick,out long blockPointer,out DateTime time)
        {
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { + tagid1+tagid2+...+tagidn }+ {[tag1 block point1(8) + tag2 block point1+ tag3 block point1+...] + [tag1 block point2(8) + tag2 block point2+ tag3 block point2+...]....}
            var dataoffset = offset + 16;

            //读取时间
            time = datafile.ReadDateTime(dataoffset);
            dataoffset += 8;

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
                    var dtmp = new Dictionary<int, int>();
                    using (var dd = datafile.Read(dataoffset, dsize))
                    {
                        MarshalVarintCodeMemory vcm = new MarshalVarintCodeMemory(dd.StartMemory, dsize);
                        var ltmp = vcm.ToIntList();
                        //vcm.Dispose();

                        
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

                       
                    }
                    return dtmp;
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
        public static int ReadTagIndexInDataPointer(this DataFileSeriserbase datafile, int tid, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick,out long blockpointer,out DateTime time)
        {
            var hfile = datafile.CheckBlockHeadCach(offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer,out time);
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
        public static List<long> ReadTargetBlockAddress(this DataFileSeriserbase datafile, List<int> tid, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockpointer, out DateTime time)
        {
            var hfile = datafile.CheckBlockHeadCach(offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer,out time);
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
            DateTime time;
            long blockpointer = 0;

            var blockIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer,out time);
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
            DateTime time;
            var blockIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer,out time);
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
            DateTime time;
            var tagIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer,out time);
            
            Dictionary<long, MarshalMemoryBlock> rtmp = new Dictionary<long, MarshalMemoryBlock>();

            Dictionary<MarshalMemoryBlock, List<DateTime>> re = new Dictionary<MarshalMemoryBlock, List<DateTime>>();

            if (tagCount == 0) return re;

            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(0);
            //var startTime = time;
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
            DateTime time;
            var tagIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick,out blockpointer,out time);
            int blockcount = fileDuration * 60 / blockDuration;

            //读取文件开始时间
            var startTime = datafile.ReadDateTime(0);
            //var startTime = time;

            Dictionary<MarshalMemoryBlock, Tuple<DateTime, DateTime>> re = new Dictionary<MarshalMemoryBlock, Tuple<DateTime, DateTime>>();

            DateTime sstart = start;
            DateTime send = end;

            while (sstart < end)
            {
                var ttmp = Math.Round((sstart - startTime).TotalSeconds,3);
                var vv = blockDuration*60 - (ttmp %(blockDuration * 60));

                send = sstart.AddSeconds(vv);

                //send = (sstart  - new TimeSpan(0, 0, 0, sstart.Second, sstart.Millisecond)).AddMinutes(blockDuration);
                if (send > end)
                {
                    send = end;
                }
                int blockindex = (int)(ttmp / (blockDuration * 60));
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
                            re.Add(rmm, new Tuple<DateTime, DateTime>(sstart, send));
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
