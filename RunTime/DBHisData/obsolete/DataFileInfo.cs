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
using System.Diagnostics;
using System.Net.Http.Headers;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
     /*
     * ****文件结构****
     * 一个文件头 + 多个数据区组成 ， 一个数据区：数据区头+数据块指针区+数据块区
     * [] 表示重复的一个或多个内容
     * 
     HisData File Structor
     FileHead(84) + [HisDataRegion]

     FileHead: dataTime(8)(FileTime)+dataTime(8)(LastUpdateTime)+DataRegionCount(4)+DatabaseName(64)
 
     HisDataRegion Structor: RegionHead + DataBlockPoint Area + DataBlock Area

     RegionHead:          PreDataRegionPoint(8) + NextDataRegionPoint(8) + Datatime(8)+ tagcount(4)+ tagid sum(8)+file duration(4)+block duration(4)+Time tick duration(4)
     DataBlockPoint Area: [ID]+[block Point]
     [block point]:       [[tag1 point,tag2 point,....][tag1 point,tag2 point,...].....]   以时间单位对变量的数去区指针进行组织
     DataBlock Area:      [block size + data block]
    */
    /// <summary>
    /// 
    /// </summary>
    public class DataFileInfo
    {

        #region ... Variables  ...

        private SortedDictionary<DateTime, Tuple<TimeSpan, long, DateTime>> mTimeOffsets = new SortedDictionary<DateTime, Tuple<TimeSpan, long, DateTime>>();

        private bool mInited = false;

        private static object mLockObj = new object();

        private DateTime mLastTime;

        private long mLastProcessOffset = -1;

        /// <summary>
        /// 
        /// </summary>
        private int mRegionCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public DateTime LastTime
        {
            get
            {
                return mLastTime;
            }
        }


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
        public DateTime EndTime
        {
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
                GetFileLastUpdateTime();

                lock (DataFileManager.CurrentDateTime)
                {
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
        }

        /// <summary>
        /// 
        /// </summary>
        private void GetFileLastUpdateTime()
        {
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenForReadOnly(FileName);
                //读取最后一次更新时间
                mLastTime = ss.ReadDateTime(8);
                ss.Dispose();
                mInited = false;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public void Scan()
        {
            long offset = DataFileManager.FileHeadSize;
            DateTime time, tmp;
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenForReadOnly(FileName);

                //读取文件时间
                DateTime fileTime = ss.ReadDateTime(0);
                //读取最后一次更新时间
                mLastTime = ss.ReadDateTime(8);

                var rcount = ss.ReadInt(16);
                if(rcount!= mRegionCount)
                {
                    mRegionCount = rcount;
                }
                else
                {
                    //ss.Close();
                    ss.Dispose();
                    return;
                }
                try
                {
                    do
                    {
                        //读取数据区时间
                        time = ss.ReadDateTime(offset + 16);

                        if (time == DateTime.MinValue)
                        {
                            tmp = time;
                            break;
                        }

                        long oset = offset;
                        //读取下个区域位置
                        offset = ss.ReadLong(offset + 8);

                        if (offset > 0)
                        {
                            var dt2 = ss.ReadDateTime(offset + 16);
                            mTimeOffsets.Add(time, new Tuple<TimeSpan, long, DateTime>(dt2 - time, oset, dt2));
                            tmp = dt2;
                        }
                        else
                        {
                            var tspan = StartTime + Duration - time;
                            if (tspan.TotalMilliseconds > 0)
                                mTimeOffsets.Add(time, new Tuple<TimeSpan, long, DateTime>(tspan, oset, time + tspan));
                            tmp = time + tspan;
                        }
                        mLastProcessOffset = oset;
                    }
                    while (offset > 0);

                    if (mLastTime <= fileTime)
                    {
                        mLastTime = tmp;
                    }
                    //ss.Close();
                    ss.Dispose();
                }
                catch
                {

                }

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
        public Dictionary<DateTime, Tuple<TimeSpan, long, DateTime>> GetFileOffsets(DateTime startTime, DateTime endTime)
        {
            lock (mLockObj)
                if (!mInited) Scan();
            Dictionary<DateTime, Tuple<TimeSpan, long, DateTime>> re = new Dictionary<DateTime, Tuple<TimeSpan, long, DateTime>>();
            foreach (var vv in mTimeOffsets)
            {
                //if (vv.Key >= startTime && vv.Key < endTime)
                if ((startTime >= vv.Key && startTime < vv.Key + vv.Value.Item1) || (endTime >= vv.Key && endTime < vv.Key + vv.Value.Item1) || (vv.Key >= startTime && (vv.Key + vv.Value.Item1) <= endTime))
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
        public List<Tuple<DateTime, DateTime>> GetNoValueOffsets(DateTime startTime, DateTime endTime)
        {
            List<Tuple<DateTime, DateTime>> re = new List<Tuple<DateTime, DateTime>>();
            DateTime stime = startTime;
            List<DateTimeSpan> dtmp = new List<DateTimeSpan>();
            DateTimeSpan ddt = new DateTimeSpan() { Start = startTime, End = endTime };
            foreach (var vv in mTimeOffsets)
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
            //long ltmp = 0, ltmp1 = 0;
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            var offset = file.GetFileOffsets(startTime, endTime);

            var vff = file.GetFileSeriser();
            //ltmp = sw.ElapsedMilliseconds;

            foreach (var vv in offset)
            {
                DateTime stime = vv.Key > startTime ? vv.Key : startTime;
                DateTime etime = vv.Key + vv.Value.Item1 > endTime ? endTime : vv.Key + vv.Value.Item1;
                vff.ReadAllValue(vv.Value.Item2, tid, stime, etime, result);
            }

            //ltmp1 = sw.ElapsedMilliseconds;
            //vff.Close();
            Task.Run(() => { vff.Dispose(); });

            //sw.Stop();
            //Debug.WriteLine("ReadAllValue:" + ltmp + " ," + (ltmp1 - ltmp) + "," + (sw.ElapsedMilliseconds - ltmp1));
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
                    if (vf.Key > -1)
                        vff.Read<T>(vf.Key, tid, vf.Value, type, result);
                    else
                    {
                        foreach (var vv in vf.Value)
                        {
                            result.Add(default(T), vv, (byte)QualityConst.Null);
                        }
                    }
                }
            }
        }
        #endregion

        #region DataFileSeriser Read


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tid"></param>
        /// <param name="dataTimes"></param>
        /// <param name="type"></param>
        /// <param name="res"></param>
        public static void Read<T>(this DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<T> res)
        {
            int timetick = 0;
            var data = datafile.ReadTagDataBlock2(tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                var index = vv.Value.Item2;
                DeCompressDataBlockValue<T>(vv.Key, vv.Value.Item1, timetick, type, res, new Func<byte, Dictionary<string, object>, object>((tp,ctx) => {

                    object oval = null;
                    int ttick = 0;
                    int dindex = index;
                    if (tp == 0)
                    {
                        //往前读最后一个有效值
                        do
                        {
                            dindex--;
                            if (dindex < 0) return TagHisValue<T>.Empty;
                            var datas = datafile.ReadTagDataBlock(tid, offset, dindex, out ttick);
                            if (datas == null) return null;
                            oval = DeCompressDataBlockRawValue<T>(datas, 0);
                        }
                        while (oval == null);
                    }
                    else
                    {
                        //往后读第一个有效值
                        do
                        {
                            dindex++;
                            
                            if (dindex > 47) 
                                return TagHisValue<T>.Empty;

                            var datas = datafile.ReadTagDataBlock(tid, offset, dindex, out ttick);
                            if (datas == null) return null;
                            oval = DeCompressDataBlockRawValue<T>(datas, 1);
                        }
                        while (oval == null);
                    }
                    return oval;

                }));
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
            int index = 0;
            using (var data = datafile.ReadTagDataBlock(tid, offset, dataTime, out timetick, out index))
            {
                return DeCompressDataBlockValue<T>(data, dataTime, timetick, type, new Func<byte, Dictionary<string, object>, object>((tp,ctx) => {
                    TagHisValue<T> oval = TagHisValue<T>.Empty;
                    int ttick = 0;
                    int dindex = index;
                    if (tp == 0)
                    {
                        //往前读最后一个有效值
                        do
                        {
                            dindex--;
                            if (dindex < 0) return TagHisValue<T>.Empty;
                            var datas = datafile.ReadTagDataBlock(tid, offset, dindex, out ttick);
                            if (datas == null) return null;
                            oval = DeCompressDataBlockRawValue<T>(datas, 0);
                        }
                        while (oval.IsEmpty());
                    }
                    else
                    {
                        //往后读第一个有效值
                        do
                        {
                            dindex++;
                            var datas = datafile.ReadTagDataBlock(tid, offset, dindex, out ttick);
                            if (datas == null) return null;
                            oval = DeCompressDataBlockRawValue<T>(datas, 1);
                        }
                        while (oval.IsEmpty());
                    }
                    return oval;
                }));
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
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            foreach (var vv in datafile.ReadTagDataBlock2(tid, offset, startTime, endTime))
            {

                DeCompressDataBlockAllValue(vv.Item1, vv.Item2, vv.Item3, vv.Item4, result);
                vv.Item1.Dispose();
            }
            //sw.Stop();
            //Debug.WriteLine("Read all value:" + sw.ElapsedMilliseconds + " file:" + datafile.FileName);
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
        private static object DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type,Func<byte,Dictionary<string,object>,object> ReadOtherDatablockAction)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                return tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type,ReadOtherDatablockAction,null);
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
        private static void DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte, Dictionary<string, object>, object> ReadOtherDatablockAction)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type, result, ReadOtherDatablockAction,null);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="readValueType"></param>
        /// <returns></returns>
        private static TagHisValue<T> DeCompressDataBlockRawValue<T>(MarshalMemoryBlock memory,byte readValueType)
        {
            var ctype = memory.ReadByte();
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
               return  tp.DeCompressRawValue<T>(memory,1 ,readValueType,null);
            }
            return TagHisValue<T>.Empty;
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
        public static Dictionary<int, int> CheckBlockHeadCach(this DataFileSeriserbase datafile, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockPointer, out DateTime time)
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
                        TagHeadOffsetManager.manager.Add(idsum, count, dtmp, blockPointer);

                        dd.Dispose();
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
        public static int ReadTagIndexInDataPointer(this DataFileSeriserbase datafile, int tid, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockpointer, out DateTime time)
        {
            var hfile = datafile.CheckBlockHeadCach(offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
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
            var hfile = datafile.CheckBlockHeadCach(offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
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
        public static MarshalMemoryBlock ReadTagDataBlock(this DataFileSeriserbase datafile, int tid, long offset, DateTime dataTime, out int timetick,out int index)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            DateTime time;
            long blockpointer = 0;

            var dindex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(16);
            var ttmp = (dataTime - startTime).TotalMinutes;
            int blockIndex = (int)(ttmp / blockDuration);
            if (ttmp % blockDuration > 0)
            {
                blockIndex++;
            }

            if (blockIndex > blockcount)
            {
                throw new Exception("DataPointer index is out of total block number");
            }
            index = blockIndex;

            var headdata = GetHeadBlock(datafile, offset + blockpointer, tagCount * blockcount * 12);

            ////var dataPointer = datafile.ReadLong(blockIndex * 8 + dindex * tagCount * 8); //读取DataBlock的地址
            //var dataPointer = datafile.ReadInt(offset + blockpointer + dindex * 12 + blockIndex * tagCount * 12); //读取DataBlock的相对地址
            //var dataPointerbase = datafile.ReadLong(offset + blockpointer + dindex * 12 + blockIndex * tagCount * 12 + 4); //读取DataBlock的基地址

            var dataPointer = headdata.ReadInt(dindex * 12 + blockIndex * tagCount * 12); //读取DataBlock的相对地址
            var dataPointerbase = headdata.ReadLong(dindex * 12 + blockIndex * tagCount * 12 + 4); //读取DataBlock的基地址


            var vmm = GetDataMemory(datafile, dataPointerbase, dataPointer);
            return vmm;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        public static MarshalMemoryBlock ReadTagDataBlock(this DataFileSeriserbase datafile,int tid,long offset,int index, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            DateTime time;
            long blockpointer = 0;

            var dindex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            int blockIndex = index;

            int blockcount = fileDuration * 60 / blockDuration;
            var headdata = GetHeadBlock(datafile, offset + blockpointer, tagCount * blockcount * 12);

            //var dataPointer = datafile.ReadLong(blockIndex * 8 + dindex * tagCount * 8); //读取DataBlock的地址

            //var dataPointer = datafile.ReadInt(offset + blockpointer + dindex * 12 + blockIndex * tagCount * 12); //读取DataBlock的相对地址
            //var dataPointerbase = datafile.ReadLong(offset + blockpointer + dindex * 12 + blockIndex * tagCount * 12 + 4); //读取DataBlock的基地址

            var dataPointer = headdata.ReadInt(dindex * 12 + blockIndex * tagCount * 12); //读取DataBlock的相对地址
            var dataPointerbase = headdata.ReadLong(dindex * 12 + blockIndex * tagCount * 12 + 4); //读取DataBlock的基地址


            if (dataPointerbase > 0)
            {
                var vmm = GetDataMemory(datafile, dataPointerbase, dataPointer);
                return vmm;
            }
            else
            {
                return null;
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="datafile"></param>
        ///// <param name="tid"></param>
        ///// <param name="offset"></param>
        ///// <param name="dataTimes"></param>
        ///// <param name="timetick"></param>
        ///// <returns></returns>
        //public static Dictionary<DateTime, MarshalMemoryBlock> ReadTagDataBlock(this DataFileSeriserbase datafile, int tid, long offset, List<DateTime> dataTimes, out int timetick)
        //{
        //    int fileDuration, blockDuration = 0;
        //    int tagCount = 0;
        //    long blockpointer = 0;
        //    DateTime time;
        //    var blockIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
        //    int blockcount = fileDuration * 60 / blockDuration;

        //    var startTime = datafile.ReadDateTime(16);

        //    Dictionary<long, MarshalMemoryBlock> rtmp = new Dictionary<long, MarshalMemoryBlock>();

        //    Dictionary<DateTime, MarshalMemoryBlock> re = new Dictionary<DateTime, MarshalMemoryBlock>();

        //    foreach (var vdd in dataTimes)
        //    {
        //        var ttmp = (vdd - startTime).TotalMinutes;
        //        int dindex = (int)(ttmp / blockDuration);
        //        if (ttmp % blockDuration > 0)
        //        {
        //            dindex++;
        //        }

        //        if (dindex > blockcount)
        //        {
        //            throw new Exception("DataPointer index is out of total block number");
        //        }

        //        var dataPointer = datafile.ReadInt(offset + blockpointer + dindex * 12 + blockIndex * tagCount * 12); //读取DataBlock的相对地址
        //        var dataPointerbase = datafile.ReadLong(offset + blockpointer + dindex * 12 + blockIndex * tagCount * 12 + 4); //读取DataBlock的基地址


        //        //var dataPointer = datafile.ReadLong(blockIndex * 8 + dindex * tagCount * 8); //读取DataBlock的地址

        //        //var datasize = datafile.ReadInt(dataPointer); //读取DataBlock 的大小

        //        var vmm = GetDataMomeoy(datafile, dataPointerbase);
        //        var datasize = vmm.ReadInt(dataPointer);


        //        if (!rtmp.ContainsKey(dataPointer))
        //        {
        //            var rmm = datafile.Read(dataPointer + 4, datasize);
        //            re.Add(vdd, rmm);
        //            rtmp.Add(dataPointer, rmm);
        //        }
        //        else
        //        {
        //            re.Add(vdd, rtmp[dataPointer]);
        //        }
        //    }
        //    return re;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="tid"></param>
        /// <param name="offset"></param>
        /// <param name="dataTimes"></param>
        /// <param name="timetick"></param>
        /// <returns></returns>
        public static Dictionary<MarshalMemoryBlock, Tuple<List<DateTime>, int>> ReadTagDataBlock2(this DataFileSeriserbase datafile, int tid, long offset, List<DateTime> dataTimes, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            long blockpointer = 0;
            DateTime time;
            var tagIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);

            Dictionary<long, MarshalMemoryBlock> rtmp = new Dictionary<long, MarshalMemoryBlock>();

            Dictionary<MarshalMemoryBlock, Tuple<List<DateTime>,int>> re = new Dictionary<MarshalMemoryBlock, Tuple<List<DateTime>, int>>();

            if (tagCount == 0) return re;

            int blockcount = fileDuration * 60 / blockDuration;

            var startTime = datafile.ReadDateTime(0);

            var headdata = GetHeadBlock(datafile, offset + blockpointer, tagCount * blockcount * 12);

            foreach (var vdd in dataTimes)
            {
                var ttmp = (vdd - startTime).TotalMinutes;
                int blockindex = (int)(ttmp / blockDuration);

                if (blockindex > blockcount)
                {
                    throw new Exception("DataPointer index is out of total block number");
                }

                //var dataPointer = datafile.ReadInt(offset + blockpointer + tagIndex * 12 + blockindex * tagCount * 12); //读取DataBlock的相对地址
                //var dataPointerbase = datafile.ReadLong(offset + blockpointer + tagIndex * 12 + blockindex * tagCount * 12 + 4); //读取DataBlock的基地址

                var dataPointer = headdata.ReadInt(tagIndex * 12 + blockindex * tagCount * 12); //读取DataBlock的相对地址
                var dataPointerbase = headdata.ReadLong(tagIndex * 12 + blockindex * tagCount * 12 + 4); //读取DataBlock的基地址

                if (dataPointerbase > 0)
                {
                    //var datasize = datafile.ReadInt(dataPointer); //读取DataBlock 的大小
                    var vmm = GetDataMemory(datafile, dataPointerbase, dataPointer);

                    if (vmm !=null)
                    {
                        if (!rtmp.ContainsKey(dataPointer))
                        {
                            //var rmm = datafile.Read(dataPointer + 4, datasize);
                            if (!re.ContainsKey(vmm))
                            {
                                re.Add(vmm, new Tuple<List<DateTime>, int>(new List<DateTime>() { vdd },blockindex));
                            }
                            else
                            {
                                re[vmm].Item1.Add(vdd);
                            }
                            rtmp.Add(dataPointer, vmm);
                        }
                        else
                        {
                            //var rmm = rtmp[dataPointer];
                            if (!re.ContainsKey(vmm))
                            {
                                re.Add(vmm, new Tuple<List<DateTime>, int>(new List<DateTime>() { vdd },blockindex));
                            }
                            else
                            {
                                re[vmm].Item1.Add(vdd);
                            }
                        }
                    }

                }

            }
            return re;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        //static Dictionary<string, MarshalMemoryBlock> mMemoryCach = new Dictionary<string, MarshalMemoryBlock>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="address"></param>
        /// <returns></returns>
        private static MarshalMemoryBlock GetDataMemory(DataFileSeriserbase datafile,long address,int datapointer)
        {
            if (datapointer < 0)
            {
                //说明数据没有采用Zip压缩，可以直接读取使用
                var dp = address +  (datapointer & 0x7FFFFFFF);
                var datasize = datafile.ReadInt(dp);
                return datafile.Read(dp + 4, datasize);
            }
            else
            {
                return DecodeMemoryCachManager.Manager.GetMemory(datafile, address, datapointer);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="address"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        private static MarshalMemoryBlock GetHeadBlock(DataFileSeriserbase datafile,long address,int len)
        {
            return HeadPointDataCachManager.Manager.GetMemory(datafile,address, len);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="datafile"></param>
        ///// <param name="address"></param>
        //private static unsafe MarshalMemoryBlock DecompressMemory(DataFileSeriserbase datafile,long address)
        //{
        //    Stopwatch sw = new Stopwatch();
        //    sw.Start();
        //    var vsize = datafile.ReadInt(address);
        //    var dsize = datafile.ReadInt(address + 4);
        //    var datas = datafile.Read(address + 8, vsize);
        //    int dtmp = 0;
        //    MarshalMemoryBlock mmb = new MarshalMemoryBlock(dsize, dsize);

        //    long ltmp = sw.ElapsedMilliseconds;

        //    System.IO.Compression.BrotliDecoder.TryDecompress(new ReadOnlySpan<byte>((void*)datas.Handles[0], vsize), new Span<byte>((void*)mmb.Handles[0], dsize), out dtmp);

        //    if(dtmp!=dsize)
        //    {
        //        LoggerService.Service.Warn("DataFileInfo", "解压缩数据长度不一致!"+datafile.FileName);
        //    }

        //    long ltmp2 = sw.ElapsedMilliseconds;

        //    datas.Dispose();
        //    sw.Stop();

        //    Debug.Print("查询耗时 构建对象耗时:" + ltmp + ",解压耗时:" + (ltmp2 - ltmp)+", Dispose 耗时："+(sw.ElapsedMilliseconds-ltmp2));

        //    return mmb;
        //}

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
        public static IEnumerable<Tuple<MarshalMemoryBlock,DateTime, DateTime,int>> ReadTagDataBlock2(this DataFileSeriserbase datafile, int tid, long offset, DateTime start, DateTime end)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            long blockpointer = 0;
            int timetick = 0;
            DateTime time;

            var tagIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            int blockcount = fileDuration * 60 / blockDuration;

            //读取文件开始时间
            var startTime = datafile.ReadDateTime(0);

            DateTime sstart = start;
            DateTime send = end;

            var headdata = GetHeadBlock(datafile, offset + blockpointer, tagCount * blockcount * 12);

            while (sstart < end)
            {
                var ttmp = Math.Round((sstart - startTime).TotalSeconds, 3);
                var vv = blockDuration * 60 - (ttmp % (blockDuration * 60));
                send = sstart.AddSeconds(vv);

                if (send > end)
                {
                    send = end;
                }
                int blockindex = (int)(ttmp / (blockDuration * 60));
                
                if (blockindex >= blockcount)
                {
                    break;
                    //throw new Exception("DataPointer index is out of total block number");
                }

                var dataPointer = headdata.ReadInt(tagIndex * 12 + blockindex * tagCount * 12); //读取DataBlock的相对地址

                var dataPointerbase = headdata.ReadLong(tagIndex * 12 + blockindex * tagCount * 12 + 4); //读取DataBlock的基地址

                if (dataPointerbase > 0)
                {
                    var vmm = GetDataMemory(datafile, dataPointerbase, dataPointer);
                    if (vmm != null)
                        yield return new Tuple<MarshalMemoryBlock, DateTime, DateTime, int>(vmm, sstart, send, timetick);
                }
                sstart = send;
            }
        }
        #endregion
    }
}
