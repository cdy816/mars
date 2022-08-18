﻿//==============================================================
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
    * ****DBD 文件结构****
    * 一个文件头 + 多个数据区组成 ， 一个数据区：数据区头+数据块指针区+数据块区
    * [] 表示重复的一个或多个内容
    * 
    HisData File Structor
    FileHead(84) + [HisDataRegion]

    FileHead: dataTime(8)(FileTime)+dateTime(8)(LastUpdateTime)+DataRegionCount(4)+DatabaseName(64)

    HisDataRegion Structor: RegionHead + DataBlockPoint Area + DataBlock Area

    RegionHead:          PreDataRegion(8) + NextDataRegion(8) + Datatime(8)+file duration(4)+ block duration(4)+Time tick duration(4)  + tagcount(4)
    DataBlockPoint Area: [ID]+[block Point]
    [block point]:       [[tag1 block1 point,tag2 block1 point,....][tag1 block2 point(12),tag2 block2 point(12),...].....]   以时间单位对变量的数据区指针进行组织,
    [tag block point]:   offset pointer(4)+ datablock area point(8)   offset pointer: bit 32 标识data block 类型,1:标识非压缩区域，0:压缩区域,bit1~bit31 偏移地址
    DataBlock Area:      [[tag1 block1 size + compressType+ tag1 data block1][tag2 block1 size + compressType+ tag2 data block1]....][[tag1 block2 size + compressType+ tag1 data block2][tag2 block2 size + compressType+ tag2 data block2]....]....
    */

    /// <summary>
    /// 
    /// </summary>
    public class DataFileInfo4 : IDataFile
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

        /// <summary>
        /// 单个历史文件记录的变量个数
        /// </summary>
        public const int PageFileTagCount = 100000;

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
        /// 
        /// </summary>
        public string BackFileName { get; set; }

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

        /// <summary>
        /// 是否为压缩文件
        /// </summary>
        public bool IsZipFile { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void UpdateLastDatetime()
        {

            lock (mLockObj)
            {
                //对于压缩文件，说明很老的文件，不需要更新
                if (IsZipFile) return;

                //mTimeOffsets.Clear();
                GetFileLastUpdateTime();

                lock (DataFileManager.CurrentDateTime)
                {
                    if (DataFileManager.CurrentDateTime.ContainsKey(FId))
                    {
                        if (DataFileManager.CurrentDateTime[FId] < mLastTime)
                            DataFileManager.CurrentDateTime[FId] = mLastTime;
                    }
                    else
                    {
                        DataFileManager.CurrentDateTime.Add(FId, mLastTime);
                    }
                }

                HeadPointDataCachManager.Manager.ClearMemoryCach(this.FileName);
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
            DateTime time, tmp=DateTime.MinValue;
            using (var ss = DataFileSeriserManager.manager.GetDefaultFileSersie())
            {
                ss.OpenForReadOnly(FileName);

                //读取文件时间
                DateTime fileTime = ss.ReadDateTime(0);
                //读取最后一次更新时间
                mLastTime = ss.ReadDateTime(8);

                var rcount = ss.ReadInt(16);

                if (rcount != mRegionCount)
                {
                    mRegionCount = rcount;
                    mTimeOffsets.Clear();
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
                            if (!mTimeOffsets.ContainsKey(time))
                            {
                                var vtmps = (dt2 - time).TotalDays;
                                if (vtmps <= 30 && vtmps >= 0)
                                {
                                    mTimeOffsets.Add(time, new Tuple<TimeSpan, long, DateTime>(dt2 - time, oset, dt2));
                                }
                                else if(dt2 ==DateTime.MinValue)
                                {
                                    var tspan = StartTime + Duration - time;
                                    if (tspan.TotalMilliseconds >= 0 && !mTimeOffsets.ContainsKey(time))
                                        mTimeOffsets.Add(time, new Tuple<TimeSpan, long, DateTime>(tspan, oset, time + tspan));
                                }
                                else
                                {
                                    LoggerService.Service.Warn("DataFileInfo", this.FileName + ": " + offset + " " + vtmps + "  遇到无效数据!");
                                    //Debug.Print(this.FileName + ": " + offset + " " + vtmps+"  无效数据!");
                                }
                                tmp = dt2;
                            }
                            else
                            {
                                tmp = dt2;
                                break;
                            }
                            
                        }
                        else
                        {
                            var tspan = StartTime + Duration - time;
                            if (tspan.TotalMilliseconds >= 0 && !mTimeOffsets.ContainsKey(time))
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
                try
                {
                    //if (vv.Key >= startTime && vv.Key < endTime)
                    if ((startTime >= vv.Key && startTime < vv.Key + vv.Value.Item1) || (endTime >= vv.Key && endTime < vv.Key + vv.Value.Item1) || (vv.Key >= startTime && (vv.Key + vv.Value.Item1) <= endTime))
                    {
                        re.Add(vv.Key, vv.Value);
                    }
                }
                catch
                {

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




    public static class DataFileInfo4Extend
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static DataFileSeriserbase GetFileSeriser(this DataFileInfo4 file)
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
        public static void ReadAllValue<T>(this DataFileInfo4 file, int tid, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
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
                ReadAllValue(vff,vv.Value.Item2, tid, stime, etime, result);
            }

            //ltmp1 = sw.ElapsedMilliseconds;
            //vff.Close();
            vff.Dispose();

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
        public static object Read<T>(this DataFileInfo4 file, int tid, DateTime time, QueryValueMatchType type)
        {
            using (var vff = file.GetFileSeriser())
            {
                var offset = file.GetFileOffsets(time);
                return Read<T>(vff,offset, tid, time, type);
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
        public static HisQueryResult<T> Read<T>(this DataFileInfo4 file, int tid, List<DateTime> times, QueryValueMatchType type)
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
        public static void Read<T>(this DataFileInfo4 file, int tid, List<DateTime> times, QueryValueMatchType type, HisQueryResult<T> result)
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
                        Read<T>(vff, vf.Key, tid, vf.Value, type, result);
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
        public static void Read<T>(DataFileSeriserbase datafile, long offset, int tid, List<DateTime> dataTimes, QueryValueMatchType type, HisQueryResult<T> res)
        {
            int timetick = 0;
            var data = ReadTagDataBlock2(datafile,tid, offset, dataTimes, out timetick);
            foreach (var vv in data)
            {
                var index = vv.Value.Item2;
                
                DeCompressDataBlockValue<T>(vv.Key, vv.Value.Item1, timetick, type, res, new Func<byte,Dictionary<string,object>, object>((tp,ctx) => {

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
                            var datas = ReadTagDataBlock(datafile,tid, offset, dindex, out ttick);
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

                            var datas = ReadTagDataBlock(datafile,tid, offset, dindex, out ttick);
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
        public static object Read<T>(DataFileSeriserbase datafile, long offset, int tid, DateTime dataTime, QueryValueMatchType type)
        {
            int timetick = 0;
            int index = 0;
            using (var data = ReadTagDataBlock(datafile,tid, offset, dataTime, out timetick, out index))
            {
                return DeCompressDataBlockValue<T>(data, dataTime, timetick, type, new Func<byte,Dictionary<string,object>, object>((tp,ctx) => {
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
                            var datas = ReadTagDataBlock(datafile,tid, offset, dindex, out ttick);
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
                            var datas = ReadTagDataBlock(datafile,tid, offset, dindex, out ttick);
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
        public static void ReadAllValue<T>(DataFileSeriserbase datafile, long offset, int tid, DateTime startTime, DateTime endTime, HisQueryResult<T> result)
        {
            //Stopwatch sw = new Stopwatch();
            //sw.Start();
            foreach (var vv in ReadTagDataBlock2(datafile,tid, offset, startTime, endTime))
            {
                if (vv != null)
                {
                    DeCompressDataBlockAllValue(vv.Item1, vv.Item2, vv.Item3, vv.Item4, result);
                    vv.Item1.Dispose();
                }
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
        private static object DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, DateTime datatime, int timeTick, QueryValueMatchType type,Func<byte,QueryContext,object> ReadOtherDatablockAction)
        {
            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            ctype = GetCompressType(ctype, out byte tagtype);
            QueryContext ctx = new QueryContext();
            ctx.Add("hasnext", false);
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                if (!CheckTagTypeChanged<T>(tagtype))
                {
                    return tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction,ctx);
                }
                else
                {
                    //如果记录的类型发生了改变，则需要转换
                    TagType tpp = (TagType)ctype;
                    switch (tpp)
                    {
                        case TagType.Bool:
                            return tp.DeCompressValue<bool>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.Byte:
                            return tp.DeCompressValue<byte>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.Short:
                            return tp.DeCompressValue<short>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.UShort:
                            return tp.DeCompressValue<ushort>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.Int:
                            return tp.DeCompressValue<int>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.UInt:
                            return tp.DeCompressValue<uint>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.Long:
                            return tp.DeCompressValue<long>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.ULong:
                            return tp.DeCompressValue<ulong>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.DateTime:
                            return tp.DeCompressValue<DateTime>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.Float:
                            return tp.DeCompressValue<float>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.Double:
                            return tp.DeCompressValue<double>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.String:
                            return tp.DeCompressValue<string>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.IntPoint:
                            return tp.DeCompressValue<IntPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.IntPoint3:
                            return tp.DeCompressValue<IntPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.UIntPoint:
                            return tp.DeCompressValue<UIntPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.UIntPoint3:
                            return tp.DeCompressValue<UIntPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.LongPoint:
                            return tp.DeCompressValue<LongPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.ULongPoint:
                            return tp.DeCompressValue<ULongPointData>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.LongPoint3:
                            return tp.DeCompressValue<LongPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                        case TagType.ULongPoint3:
                            return tp.DeCompressValue<ULongPoint3Data>(memory, 1, datatime, timeTick, type, ReadOtherDatablockAction, ctx);
                    }
                }
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
        private static void DeCompressDataBlockValue<T>(MarshalMemoryBlock memory, List<DateTime> datatime, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte, QueryContext, object> ReadOtherDatablockAction)
        {
            QueryContext ctx = new QueryContext();
            ctx.Add("hasnext", false);

            //MarshalMemoryBlock target = new MarshalMemoryBlock(memory.Length);
            //读取压缩类型
            var ctype = memory.ReadByte();
            ctype = GetCompressType(ctype, out byte tagtype);
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                if (!CheckTagTypeChanged<T>(tagtype))
                {
                    tp.DeCompressValue<T>(memory, 1, datatime, timeTick, type, result, ReadOtherDatablockAction, ctx);
                }
                else
                {
                    DateTime time;
                    byte qu;
                    //如果记录的类型发生了改变，则需要转换
                    TagType tpp = (TagType)ctype;
                    switch (tpp)
                    {
                        case TagType.Bool:
                            var htmp = new HisQueryResult<bool>(600);
                            tp.DeCompressValue<bool>(memory, 1, datatime, timeTick, type, htmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < htmp.Count; i++)
                            {
                                var bval = htmp.GetTargetValue(htmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Byte:
                            var btmp = new HisQueryResult<byte>(600);
                            tp.DeCompressValue<byte>(memory, 1, datatime, timeTick, type, btmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < btmp.Count; i++)
                            {
                                var bval = btmp.GetTargetValue(btmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Short:
                            var stmp = new HisQueryResult<short>(600);
                            tp.DeCompressValue<short>(memory, 1, datatime, timeTick, type, stmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < stmp.Count; i++)
                            {
                                var bval = stmp.GetTargetValue(stmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UShort:
                            var ustmp = new HisQueryResult<ushort>(600);
                            tp.DeCompressValue<ushort>(memory, 1, datatime, timeTick, type, ustmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < ustmp.Count; i++)
                            {
                                var bval = ustmp.GetTargetValue(ustmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Int:
                            var itmp = new HisQueryResult<int>(600);
                            tp.DeCompressValue<int>(memory, 1, datatime, timeTick, type, itmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < itmp.Count; i++)
                            {
                                var bval = itmp.GetTargetValue(itmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UInt:
                            var uitmp = new HisQueryResult<uint>(600);
                            tp.DeCompressValue<uint>(memory, 1, datatime, timeTick, type, uitmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < uitmp.Count; i++)
                            {
                                var bval = uitmp.GetTargetValue(uitmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Long:
                            var ltmp = new HisQueryResult<long>(600);
                            tp.DeCompressValue<long>(memory, 1, datatime, timeTick, type, ltmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < ltmp.Count; i++)
                            {
                                var bval = ltmp.GetTargetValue(ltmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULong:
                            var ultmp = new HisQueryResult<ulong>(600);
                            tp.DeCompressValue<ulong>(memory, 1, datatime, timeTick, type, ultmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < ultmp.Count; i++)
                            {
                                var bval = ultmp.GetTargetValue(ultmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.DateTime:
                            var dttmp = new HisQueryResult<DateTime>(600);
                            tp.DeCompressValue<DateTime>(memory, 1, datatime, timeTick, type, dttmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < dttmp.Count; i++)
                            {
                                var bval = dttmp.GetTargetValue(dttmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Float:
                            var ftmp = new HisQueryResult<float>(600);
                            tp.DeCompressValue<float>(memory, 1, datatime, timeTick, type, ftmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < ftmp.Count; i++)
                            {
                                var bval = ftmp.GetTargetValue(ftmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Double:
                            var dtmp = new HisQueryResult<double>(600);
                            tp.DeCompressValue<double>(memory, 1, datatime, timeTick, type, dtmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < dtmp.Count; i++)
                            {
                                var bval = dtmp.GetTargetValue(dtmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.String:
                            var sstmp = new HisQueryResult<string>(600);
                            tp.DeCompressValue<string>(memory, 1, datatime, timeTick, type, sstmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < sstmp.Count; i++)
                            {
                                var bval = sstmp.GetTargetValue(sstmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint:
                            var iptmp = new HisQueryResult<IntPointData>(600);
                            tp.DeCompressValue<IntPointData>(memory, 1, datatime, timeTick, type, iptmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < iptmp.Count; i++)
                            {
                                var bval = iptmp.GetTargetValue(iptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint3:
                            var ip3tmp = new HisQueryResult<IntPoint3Data>(600);
                            tp.DeCompressValue<IntPoint3Data>(memory, 1, datatime, timeTick, type, ip3tmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < ip3tmp.Count; i++)
                            {
                                var bval = ip3tmp.GetTargetValue(ip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint:
                            var uptmp = new HisQueryResult<UIntPointData>(600);
                            tp.DeCompressValue<UIntPointData>(memory, 1, datatime, timeTick, type, uptmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < uptmp.Count; i++)
                            {
                                var bval = uptmp.GetTargetValue(uptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint3:
                            var uip3tmp = new HisQueryResult<UIntPoint3Data>(600);
                            tp.DeCompressValue<UIntPoint3Data>(memory, 1, datatime, timeTick, type, uip3tmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < uip3tmp.Count; i++)
                            {
                                var bval = uip3tmp.GetTargetValue(uip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint:
                            var liptmp = new HisQueryResult<LongPointData>(600);
                            tp.DeCompressValue<LongPointData>(memory, 1, datatime, timeTick, type, liptmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < liptmp.Count; i++)
                            {
                                var bval = liptmp.GetTargetValue(liptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint:
                            var uliptmp = new HisQueryResult<ULongPointData>(600);
                            tp.DeCompressValue<ULongPointData>(memory, 1, datatime, timeTick, type, uliptmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < uliptmp.Count; i++)
                            {
                                var bval = uliptmp.GetTargetValue(uliptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint3:
                            var lip3tmp = new HisQueryResult<LongPoint3Data>(600);
                            tp.DeCompressValue<LongPoint3Data>(memory, 1, datatime, timeTick, type, lip3tmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < lip3tmp.Count; i++)
                            {
                                var bval = lip3tmp.GetTargetValue(lip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint3:
                            var ulip3tmp = new HisQueryResult<ULongPoint3Data>(600);
                            tp.DeCompressValue<ULongPoint3Data>(memory, 1, datatime, timeTick, type, ulip3tmp, ReadOtherDatablockAction, ctx);
                            for (int i = 0; i < ulip3tmp.Count; i++)
                            {
                                var bval = ulip3tmp.GetTargetValue(ulip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                    }
                }
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
            ctype = GetCompressType(ctype, out byte tagtype);
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                if (!CheckTagTypeChanged<T>(tagtype))
                {
                    return tp.DeCompressRawValue<T>(memory, 1, readValueType,null);
                }
                else
                {
                    //DateTime time;
                    //byte qu;
                    ////如果记录的类型发生了改变，则需要转换
                    //TagType tpp = (TagType)ctype;
                    //switch (tpp)
                    //{
                    //    case TagType.Bool:
                           
                    //        return tp.DeCompressRawValue<bool>(memory, 1, readValueType);
                            
                    //    case TagType.Byte:
                           
                    //        res = tp.DeCompressRawValue<byte>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.Short:
                           
                    //        res = tp.DeCompressRawValue<short>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.UShort:
                           
                    //        res = tp.DeCompressRawValue<ushort>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.Int:
                          
                    //        res = tp.DeCompressRawValue<int>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.UInt:
                         
                    //        res = tp.DeCompressRawValue<uint>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.Long:
                          
                    //        res = tp.DeCompressRawValue<long>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.ULong:
                       
                    //        res = tp.DeCompressRawValue<ulong>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.DateTime:
                            
                    //        res = tp.DeCompressRawValue<DateTime>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.Float:
                            
                    //        res = tp.DeCompressRawValue<float>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.Double:
                            
                    //        res = tp.DeCompressRawValue<double>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.String:
                            
                    //        res = tp.DeCompressRawValue<string>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.IntPoint:
                          
                    //        res = tp.DeCompressRawValue<IntPointData>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.IntPoint3:
                           
                    //        res = tp.DeCompressRawValue<IntPoint3Data>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.UIntPoint:
                            
                    //        res = tp.DeCompressRawValue<UIntPointData>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.UIntPoint3:
                            
                    //        res = tp.DeCompressRawValue<UIntPoint3Data>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.LongPoint:
                          
                    //        res = tp.DeCompressRawValue<LongPointData>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.ULongPoint:
                            
                    //        res = tp.DeCompressRawValue<bool>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.LongPoint3:
                          
                    //        res = tp.DeCompressRawValue<bool>(memory, 1, readValueType);
                    //        break;
                    //    case TagType.ULongPoint3:
                          
                    //        res = tp.DeCompressRawValue<bool>(memory, 1, readValueType);
                    //        break;
                    //}
                }
            }
            return TagHisValue<T>.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        /// <param name="tagType"></param>
        /// <returns></returns>
        private static byte GetCompressType(byte val,out byte tagType)
        {
            tagType =  (byte)((val >> 3)-1);
            return (byte)(val & 0x03);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tagtype"></param>
        /// <returns></returns>
        private static bool CheckTagTypeChanged<T>(byte tagtype)
        {
            if (tagtype == 255) return false;

            string sname = typeof(T).Name.ToLower();
            TagType tp = (TagType)tagtype;
            switch (sname)
            {
                case "bool":
                    return tp != TagType.Bool;
                case "byte":
                    return tp != TagType.Byte;
                case "short":
                    return tp != TagType.Short;
                case "ushort":
                    return tp != TagType.UShort;
                case "int":
                    return tp != TagType.Int;
                case "uint":
                    return tp != TagType.UInt;
                case "long":
                    return tp != TagType.Long;
                case "ulong":
                    return tp != TagType.ULong;
                case "double":
                    return tp != TagType.Double;
                case "float":
                    return tp != TagType.Float;
                case "datetime":
                    return tp != TagType.DateTime;
                case "string":
                    return tp != TagType.String;
                case "intpoint":
                    return tp != TagType.IntPoint;
                case "intpoint3":
                    return tp != TagType.IntPoint3;
                case "uintpoint":
                    return tp != TagType.UIntPoint;
                case "uintpoint3":
                    return tp != TagType.UIntPoint3;
                case "longpoint":
                    return tp != TagType.LongPoint;
                case "longpoint3":
                    return tp != TagType.LongPoint3;
                case "ulongpoint":
                    return tp != TagType.ULongPoint;
                case "ulongpoint3":
                    return tp != TagType.ULongPoint3;
            }

            return false;
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
            ctype = GetCompressType(ctype, out byte tagtype);
            var tp = CompressUnitManager2.Manager.GetCompress(ctype);
            if (tp != null)
            {
                //如果变量类型没有改变
                if (!CheckTagTypeChanged<T>(tagtype))
                {
                    tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, result);
                }
                else
                {
                    DateTime time;
                    byte qu;
                    //如果记录的类型发生了改变，则需要转换
                    TagType tpp = (TagType)ctype;
                    switch(tpp)
                    {
                        case TagType.Bool:
                            var htmp = new HisQueryResult<bool>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, htmp);
                            for(int i=0;i<htmp.Count;i++)
                            {
                                var bval = htmp.GetTargetValue(htmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Byte:
                            var btmp = new HisQueryResult<byte>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, btmp);
                            for (int i = 0; i < btmp.Count; i++)
                            {
                                var bval = btmp.GetTargetValue(btmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Short:
                            var stmp = new HisQueryResult<short>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, stmp);
                            for (int i = 0; i < stmp.Count; i++)
                            {
                                var bval = stmp.GetTargetValue(stmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UShort:
                            var ustmp = new HisQueryResult<ushort>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ustmp);
                            for (int i = 0; i < ustmp.Count; i++)
                            {
                                var bval = ustmp.GetTargetValue(ustmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Int:
                            var itmp = new HisQueryResult<int>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, itmp);
                            for (int i = 0; i < itmp.Count; i++)
                            {
                                var bval = itmp.GetTargetValue(itmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UInt:
                            var uitmp = new HisQueryResult<uint>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uitmp);
                            for (int i = 0; i < uitmp.Count; i++)
                            {
                                var bval = uitmp.GetTargetValue(uitmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Long:
                            var ltmp = new HisQueryResult<long>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ltmp);
                            for (int i = 0; i < ltmp.Count; i++)
                            {
                                var bval = ltmp.GetTargetValue(ltmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULong:
                            var ultmp = new HisQueryResult<ulong>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ultmp);
                            for (int i = 0; i < ultmp.Count; i++)
                            {
                                var bval = ultmp.GetTargetValue(ultmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.DateTime:
                            var dttmp = new HisQueryResult<DateTime>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, dttmp);
                            for (int i = 0; i < dttmp.Count; i++)
                            {
                                var bval = dttmp.GetTargetValue(dttmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Float:
                            var ftmp = new HisQueryResult<float>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ftmp);
                            for (int i = 0; i < ftmp.Count; i++)
                            {
                                var bval = ftmp.GetTargetValue(ftmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.Double:
                            var dtmp = new HisQueryResult<double>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, dtmp);
                            for (int i = 0; i < dtmp.Count; i++)
                            {
                                var bval = dtmp.GetTargetValue(dtmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.String:
                            var sstmp = new HisQueryResult<string>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, sstmp);
                            for (int i = 0; i < sstmp.Count; i++)
                            {
                                var bval = sstmp.GetTargetValue(sstmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint:
                            var iptmp = new HisQueryResult<IntPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, iptmp);
                            for (int i = 0; i < iptmp.Count; i++)
                            {
                                var bval = iptmp.GetTargetValue(iptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.IntPoint3:
                            var ip3tmp = new HisQueryResult<IntPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ip3tmp);
                            for (int i = 0; i < ip3tmp.Count; i++)
                            {
                                var bval = ip3tmp.GetTargetValue(ip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint:
                            var uptmp = new HisQueryResult<UIntPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uptmp);
                            for (int i = 0; i < uptmp.Count; i++)
                            {
                                var bval = uptmp.GetTargetValue(uptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.UIntPoint3:
                            var uip3tmp = new HisQueryResult<UIntPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uip3tmp);
                            for (int i = 0; i < uip3tmp.Count; i++)
                            {
                                var bval = uip3tmp.GetTargetValue(uip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint:
                            var liptmp = new HisQueryResult<LongPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, liptmp);
                            for (int i = 0; i < liptmp.Count; i++)
                            {
                                var bval = liptmp.GetTargetValue(liptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint:
                            var uliptmp = new HisQueryResult<ULongPointData>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, uliptmp);
                            for (int i = 0; i < uliptmp.Count; i++)
                            {
                                var bval = uliptmp.GetTargetValue(uliptmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.LongPoint3:
                            var lip3tmp = new HisQueryResult<LongPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, lip3tmp);
                            for (int i = 0; i < lip3tmp.Count; i++)
                            {
                                var bval = lip3tmp.GetTargetValue(lip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                        case TagType.ULongPoint3:
                            var ulip3tmp = new HisQueryResult<ULongPoint3Data>(600);
                            tp.DeCompressAllValue(memory, 1, startTime, endTime, timeTick, ulip3tmp);
                            for (int i = 0; i < ulip3tmp.Count; i++)
                            {
                                var bval = ulip3tmp.GetTargetValue(ulip3tmp.GetValue(i, out time, out qu));
                                result.Add(bval, time, qu);
                            }
                            break;
                    }
                }
            }
        }

        #endregion

        #region 读取数据区域头数据

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datafile"></param>
        /// <param name="offset"></param>
        /// <param name="tagCount"></param>
        /// <param name="fileDuration"></param>
        /// <param name="blockDuration"></param>
        /// <param name="timetick"></param>
        /// <param name="blockPointer"></param>
        /// <param name="time"></param>
        public static void ReadRegionHead(DataFileSeriserbase datafile, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockPointer, out DateTime time)
        {
            //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+file duration(4)+ block duration(4)+Time tick duration(4)+ tagcount(4)+{ + tagid1+tagid2+...+tagidn }+ {[tag1 block point1(8) + tag2 block point1+ tag3 block point1+...] + [tag1 block point2(8) + tag2 block point2+ tag3 block point2+...]....}
           
            var dataoffset = offset + 16;

            //读取时间
            time = datafile.ReadDateTime(dataoffset);
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

            //读取变量个数
            tagCount = datafile.ReadInt(dataoffset);
            dataoffset += 4;

            blockPointer = dataoffset - offset;
        }

        ///// <summary>
        ///// 检测数据头部指针区域数据是否被缓存
        ///// </summary>
        ///// <param name="datafile"></param>
        ///// <param name="offset"></param>
        ///// <param name="fileDuration"></param>
        ///// <param name="blockDuration"></param>
        ///// <param name="timetick"></param>
        ///// <returns></returns>
        //public static Dictionary<int, int> CheckBlockHeadCach(DataFileSeriserbase datafile, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockPointer, out DateTime time)
        //{
        //    //文件头部结构:Pre DataRegion(8) + Next DataRegion(8) + Datatime(8)+tagcount(4)+ tagid sum(8) +file duration(4)+ block duration(4)+Time tick duration(4)+ { + tagid1+tagid2+...+tagidn }+ {[tag1 block point1(8) + tag2 block point1+ tag3 block point1+...] + [tag1 block point2(8) + tag2 block point2+ tag3 block point2+...]....}
        //    var dataoffset = offset + 16;

        //    //读取时间
        //    time = datafile.ReadDateTime(dataoffset);
        //    dataoffset += 8;

        //    //读取变量个数
        //    int count = datafile.ReadInt(dataoffset);
        //    dataoffset += 4;

        //    tagCount = count;

        //    //读取校验和
        //    long idsum = datafile.ReadLong(dataoffset);
        //    dataoffset += 8;

        //    //读取单个文件的时长
        //    fileDuration = datafile.ReadInt(dataoffset);
        //    dataoffset += 4;
        //    //读取数据块时长
        //    blockDuration = datafile.ReadInt(dataoffset);
        //    dataoffset += 4;
        //    //读取时钟周期
        //    timetick = datafile.ReadInt(dataoffset);
        //    dataoffset += 4;

        //    lock (TagHeadOffsetManager.manager)
        //    {
        //        if (!TagHeadOffsetManager.manager.Contains(idsum, count))
        //        {
        //            //Tag id 列表经过压缩，内容格式为:DataSize + Data
        //            var dsize = datafile.ReadInt(dataoffset);

        //            if (dsize <= 0)
        //            {
        //                tagCount = 0;
        //                fileDuration = 0;
        //                blockDuration = 0;
        //                timetick = 0;
        //                blockPointer = 0;
        //                return new Dictionary<int, int>();
        //            }

        //            dataoffset += 4;

        //            blockPointer = dataoffset + dsize - offset;
        //            var dtmp = new Dictionary<int, int>();
        //            using (var dd = datafile.Read(dataoffset, dsize))
        //            {
        //                MarshalVarintCodeMemory vcm = new MarshalVarintCodeMemory(dd.StartMemory, dsize);
        //                var ltmp = vcm.ToIntList();
        //                //vcm.Dispose();


        //                if (ltmp.Count > 0)
        //                {
        //                    int preid = ltmp[0];
        //                    dtmp.Add(preid, 0);
        //                    for (int i = 1; i < ltmp.Count; i++)
        //                    {
        //                        var id = ltmp[i] + preid;
        //                        dtmp.Add(id, i);
        //                        preid = id;
        //                    }
        //                }
        //                TagHeadOffsetManager.manager.Add(idsum, count, dtmp, blockPointer);

        //                dd.Dispose();
        //            }
        //            return dtmp;
        //        }
        //        else
        //        {
        //            var re = TagHeadOffsetManager.manager.Get(idsum, count);
        //            blockPointer = re.Item2;
        //            return re.Item1;
        //        }
        //    }
        //}

        ///// <summary>
        ///// 读取某个变量在头部文件种的序号
        ///// </summary>
        ///// <param name="datafile"></param>
        ///// <param name="tid"></param>
        ///// <param name="offset"></param>
        ///// <param name="fileDuration"></param>
        ///// <param name="blockDuration"></param>
        ///// <param name="timetick"></param>
        ///// <returns></returns>
        //public static int ReadTagIndexInDataPointer( DataFileSeriserbase datafile, int tid, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockpointer, out DateTime time)
        //{
        //    var hfile = CheckBlockHeadCach(datafile,offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
        //    if (hfile.ContainsKey(tid))
        //    {
        //        return hfile[tid];
        //    }
        //    return -1;
        //}

        ///// <summary>
        ///// 读取数据区域的数据头数据
        ///// </summary>
        ///// <param name="datafile"></param>
        ///// <param name="tid"></param>
        ///// <param name="offset"></param>
        ///// <param name="fileDuration"></param>
        ///// <param name="blockDuration"></param>
        ///// <param name="timetick"></param>
        ///// <returns></returns>
        //public static List<long> ReadTargetBlockAddress(DataFileSeriserbase datafile, List<int> tid, long offset, out int tagCount, out int fileDuration, out int blockDuration, out int timetick, out long blockpointer, out DateTime time)
        //{
        //    var hfile = datafile.CheckBlockHeadCach(offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
        //    List<long> re = new List<long>();
        //    foreach (var vv in tid)
        //    {
        //        if (hfile.ContainsKey(vv))
        //        {
        //            re.Add(hfile[vv]);
        //        }
        //        else
        //        {
        //            re.Add(-1);
        //        }
        //    }
        //    return re;
        //}

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
        public static MarshalMemoryBlock ReadTagDataBlock(DataFileSeriserbase datafile, int tid, long offset, DateTime dataTime, out int timetick,out int index)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            DateTime time;
            long blockpointer = 0;

            ReadRegionHead(datafile, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            //var blockIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);

            var dindex = tid % DataFileInfo4.PageFileTagCount;

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

            var headdata = GetHeadBlock(datafile, offset + blockpointer, tagCount * blockcount * 8);

            var dataPointer = headdata.ReadInt(dindex * 8 + blockIndex * tagCount * 8); //读取DataBlock的相对地址

            var vmm = GetDataMemory(datafile,  dataPointer);
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
        public static MarshalMemoryBlock ReadTagDataBlock(DataFileSeriserbase datafile,int tid,long offset,int index, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            DateTime time;
            long blockpointer = 0;

            //var dindex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            ReadRegionHead(datafile, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            var dindex = tid % DataFileInfo4.PageFileTagCount;

            int blockIndex = index;

            int blockcount = fileDuration * 60 / blockDuration;
            var headdata = GetHeadBlock(datafile, offset + blockpointer, tagCount * blockcount * 8);

            var dataPointer = headdata.ReadInt(dindex * 8 + blockIndex * tagCount * 8); //读取DataBlock的相对地址
            //var dataPointerbase = headdata.ReadLong(dindex * 12 + blockIndex * tagCount * 12 + 4); //读取DataBlock的基地址


            if (dataPointer > 0)
            {
                var vmm = GetDataMemory(datafile,dataPointer);
                return vmm;
            }
            else
            {
                return null;
            }
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
        public static Dictionary<MarshalMemoryBlock, Tuple<List<DateTime>, int>> ReadTagDataBlock2(DataFileSeriserbase datafile, int tid, long offset, List<DateTime> dataTimes, out int timetick)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            long blockpointer = 0;
            DateTime time;
            //var tagIndex = datafile.ReadTagIndexInDataPointer(tid, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            
            ReadRegionHead(datafile, offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);
            var tagIndex = tid % DataFileInfo4.PageFileTagCount;

          

            Dictionary<long, MarshalMemoryBlock> rtmp = new Dictionary<long, MarshalMemoryBlock>();

            Dictionary<MarshalMemoryBlock, Tuple<List<DateTime>,int>> re = new Dictionary<MarshalMemoryBlock, Tuple<List<DateTime>, int>>();

            if (tagCount == 0 || tagIndex>=tagCount) return re;

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

                var dataPointer = headdata.ReadInt(tagIndex * 8 + blockindex * tagCount * 8); //读取DataBlock的相对地址

                if (dataPointer > 0)
                {
                    //var datasize = datafile.ReadInt(dataPointer); //读取DataBlock 的大小
                    var vmm = GetDataMemory(datafile, dataPointer);

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
        private static MarshalMemoryBlock GetDataMemory(DataFileSeriserbase datafile,long address)
        {
            //说明数据没有采用Zip压缩，可以直接读取使用
            var dp = address;
            var datasize = datafile.ReadInt(dp);
            return datafile.Read(dp + 4, datasize);
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
        public static IEnumerable<Tuple<MarshalMemoryBlock,DateTime, DateTime,int>> ReadTagDataBlock2(DataFileSeriserbase datafile, int tid, long offset, DateTime start, DateTime end)
        {
            int fileDuration, blockDuration = 0;
            int tagCount = 0;
            long blockpointer = 0;
            int timetick = 0;
            DateTime time;



            ReadRegionHead(datafile,  offset, out tagCount, out fileDuration, out blockDuration, out timetick, out blockpointer, out time);

            if (tagCount == 0) yield return null;

            var tagIndex = tid % DataFileInfo4.PageFileTagCount;

            if(tagIndex>=tagCount) yield return null;

            int blockcount = fileDuration * 60 / blockDuration;

            //读取文件开始时间
            var startTime = datafile.ReadDateTime(0);

            DateTime sstart = start;
            DateTime send = end;

            var headdata = GetHeadBlock(datafile, offset + blockpointer, tagCount * blockcount * 8);

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

                var dataPointer = headdata.ReadInt(tagIndex * 8 + blockindex * tagCount * 8); //读取DataBlock的相对地址
                               
                if (dataPointer > 0)
                {
                    var vmm = GetDataMemory(datafile, dataPointer);
                    if (vmm != null)
                        yield return new Tuple<MarshalMemoryBlock, DateTime, DateTime, int>(vmm, sstart, send, timetick);
                }
                sstart = send;
            }
        }
        #endregion
    }
}
