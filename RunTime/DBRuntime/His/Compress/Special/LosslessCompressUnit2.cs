//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using DBRuntime.His.Compress;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cdy.Tag
{

    /// <summary>
    /// 无损压缩数据块内容格式
    /// 统计值(52)(支持统计值数据类型)+压缩数据区
    /// </summary>
    public class LosslessCompressUnit2 : CompressUnitbase2
    {
        protected MemoryBlock mMarshalMemory;

        protected ProtoMemory mVarintMemory;

        protected DoubleCompressBuffer mDCompress;

        protected FloatCompressBuffer mFCompress;

        /// <summary>
        /// 
        /// </summary>
        protected CustomQueue<int> emptys = new CustomQueue<int>(604);

        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "无损压缩";

        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 1;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase2 Clone()
        {
            return new LosslessCompressUnit2();
        }

        #region Compress

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <param name="statisticTarget"></param>
        /// <param name="statisticAddr"></param>
        /// <returns></returns>
        public override long CompressWithNoStatistic(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size)
        {
            target.WriteDatetime(targetAddr, this.StartTime);
            target.Write(TimeTick);
            switch (TagType)
            {
                case TagType.Bool:
                    return Compress<bool>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Byte:
                    return Compress<byte>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UShort:
                    return Compress<ushort>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Short:
                    return Compress<short>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UInt:
                    return Compress<uint>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Int:
                    return Compress<int>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULong:
                    return Compress<ulong>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Long:
                    return Compress<long>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Double:
                    return Compress<double>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.DateTime:
                    return Compress<DateTime>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Float:
                    return Compress<float>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.String:
                    return Compress<string>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.IntPoint:
                    return Compress<IntPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UIntPoint:
                    return Compress<UIntPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.LongPoint:
                    return Compress<LongPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULongPoint:
                    return Compress<ULongPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.IntPoint3:
                    return Compress<IntPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UIntPoint3:
                    return Compress<UIntPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.LongPoint3:
                    return Compress<LongPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULongPoint3:
                    return Compress<ULongPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
            }
            return 12;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <param name="statisticTarget">统计值内存地址</param>
        /// <param name="statisticAddr">统计值起始地址偏移</param>
        /// <returns></returns>
        public override long Compress(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, IMemoryBlock statisticTarget, long statisticAddr)
        {

            target.WriteDatetime(targetAddr, this.StartTime);

            //LoggerService.Service.Debug("LosslessCompressUnit2", "Record time: "+this.StartTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            target.Write(TimeTick);
            switch (TagType)
            {
                case TagType.Bool:
                    return Compress<bool>(source, sourceAddr, target, targetAddr+12, size,TagType) + 12;
                case TagType.Byte:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<byte>(source, sourceAddr, target, targetAddr+ 12, size, TagType) + 12;
                case TagType.UShort:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<ushort>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Short:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<short>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UInt:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<uint>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Int:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<int>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULong:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<ulong>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Long:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<long>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Double:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<double>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.DateTime:
                    return Compress<DateTime>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Float:
                    NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);
                    return Compress<float>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.String:
                    return Compress<string>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.IntPoint:
                    return Compress<IntPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UIntPoint:
                    return Compress<UIntPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.LongPoint:
                    return Compress<LongPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULongPoint:
                    return Compress<ULongPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.IntPoint3:
                    return Compress<IntPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UIntPoint3:
                    return Compress<UIntPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.LongPoint3:
                    return Compress<LongPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULongPoint3:
                    return Compress<ULongPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
            }
            return 12;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="timerVals"></param>
        ///// <param name="emptyIds"></param>
        ///// <returns></returns>
        //protected Memory<byte> CompressTimers(List<int> timerVals, CustomQueue<int> emptyIds)
        //{
        //    int preids = 0;
        //    mVarintMemory.Reset();
        //    emptys.Reset();
        //    //emptys.WriteIndex = 0;
        //    //emptyIds.ReadIndex = 0;
        //    bool isFirst = true;
        //    for (int i = 0; i < timerVals.Count; i++)
        //    {
        //        if (timerVals[i] > 0||i==0)
        //        {
        //            var id = timerVals[i];
        //            if (isFirst)
        //            {
        //                mVarintMemory.WriteInt32(id);
        //                isFirst = false;
        //            }
        //            else
        //            {
        //                mVarintMemory.WriteInt32(id - preids);
        //            }
        //            preids = id;
        //        }
        //        else
        //        {
        //            emptyIds.Insert(i);
        //        }
        //    }
        //    return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="startAddr"></param>
        /// <param name="count"></param>
        /// <param name="value"></param>
        private void CalAvgValue(IMemoryFixedBlock source,long startAddr,out int count,out double value,long size, TagType type)
        {
            byte tlen = (source as HisDataMemoryBlock).TimeLen;
            var valuecount = size - this.QulityOffset;
            long valueoffset = tlen * valuecount;
            
            double dval = 0;
            emptys.Reset();
            ReadAvaiableTimerIds(source, startAddr, valuecount, emptys);
            switch (type)
            {
                case TagType.Byte:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadByte(valueoffset + vid);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.Short:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadShort(valueoffset + vid*2);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.UShort:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadUShort(valueoffset + vid * 2);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.Int:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadInt(valueoffset + vid * 4);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.UInt:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadUInt(valueoffset + vid * 4);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.Long:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadLong(valueoffset + vid * 8);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.ULong:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadULong(valueoffset + vid * 8);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.Double:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadDouble(valueoffset + vid * 8);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
                case TagType.Float:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadFloat(valueoffset + vid * 4);
                            dval += val;
                        }
                        dval = dval / emptys.WriteIndex;
                    }
                    break;
            }
            count = emptys.WriteIndex;
            value = dval;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="startAddr"></param>
        /// <param name="maxValue"></param>
        /// <param name="maxTime"></param>
        /// <param name="minValue"></param>
        /// <param name="minTime"></param>
        private void CalMaxMinValue(IMemoryFixedBlock source,long startAddr,out double maxValue,out DateTime maxTime,out double minValue,out DateTime minTime,long size, TagType type)
        {

            maxValue = double.MinValue; minValue = double.MaxValue;
            maxTime = minTime = DateTime.Now;
            int maxtimeId=-1, mintimeId = -1;

            byte tlen = (source as HisDataMemoryBlock).TimeLen;
            var valuecount = size - this.QulityOffset;
            long valueoffset = tlen * valuecount;
            //long valueoffset = tlen * size;
            emptys.Reset();
            ReadAvaiableTimerIds(source, startAddr, valuecount, emptys);
            switch (type)
            {
                case TagType.Byte:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadByte(valueoffset + vid);
                            if(val<minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if(val>maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                    }
                    break;
                case TagType.Short:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadShort(valueoffset + vid * 2);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                      
                    }
                    break;
                case TagType.UShort:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadUShort(valueoffset + vid * 2);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                        
                    }
                    break;
                case TagType.Int:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadInt(valueoffset + vid * 4);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                        
                    }
                    break;
                case TagType.UInt:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadUInt(valueoffset + vid * 4);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                        
                    }
                    break;
                case TagType.Long:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadLong(valueoffset + vid * 8);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                       
                    }
                    break;
                case TagType.ULong:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadULong(valueoffset + vid * 8);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                        
                    }
                    break;
                case TagType.Double:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadDouble(valueoffset + vid * 8);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                    }
                    break;
                case TagType.Float:
                    if (emptys.WriteIndex > 0)
                    {
                        for (int i = 0; i < emptys.WriteIndex; i++)
                        {
                            var vid = emptys.IncRead();
                            var val = source.ReadFloat(valueoffset + vid * 4);
                            if (val < minValue)
                            {
                                minValue = val;
                                mintimeId = vid;
                            }
                            if (val > maxValue)
                            {
                                maxValue = val;
                                maxtimeId = vid;
                            }
                        }
                    }
                    break;
            }

            if(maxtimeId>-1 && mintimeId>-1)
            {
                emptys.Reset();
                ReadAllTimerValues(source, startAddr, valuecount, emptys);

                var maxtimeval = emptys.Read(maxtimeId);
                var mintimeval = emptys.Read(mintimeId);
                maxTime = StartTime.AddMilliseconds(maxtimeval * TimeTick);
                minTime = StartTime.AddMilliseconds(mintimeval * TimeTick);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="startAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetaddr"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        protected override int NumberStatistic(IMemoryFixedBlock source, long startAddr, IMemoryBlock target, long targetaddr,long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);

            int avgcount=0;
            double avgvalue=0, maxvalue=0, minvalue=0;
            DateTime maxtime=DateTime.Now, mintime=DateTime.Now;

            CalAvgValue(source, startAddr, out avgcount, out avgvalue,size,type);
            CalMaxMinValue(source, startAddr, out maxvalue, out maxtime, out minvalue, out mintime,size, type);

            target.WriteInt(targetaddr, Id);
            target.WriteInt(targetaddr + 8, avgcount);
            target.WriteDouble(targetaddr + 12, avgvalue);
            target.WriteDatetime(targetaddr + 20, maxtime);
            target.WriteDouble(targetaddr + 28, maxvalue);
            target.WriteDatetime(targetaddr + 36, mintime);
            target.WriteDouble(targetaddr + 44, minvalue);

            return 52;
        }

        /// <summary>
        /// 时间戳压缩算法：
        /// 1. 各个时间之间取后一个与前一个之间的差值，第一个保持原始值
        /// 2. 差值之后，采用Proto 压缩原理进行压缩
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="startaddr"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected virtual Memory<byte> CompressTimers(IMemoryFixedBlock timerVals,long startaddr,int count, CustomQueue<int> emptyIds)
        {
            int preids = 0;
            mVarintMemory.Reset();
            emptys.Reset();

            byte tlen = (timerVals as HisDataMemoryBlock).TimeLen;

            bool isFirst = true;
            int id = 0;
            for (int i = 0; i < count; i++)
            {
                id = tlen == 2 ? timerVals.ReadUShort((int)startaddr + i * 2) : timerVals.ReadInt((int)startaddr + i * 4);

                if (id > 0 || i == 0)
                {
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt32(id);
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteInt32(id - preids);
                    }
                    preids = id;
                }
                else
                {
                    emptyIds.Insert(i);
                }
            }
            return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timers"></param>
        private void ReadAvaiableTimerIds(IMemoryFixedBlock timerVals, long startaddr, long count, CustomQueue<int> timers)
        {
            timers.Reset();
            byte tlen = (timerVals as HisDataMemoryBlock).TimeLen;
            timers.CheckAndResize((int)count);
        
            for (int i = 0; i < count; i++)
            {
                var id = tlen == 2 ? timerVals.ReadUShort((int)startaddr + i * 2) : timerVals.ReadInt((int)startaddr + i * 4);
                if (id > 0)
                {
                    timers.Insert(i);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="startaddr"></param>
        /// <param name="count"></param>
        /// <param name="timers"></param>
        private void ReadAllTimerValues(IMemoryFixedBlock timerVals, long startaddr, long count, CustomQueue<int> timers)
        {
            timers.Reset();
            byte tlen = (timerVals as HisDataMemoryBlock).TimeLen;
            timers.CheckAndResize((int)count);
           
            for (int i = 0; i < count; i++)
            {
                var id = tlen == 2 ? timerVals.ReadUShort((int)startaddr + i * 2) : timerVals.ReadInt((int)startaddr + i * 4);
                timers.Insert(id);
            }
        }

        /// <summary>
        /// Byte 压缩算法： 字节直接存储;
        /// Short、Int、Long,Datetime 类型数据压缩算法：
        /// 1.相邻直接的数据，取差值
        /// 2.将数据采用Proto 压缩算法进行压缩;
        /// 
        /// Double 压缩算法：
        /// 1.将Double 数据转换成Long型
        /// 2.将转换之后的数据采用Proto 压缩算法进行压缩;
        /// 
        /// Float 压缩算法：
        /// 1.将Float 数据转换成Int型
        /// 2.将转换之后的数据采用Proto 压缩算法进行压缩;
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected virtual Memory<byte> CompressValues<T>(IMemoryFixedBlock source,long offset,int count, CustomQueue<int> emptyIds,TagType type)
        {
            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            bool isFirst = true;
            switch (type)
            {
                case TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadByte((int)offset + i);
                            mMarshalMemory.Write(id);
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                            //    emptyIds.TryDequeue(out ig);
                        }
                    }
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Short:
                    short sval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadShort((int)offset + i * 2);
                            if (isFirst)
                            {
                                mVarintMemory.WriteSInt32(id);
                                isFirst = false;
                                sval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - sval);
                                sval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UShort:
                    ushort ssval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUShort((int)offset + i * 2);
                            if (isFirst)
                            {
                                mVarintMemory.WriteSInt32(id);
                                isFirst = false;
                                ssval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - ssval);
                                ssval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Int:
                    int isval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i * 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                isFirst = false;
                                isval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - isval);
                                isval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UInt:
                    uint uisval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                isFirst = false;
                                uisval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - uisval));
                            }
                            uisval = id;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Long:
                    long lsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                isFirst = false;
                                lsval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((id - lsval));
                                lsval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.ULong:
                    ulong ulsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                isFirst = false;
                                ulsval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(id - ulsval));
                                ulsval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.DateTime:
                    ulong udlsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                isFirst = false;
                                udlsval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(id - udlsval));
                                udlsval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Double:
                    mDCompress.Reset();
                    mDCompress.Precision = this.Precision;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadDouble((int)offset + i * 8);
                            mDCompress.Append(id);
                           // mMarshalMemory.Write(id);
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    mDCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Float:
                    mFCompress.Reset();
                    mFCompress.Precision = this.Precision;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadFloat((int)offset + i * 4);
                            mFCompress.Append(id);
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    mFCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.IntPoint:
                    int psval = 0;
                    int psval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i * 8);
                            var id2 = source.ReadInt((int)offset + i * 8 + 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - psval);
                                mVarintMemory.WriteSInt32(id2 - psval2);
                            }
                            psval = id;
                            psval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UIntPoint:
                    uint upsval = 0;
                    uint upsval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 8);
                            var id2 = source.ReadUInt((int)offset + i * 8 + 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - upsval));
                                mVarintMemory.WriteSInt32((int)(id2 - upsval2));
                            }
                            upsval = id;
                            upsval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.IntPoint3:
                     psval = 0;
                     psval2 = 0;
                    int psval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i * 12);
                            var id2 = source.ReadInt((int)offset + i * 12 + 4);
                            var id3 = source.ReadInt((int)offset + i * 12 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                mVarintMemory.WriteInt32(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - psval));
                                mVarintMemory.WriteSInt32((int)(id2 - psval2));
                                mVarintMemory.WriteSInt32((int)(id3 - psval3));
                            }
                            psval = id;
                            psval2 = id2;
                            psval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UIntPoint3:
                     upsval = 0;
                     upsval2 = 0;
                    uint upsval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 12);
                            var id2 = source.ReadUInt((int)offset + i * 12 + 4);
                            var id3 = source.ReadUInt((int)offset + i * 12 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                mVarintMemory.WriteInt32(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - upsval));
                                mVarintMemory.WriteSInt32((int)(id2 - upsval2));
                                mVarintMemory.WriteSInt32((int)(id3 - upsval3));
                            }
                            upsval = id;
                            upsval2 = id2;
                            upsval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.LongPoint:
                    long lpsval = 0;
                    long lpsval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 16);
                            var id2 = source.ReadLong((int)offset + i * 16 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64(id - lpsval);
                                mVarintMemory.WriteSInt64(id2 - lpsval2);
                            }
                            lpsval = id;
                            lpsval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.ULongPoint:
                    ulong ulpsval = 0;
                    ulong ulpsval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 16);
                            var id2 = source.ReadULong((int)offset + i * 16 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(id - ulpsval));
                                mVarintMemory.WriteSInt64((long)(id2 - ulpsval2));
                            }
                            ulpsval = id;
                            ulpsval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.LongPoint3:
                    lpsval = 0;
                    lpsval2 = 0;
                    long lpsval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 24);
                            var id2 = source.ReadLong((int)offset + i * 24 + 8);
                            var id3 = source.ReadLong((int)offset + i * 24 + 16);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                mVarintMemory.WriteInt64(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64(id - lpsval);
                                mVarintMemory.WriteSInt64(id2 - lpsval2);
                                mVarintMemory.WriteSInt64(id3 - lpsval3);
                            }
                            lpsval = id;
                            lpsval2 = id2;
                            lpsval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.ULongPoint3:
                    ulpsval = 0;
                    ulpsval2 = 0;
                    ulong ulpsval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 24);
                            var id2 = source.ReadULong((int)offset + i * 24 + 8);
                            var id3 = source.ReadULong((int)offset + i * 24 + 16);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                mVarintMemory.WriteInt64(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(id - ulpsval));
                                mVarintMemory.WriteSInt64((long)(id2 - ulpsval2));
                                mVarintMemory.WriteSInt64((long)(id3 - ulpsval3));
                            }
                            ulpsval = id;
                            ulpsval2 = id2;
                            ulpsval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                default:
                    break;
            }
            return mVarintMemory.DataBuffer.AsMemory<byte>(0, (int)mVarintMemory.WritePosition);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Vals"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected Memory<byte> CompressValues(List<string> Vals, CustomQueue<int> emptyIds)
        {
            mMarshalMemory.Position = 0;
            int ig = -1;
            ig = emptys.ReadIndex <= emptyIds.WriteIndex ? emptys.IncRead() : -1;
            for (int i = 0; i < Vals.Count; i++)
            {
                if(i != ig)
                {
                    var id = Vals[i];
                    mMarshalMemory.Write(id);
                }
                else
                {
                    ig = emptys.ReadIndex <= emptyIds.WriteIndex ? emptys.IncRead() : -1;
                }
            }
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// String 类型的数据采用GZip 算法进行压缩
        /// </summary>
        /// <param name="Vals"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected Memory<byte> CompressValues2(List<string> Vals, CustomQueue<int> emptyIds,out byte[] bytearray)
        {
            mMarshalMemory.Position = 0;
            var bvals = ArrayPool<byte>.Shared.Rent(Vals.Count * 255);
            int ig = -1;
            ig = emptys.ReadIndex <= emptyIds.WriteIndex ? emptys.IncRead() : -1;
            long size = 0;
            var vss = new System.IO.MemoryStream(bvals);
            using (System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(vss, System.IO.Compression.CompressionLevel.Optimal))
            {
                for (int i = 0; i < Vals.Count; i++)
                {
                    if (i != ig)
                    {
                        var id = Vals[i];
                        var vals = Encoding.Unicode.GetBytes(Vals[i]);
                        gs.Write(BitConverter.GetBytes(vals.Length));
                        gs.Write(vals);
                    }
                    else
                    {
                        ig = emptys.ReadIndex <= emptyIds.WriteIndex ? emptys.IncRead() : -1;
                    }
                }
                gs.Flush();
                size = vss.Position;
            }
            vss.Dispose();
            bytearray = bvals;
            return bvals.AsMemory<byte>(0, (int)size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="bvals"></param>
        /// <returns></returns>
        public List<string> DeCompressToStrings(byte[] bvals)
        {
            List<string> re = new List<string>();
            byte[] btmp = System.Buffers.ArrayPool<byte>.Shared.Rent(4);
            using (System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(new System.IO.MemoryStream(bvals), System.IO.Compression.CompressionMode.Decompress))
            {
                while (gs.Read(btmp, 0, 4)>0)
                {
                    int ilen = BitConverter.ToInt32(btmp, 0);
                    if (ilen > 0)
                    {
                        byte[] bb = System.Buffers.ArrayPool<byte>.Shared.Rent(ilen);
                        gs.Read(bb, 0, ilen);
                        re.Add(Encoding.Unicode.GetString(bb, 0, ilen));
                        System.Buffers.ArrayPool<byte>.Shared.Return(bb);
                    }
                    else
                    {
                        re.Add("");
                    }
                }
            }
            System.Buffers.ArrayPool<byte>.Shared.Return(btmp);
            return re;
        }


        /// <summary>
        /// 质量戳压缩原理：
        /// 1.先将所有质量戳整理成，[值+重复个数]的形式
        /// 2.将结果值，采用Proto 压缩算法进行压缩
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="totalcount"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected Memory<byte> CompressQualitys(IMemoryFixedBlock source, long offset, int totalcount, CustomQueue<int> emptyIds)
        {
            int count = 1;
            byte qus = source.ReadByte((int)offset);
            //using (ProtoMemory memory = new ProtoMemory(qulitys.Length * 2))
            mVarintMemory.Reset();
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            //emptyIds.TryDequeue(out ig);
            mVarintMemory.WriteInt32(qus);
            for (int i = 1; i < totalcount; i++)
            {
                if (i != ig)
                {
                    byte bval = source.ReadByte((int)offset + i);
                    if (bval == qus)
                    {
                        count++;
                    }
                    else
                    {
                        mVarintMemory.WriteInt32(count);
                        qus = bval;
                        mVarintMemory.WriteInt32(qus);
                        count = 1;
                    }
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                    //    emptyIds.TryDequeue(out ig);
                }
            }
            mVarintMemory.WriteInt32(count);
            
            return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="qulitys"></param>
        /// <returns></returns>
        protected Memory<byte> CompressQualitys(byte[] qulitys, CustomQueue<int> emptyIds)
        {
            int count = 1;
            byte qus = qulitys[0];
            //using (ProtoMemory memory = new ProtoMemory(qulitys.Length * 2))
            mVarintMemory.Reset();
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            mVarintMemory.WriteInt32(qus);
            for (int i = 1; i < qulitys.Length; i++)
            {
                if (i != ig)
                {
                    if (qulitys[i] == qus)
                    {
                        count++;
                    }
                    else
                    {
                        mVarintMemory.WriteInt32(count);
                        qus = qulitys[i];
                        mVarintMemory.WriteInt32(qus);
                        count = 1;
                    }
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                }
            }
            mVarintMemory.WriteInt32(count);
            return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        }

        /// <summary>
        /// Bool 值压缩算法：
        /// 1个字节的最高位,表示Bool的值,后面7为表示该值的重复次数
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        protected Memory<byte> CompressBoolValues(IMemoryFixedBlock source, long offset, int totalcount, CustomQueue<int> emptyIds)
        {
            List<byte> re = new List<byte>(totalcount);
            byte bval = source.ReadByte((int)offset);
            byte scount = 1;
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            //emptyIds.TryDequeue(out ig);

            byte sval = (byte)(bval << 7);
            for(int i=0;i< totalcount; i++)
            {
                if (i != ig)
                {
                    var btmp = source.ReadByte((int)(offset + i));
                    if(btmp == bval && scount<127)
                    {
                        scount++;
                    }
                    else
                    {
                        sval = (byte)(sval | scount);
                        re.Add(sval);
                        scount = 1;
                        bval = btmp;
                        sval = (byte)(bval << 7);
                    }
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                    //   emptyIds.TryDequeue(out ig);
                }
            }
            sval = (byte)(sval | scount);
            re.Add(sval);

            mMarshalMemory.Position = 0;
            foreach (var vv in re)
            {
                mMarshalMemory.Write(vv);
            }
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        

        /// <summary>
        /// 数据内容：有效数据个数 + 时间戳数据块 + 值数据块 + 质量戳数据块
        /// (时间\值\质量戳)数据块: 数据长度+数据内容
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        protected virtual long Compress<T>(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);

            byte tlen = (source as HisDataMemoryBlock).TimeLen;

            if (mMarshalMemory==null)
            {
                mMarshalMemory = new MemoryBlock(count * 10);
            }
            else 
            {
                mMarshalMemory.CheckAndResize(count * 10);
            }

            if(mVarintMemory==null)
            {
                mVarintMemory = new ProtoMemory(count * 10);
            }
            else if(mVarintMemory.DataBuffer.Length<count*10)
            {
                mVarintMemory.Dispose();
                mVarintMemory = new ProtoMemory(count * 10);
            }

            emptys.CheckAndResize(count);
            emptys.Reset();

             var datas = CompressTimers(source, sourceAddr, (int)count, emptys);
            long rsize = 0;
            int rcount = count - emptys.WriteIndex - 1;

            target.WriteInt(targetAddr,rcount);
            rsize += 4;
            target.Write((int)datas.Length);
            target.Write(datas);
            rsize += 4;
            rsize += datas.Length;
            
            switch (type)
            {
                case TagType.Bool:
                    var cval = CompressBoolValues(source, count * tlen + sourceAddr, count, emptys);
                    target.Write(cval.Length);
                    target.Write(cval);
                    rsize += 4;
                    rsize += cval.Length;
                    emptys.ReadIndex = 0;
                    var cqus = CompressQualitys(source, count * (tlen+1) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Byte:
                    cval = CompressValues<byte>(source, count * tlen + sourceAddr, count, emptys,TagType);
                    target.Write(cval.Length);
                    target.Write(cval);
                    rsize += 4;
                    rsize += cval.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 1) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UShort:
                    var ures = CompressValues<ushort>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ures.Length);
                    target.Write(ures);
                    rsize += 4;
                    rsize += ures.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 2) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Short:
                   var  res = CompressValues<short>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(res.Length);
                    target.Write(res);
                    rsize += 4;
                    rsize += res.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 2) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UInt:
                    var uires = CompressValues<uint>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(uires.Length);
                    target.Write(uires);
                    rsize += 4;
                    rsize += uires.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Int:
                    var ires = CompressValues<int>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ires.Length);
                    target.Write(ires);
                    rsize += 4;
                    rsize += ires.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULong:
                    var ulres = CompressValues<ulong>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ulres.Length);
                    target.Write(ulres);
                    rsize += 4;
                    rsize += ulres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Long:
                    var lres = CompressValues<long>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(lres.Length);
                    target.Write(lres);
                    rsize += 4;
                    rsize += lres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.DateTime:
                    var dres = CompressValues<ulong>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(dres.Length);
                    target.Write(dres);
                    rsize += 4;
                    rsize += dres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Double:

                    if (mDCompress == null)
                    {
                        mDCompress = new DoubleCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        
                        if(mDCompress.VarintMemory==null || mDCompress.VarintMemory.DataBuffer==null)
                        {
                            mDCompress.VarintMemory = mVarintMemory;
                        }
                        mDCompress.CheckAndResizeTo(count);
                    }

                    var ddres = CompressValues<double>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ddres.Length);
                    target.Write(ddres);
                    rsize += 4;
                    rsize += ddres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen+8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Float:

                    if (mFCompress == null)
                    {
                        mFCompress = new FloatCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if(mFCompress.VarintMemory==null || mFCompress.VarintMemory.DataBuffer == null)
                        {
                            mFCompress.VarintMemory = mVarintMemory;
                        }
                        mFCompress.CheckAndResizeTo(count);
                    }

                    var fres = CompressValues<float>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(fres.Length);
                    target.Write(fres);
                    rsize += 4;
                    rsize += fres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen+4) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.String:
                    var vals = source.ReadStringsByFixSize(count * tlen + (int)sourceAddr, count);
                    var qus = source.ReadBytes(count);
                    byte[] bvals;
                    var sres = CompressValues2(vals, emptys,out bvals);
                    target.Write(sres.Length);
                    target.Write(sres);

                    ArrayPool<byte>.Shared.Return(bvals);

                    rsize += 4;
                    rsize += sres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(qus, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.IntPoint:
                    var ipres = CompressValues<IntPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen+8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UIntPoint:
                    ipres = CompressValues<UIntPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.LongPoint:
                    ipres = CompressValues<LongPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 16) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULongPoint:
                    ipres = CompressValues<ULongPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 16) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.IntPoint3:
                    ipres = CompressValues<IntPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 12) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UIntPoint3:
                    ipres = CompressValues<UIntPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 12) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.LongPoint3:
                    ipres = CompressValues<LongPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 24) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULongPoint3:
                    ipres = CompressValues<ULongPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen+24) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
            }
            return rsize;
        }
        #endregion

        #region Decompress

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        private List<int> DeCompressTimers(byte[] timerVals, int count)
        {
            List<int> re = new List<int>();
            using (ProtoMemory memory = new ProtoMemory(timerVals))
            {
                int sval = (int)memory.ReadInt32();
                re.Add(sval);
                int preval = sval;
                for (int i = 1; i < count; i++)
                {
                    var ss = memory.ReadInt32();
                    var val = (preval + ss);
                    re.Add(val);
                    preval = val;
                }
                return re;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        private List<byte> DeCompressQulity(byte[] values)
        {
            List<byte> re = new List<byte>();
            using (ProtoMemory memory = new ProtoMemory(values))
            {
                while(memory.ReadPosition<values.Length)
                {
                    byte sval = (byte)memory.ReadInt32(); //读取质量戳
                    int ival = memory.ReadInt32(); //读取质量戳重复次数
                    for(int i=0;i<ival;i++)
                    {
                        re.Add(sval);
                    }
                }
                return re;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        protected virtual List<T> DeCompressValue<T>(byte[] value, int count)
        {
            string tname = typeof(T).Name;
            switch (tname)
            {
                case "bool":
                case "Boolean":
                    using (MemorySpan block = new MemorySpan(value))
                    {
                        List<bool> bre = new List<bool>();
                        var rtmp = block.ToByteList();

                        foreach (var vv in rtmp)
                        {
                            bool bval = ((vv & 0x80) >> 7) > 0;
                            byte bcount = (byte)(vv & 0x7F);
                            for (int i = 0; i < bcount; i++)
                            {
                                bre.Add(bval);
                            }
                        }
                        return bre as List<T>;
                    }
                case "Byte":
                    return value.ToList() as List<T>;
                case "Int16":
                    List<short> re = new List<short>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (short)memory.ReadSInt32();
                        re.Add(vv);
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (short)memory.ReadSInt32();
                            vv = (short)(vv + vss);
                            re.Add((short)(vv));

                        }
                    }
                    return re as List<T>;
                case "UInt16":
                    List<ushort> ure = new List<ushort>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (ushort)memory.ReadSInt32();
                        ure.Add((ushort)vv);
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (short)memory.ReadSInt32();
                            vv = (ushort)(vv + vss);
                            ure.Add((ushort)(vv));

                        }
                    }
                    return ure as List<T>;
                case "Int32":
                    List<int> ire = new List<int>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (int)memory.ReadInt32();
                        ire.Add(vv);
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (int)memory.ReadSInt32();
                            vv = (int)(vv + vss);
                            ire.Add(vv);

                        }
                    }
                    return ire as List<T>;
                case "UInt32":
                    List<uint> uire = new List<uint>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (uint)memory.ReadInt32();
                        uire.Add((uint)vv);
                        for (int i = 1; i < count; i++)
                        {
                            var vss = memory.ReadSInt32();
                            vv = (uint)(vv + vss);
                            uire.Add(vv);
                        }
                    }
                    return uire as List<T>;
                case "Int64":
                    List<long> lre = new List<long>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (long)memory.ReadInt64();
                        lre.Add(vv);
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (long)memory.ReadSInt64();
                            vv = (long)(vv + vss);
                            lre.Add(vv);

                        }
                    }
                    return lre as List<T>;
                case "UInt64":
                    List<ulong> ulre = new List<ulong>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (ulong)memory.ReadInt64();
                        ulre.Add(vv);
                        for (int i = 1; i < count; i++)
                        {
                            var vss = memory.ReadSInt64();
                            vv = (ulong)((long)vv + vss);
                            ulre.Add(vv);
                        }
                    }
                    return ulre as List<T>;
                case "Double":
                    return DoubleCompressBuffer.Decompress(value) as List<T>;
                case "Single":
                    return FloatCompressBuffer.Decompress(value) as List<T>;
                case "String":
                    return DeCompressToStrings(value) as List<T>;
                case "DateTime":
                    List<DateTime> dtre = new List<DateTime>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (ulong)memory.ReadInt64();
                        dtre.Add(MemoryHelper.ReadDateTime(BitConverter.GetBytes(vv)));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = memory.ReadSInt64();
                            vv = (ulong)((long)vv + vss);
                            dtre.Add(MemoryHelper.ReadDateTime(BitConverter.GetBytes(vv)));
                        }
                    }
                    return dtre as List<T>;
                case "IntPointData":
                    List<IntPointData> lpre = new List<IntPointData>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (int)memory.ReadInt32();
                        var vv2 = (int)memory.ReadInt32();
                        lpre.Add(new IntPointData(vv, vv2));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (int)memory.ReadSInt32();
                            var vss2 = (int)memory.ReadSInt32();
                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            lpre.Add(new IntPointData((int)vv, (int)vv2));

                        }
                    }
                    return lpre as List<T>;
                case "UIntPointData":
                    List<UIntPointData> ulpre = new List<UIntPointData>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (int)memory.ReadInt32();
                        var vv2 = (int)memory.ReadInt32();
                        ulpre.Add(new UIntPointData((uint)vv, (uint)vv2));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (int)memory.ReadSInt32();
                            var vss2 = (int)memory.ReadSInt32();
                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            ulpre.Add(new UIntPointData((uint)vv, (uint)vv2));
                        }
                    }
                    return ulpre as List<T>;
                case "LongPointData":
                    List<LongPointData> lppre = new List<LongPointData>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (long)memory.ReadInt64();
                        var vv2 = (long)memory.ReadInt64();
                        lppre.Add(new LongPointData(vv, vv2));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = memory.ReadSInt64();
                            var vss2 = memory.ReadSInt64();
                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            lppre.Add(new LongPointData(vv, vv2));

                        }
                    }
                    return lppre as List<T>;
                case "ULongPointData":
                    List<ULongPointData> ulppre = new List<ULongPointData>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = memory.ReadInt64();
                        var vv2 = memory.ReadInt64();
                        ulppre.Add(new ULongPointData((ulong)vv, (ulong)vv2));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = memory.ReadSInt64();
                            var vss2 = memory.ReadSInt64();
                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            ulppre.Add(new ULongPointData((ulong)vv, (ulong)vv2));

                        }
                    }
                    return ulppre as List<T>;
                case "IntPoint3Data":
                    List<IntPoint3Data> ip3re = new List<IntPoint3Data>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (int)memory.ReadInt32();
                        var vv2 = (int)memory.ReadInt32();
                        var vv3 = (int)memory.ReadInt32();
                        ip3re.Add(new IntPoint3Data(vv, vv2, vv3));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (int)memory.ReadSInt32();
                            var vss2 = (int)memory.ReadSInt32();
                            var vss3 = (int)memory.ReadSInt32();

                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            vv3 = (vv3 + vss3);

                            ip3re.Add(new IntPoint3Data((int)vv, (int)vv2, (int)vv3));

                        }
                    }
                    return ip3re as List<T>;
                case "UIntPoint3Data":
                    List<UIntPoint3Data> uip3re = new List<UIntPoint3Data>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (int)memory.ReadInt32();
                        var vv2 = (int)memory.ReadInt32();
                        var vv3 = (int)memory.ReadInt32();
                        uip3re.Add(new UIntPoint3Data((uint)vv, (uint)vv2, (uint)vv3));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = (int)memory.ReadSInt32();
                            var vss2 = (int)memory.ReadSInt32();
                            var vss3 = (int)memory.ReadSInt32();

                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            vv3 = (vv3 + vss3);

                            uip3re.Add(new UIntPoint3Data((uint)vv, (uint)vv2, (uint)vv3));

                        }
                    }
                    return uip3re as List<T>;
                case "LongPoint3Data":
                    List<LongPoint3Data> lpp3re = new List<LongPoint3Data>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (long)memory.ReadInt64();
                        var vv2 = (long)memory.ReadInt64();
                        var vv3 = (long)memory.ReadInt64();
                        lpp3re.Add(new LongPoint3Data((long)vv, (long)vv2, (long)vv3));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = memory.ReadInt64();
                            var vss2 = memory.ReadInt64();
                            var vss3 = memory.ReadInt64();
                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            vv3 = (vv3 + vss3);

                            lpp3re.Add(new LongPoint3Data((long)vv, (long)vv2, (long)vv3));
                            //vv = vss;
                            //vv2 = vss2;
                            //vv3 = vss3;
                        }
                    }
                    return lpp3re as List<T>;
                case "ULongPoint3Data":
                    List<ULongPoint3Data> ulpp3re = new List<ULongPoint3Data>();
                    using (ProtoMemory memory = new ProtoMemory(value))
                    {
                        var vv = (long)memory.ReadInt64();
                        var vv2 = (long)memory.ReadInt64();
                        var vv3 = (long)memory.ReadInt64();
                        ulpp3re.Add(new ULongPoint3Data((ulong)vv, (ulong)vv2, (ulong)vv3));
                        for (int i = 1; i < count; i++)
                        {
                            var vss = memory.ReadInt64();
                            var vss2 = memory.ReadInt64();
                            var vss3 = memory.ReadInt64();
                            vv = (vv + vss);
                            vv2 = (vv2 + vss2);
                            vv3 = (vv3 + vss3);
                            ulpp3re.Add(new ULongPoint3Data((ulong)vv, (ulong)vv2, (ulong)vv3));
                            //vv = vss;
                            //vv2 = vss2;
                            //vv3 = vss3;
                        }
                    }
                    return ulpp3re as List<T>;
            }
            return null;
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="valueCount"></param>
        /// <returns></returns>
        protected Dictionary<int,DateTime> GetTimers(MarshalMemoryBlock source,int sourceAddr,DateTime startTime,DateTime endTime,out int valueCount)
        {
            DateTime sTime = source.ReadDateTime(sourceAddr);

            int timeTick = source.ReadInt(sourceAddr + 8);

            Dictionary<int, DateTime> re = new Dictionary<int, DateTime>();
            var count = source.ReadInt();
            var datasize = source.ReadInt();

            if (datasize <= 0)
            {
                valueCount = 0;
                return re;
            }
            
            byte[] datas = source.ReadBytes(datasize);
            var timers = DeCompressTimers(datas, count);

            DateTime preTimer = DateTime.MinValue;

            for (int i = 0; i < timers.Count; i++)
            {
                DateTime vtime;
                if (timeTick == 100)
                {
                    vtime = sTime.AddMilliseconds((short)timers[i] * timeTick);
                }
                else
                {
                    vtime = sTime.AddMilliseconds(timers[i] * timeTick);
                }

                if (vtime < preTimer) continue;
                if (vtime >= startTime && vtime < endTime)
                    re.Add(i, vtime);
                else if(vtime>endTime && (vtime - endTime).TotalMilliseconds< timeTick)
                {
                    re.Add(i, vtime);
                }
                else if (vtime < startTime && (startTime - vtime).TotalMilliseconds < timeTick)
                {
                    re.Add(i, vtime);
                }
                if(i>0)
                preTimer = vtime;
            }
            valueCount = count;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="timeTick"></param>
        /// <param name="valueCount"></param>
        /// <returns></returns>
        protected Dictionary<int, DateTime> GetTimers(MarshalMemoryBlock source, int sourceAddr, out int valueCount)
        {
            DateTime sTime = source.ReadDateTime(sourceAddr);
            int timeTick = source.ReadInt(sourceAddr + 8);

            Dictionary<int, DateTime> re = new Dictionary<int, DateTime>();
            var count = source.ReadInt();
            var datasize = source.ReadInt();
            byte[] datas = source.ReadBytes(datasize);
            var timers = DeCompressTimers(datas, count);

            for (int i = 0; i < timers.Count; i++)
            {
                if (timeTick == 100)
                {
                    re.Add(i, sTime.AddMilliseconds((short)timers[i] * timeTick));
                }
                else
                {
                    re.Add(i, sTime.AddMilliseconds(timers[i] * timeTick));
                }
            }
            valueCount = count;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <returns></returns>
        protected DateTime GetForceWriteTimers(MarshalMemoryBlock source, int sourceAddr)
        {
            DateTime sTime = source.ReadDateTime(sourceAddr);
            int timeTick = source.ReadInt(sourceAddr + 8);

            var count = source.ReadInt();
            var datasize = source.ReadInt();
            byte[] datas = source.ReadBytes(datasize);
                       
            var timers = DeCompressTimers(datas, count);

            if (timeTick == 100)
            {
                int min = timers[0] * 5;
                double sec = timers[1] / 10;

                return sTime.AddMinutes(-min).AddSeconds(-sec);
            }
            else
            {
                return sTime.AddMilliseconds(timers[0]);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private bool CheckTypeIsPointData(Type type)
        {
            return type == typeof(IntPointData) || type == typeof(UIntPointData) || type == typeof(LongPointData) || type == typeof(ULongPointData) || type == typeof(IntPoint3Data) || type == typeof(UIntPoint3Data) || type == typeof(LongPoint3Data) || type == typeof(ULongPoint3Data);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public override int DeCompressAllValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result)
        {
            int count = 0;
            var timers = GetTimers(source, sourceAddr, startTime, endTime, out count);

            if (timers.Count > 0)
            {
                var valuesize = source.ReadInt();
                var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

                var qusize = source.ReadInt();

                var qulityes = DeCompressQulity(source.ReadBytes(qusize));
                int resultCount = 0;

                if(qulityes.Count>0)
                {

                    if (qulityes[0] >= 100)
                    {
                        //对于由于单位时间内，没有记录而导致系统强制记录的值，读取所有值时忽略不做处理
                        return 0;
                    }
                    else
                    {
                        if (qulityes[0] >= 100 && timers.ContainsKey(0))
                        {
                            if (timers[1] < timers[0])
                            {
                                timers[0] = timers[1].AddMilliseconds(-10);
                            }
                        }
                    }
                }

                for (int i = 0; i < count; i++)
                {
                    //if (qulityes.Count>i && qulityes[i] < 100 && timers.ContainsKey(i) && qulityes[i]!=(byte)QualityConst.Close)
                    if (qulityes.Count > i && (qulityes[i] < 100 || qulityes[i] == (100 + (byte)QualityConst.Init)) && timers.ContainsKey(i))
                    {
                        result.Add<T>(value[i], timers[i], qulityes[i]);
                        resultCount++;
                    }
                }
                return resultCount;
            }
            return 0;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="tp"></param>
        /// <returns></returns>
        public override TagHisValue<T> DeCompressRawValue<T>(MarshalMemoryBlock source, int sourceAddr, byte tp,QueryContext context)
        {
            int count = 0;
            var timers = GetTimers(source, sourceAddr, out count);

            if (timers.Count > 0)
            {
                var valuesize = source.ReadInt();
                var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);
                var qusize = source.ReadInt();
                var qulityes = DeCompressQulity(source.ReadBytes(qusize));

                //对于只有一个值得情况,需要进行重新读取时间内容。

                if (qulityes[0] >= 100)
                {
                    var vtim = GetForceWriteTimers(source, sourceAddr);
                    timers.Clear();
                    timers.Add(0, vtim);

                    var vv = value[0];
                    value.Clear();
                    value.Add(vv);

                    var qu = (byte)(qulityes[0]-100);
                    qulityes.Clear();
                    qulityes.Add(qu);

                    count = 1;
                }
                try
                {
                    if (tp == 0)
                    {
                        //读取最后一个
                        //查找最后一个非辅助记录点
                        for (int i = count - 1; i >= 0; i--)
                        {
                            if (qulityes[i] == (byte)QualityConst.Close || qulityes[i] == (byte)QualityConst.Start)
                            {
                                return TagHisValue<T>.MinValue;
                            }
                            else if (timers.ContainsKey(i))
                            {
                                return new TagHisValue<T>() { Time = timers[i], Quality = (qulityes[i] >= 100) ? (byte)(qulityes[i] - 100) : qulityes[i], Value = value[i] };
                            }
                            //else if (timers.ContainsKey(i) && (qulityes[i] < 100) )
                            //{
                            //    return new TagHisValue<T>() { Time = timers[i], Quality = qulityes[i], Value = value[i] };
                            //}

                        }
                        //辅助记录只有在整个数据段数据都为空的情况下，才会记录所以这里进行优化 ,2022/07/01
                        ////查找最后一个点
                        //for (int i = count - 1; i >= 0; i--)
                        //{
                        //    if (timers.ContainsKey(i))
                        //    {
                        //        return new TagHisValue<T>() { Time = timers[i], Quality = (qulityes[i] >= 100 ) ? (byte)(qulityes[i] - 100) : qulityes[i], Value = value[i] };
                        //    }
                        //}
                    }
                    else
                    {
                        //读取第一个非辅助记录点
                        for (int i = 0; i < count; i++)
                        {
                            if (qulityes[i] == (byte)QualityConst.Close || qulityes[i] == (byte)QualityConst.Start)
                            {
                                return TagHisValue<T>.MinValue;
                            }
                            else if (timers.ContainsKey(i))
                            {
                                return new TagHisValue<T>() { Time = timers[i], Quality = (qulityes[i] >= 100) ? (byte)(qulityes[i] - 100) : qulityes[i], Value = value[i] };
                            }
                            //else if (timers.ContainsKey(i) && (qulityes[i] < 100))
                            //{
                            //    return new TagHisValue<T>() { Time = timers[i], Quality = qulityes[i], Value = value[i] };
                            //}

                        }
                        ////读取第一个点的值
                        //for (int i = 0; i < count; i++)
                        //{
                        //    if (timers.ContainsKey(i))
                        //    {
                        //        return new TagHisValue<T>() { Time = timers[i], Quality = (qulityes[i] >= 100) ? (byte)(qulityes[i] - 100) : qulityes[i], Value = value[i] };
                        //    }
                        //}

                    }
                }
                catch
                {

                }
                finally
                {
                    FillFirstLastValue(value, timers, qulityes, context, count);
                }
            }
            return TagHisValue<T>.Empty;
        }

        private void FillFirstLastValue<T>(List<T> value,Dictionary<int,DateTime> timers,List<byte> qulityes,QueryContext context,int count)
        {
            bool islasthase = false;
            for (int i = count - 1; i >= 0; i--)
            {
                if (qulityes[i] == (byte)QualityConst.Close|| qulityes[i] == (byte)QualityConst.Start)
                {
                    context.LastValue = null;
                    context.LastTime = timers[i];
                    context.LastQuality = qulityes[i];
                    islasthase = true;
                    break;
                }
                else if (timers.ContainsKey(i) && (qulityes[i] < 100))
                {
                    context.LastValue = value[i];
                    context.LastTime = timers[i];
                    context.LastQuality = qulityes[i];
                    islasthase = true;
                    break;
                }

            }
            if (!islasthase)
            {
                //查找最后一个点
                for (int i = count - 1; i >= 0; i--)
                {
                    if (timers.ContainsKey(i))
                    {
                        context.LastValue = value[i];
                        context.LastTime = timers[i];
                        context.LastQuality = (qulityes[i] >= 100) ? (byte)(qulityes[i] - 100) : qulityes[i];
                        islasthase = true;
                        break;
                    }
                }
            }

            bool isfirsthas = false;

            //读取第一个非辅助记录点
            for (int i = 0; i < count; i++)
            {
                if (qulityes[i] == (byte)QualityConst.Close || qulityes[i] == (byte)QualityConst.Start)
                {
                    context.FirstValue = null;
                    context.FirstTime = timers[i];
                    context.FirstQuality = qulityes[i];
                    isfirsthas = true;
                    break;
                }
                else if (timers.ContainsKey(i) && (qulityes[i] < 100))
                {
                    context.FirstValue = value[i];
                    context.FirstTime = timers[i];
                    context.FirstQuality = qulityes[i];
                    isfirsthas = true;
                    break;
                }

            }
            if (!isfirsthas)
            {
                //读取第一个点的值
                for (int i = 0; i < count; i++)
                {
                    if (timers.ContainsKey(i))
                    {
                        context.FirstValue = value[i];
                        context.FirstTime = timers[i];
                        context.FirstQuality = (qulityes[i] >= 100) ? (byte)(qulityes[i] - 100) : qulityes[i];
                        isfirsthas = true;
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="qa"></param>
        /// <returns></returns>
        protected bool IsBadQuality(byte qa)
        {
            return (qa >= (byte)QualityConst.Bad && qa <= (byte)QualityConst.Bad+20) || qa == (byte)QualityConst.Close||qa== (byte)QualityConst.Start;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="qa"></param>
        /// <returns></returns>
        protected bool IsGoodQuality(byte qa)
        {
            return !IsBadQuality(qa);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public override int DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte, QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            if (CheckTypeIsPointData(typeof(T)))
            {
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type, result, ReadOtherDatablockAction,context);
            }

            var hasnext = (bool)context["hasnext"];
            int count = 0;
            var timers = GetTimers(source, sourceAddr, out count);

            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

            var qusize = source.ReadInt();

            var qulityes = DeCompressQulity(source.ReadBytes(qusize));

            //对于只有一个值得情况,需要进行重新读取时间内容。
            if (qulityes[0] >= 100)
            {
                timers.Clear();
                timers.Add(0,GetForceWriteTimers(source, sourceAddr));

                var vv = value[0];
                value.Clear();
                value.Add(vv);

                var qu = (byte)(qulityes[0]-100);
                qulityes.Clear();
                qulityes.Add(qu);

                count = 1;

            }

            DateTime endtime = DateTime.MaxValue;
            if (qulityes.Count > 0)
            {
                //说明此段数据系统退出
                if (qulityes[qulityes.Count - 1] == (byte)QualityConst.Close)
                {
                    endtime = timers.Last().Value;
                }
            }

            var lowfirst = time.Where(e=>e<timers[0] && e<endtime);
            var greatlast = time.Where(e => e<endtime && e > timers[timers.Count - 1]);
            var times = time.Where(e => e >= timers[0] && e <= timers[timers.Count - 1] && e<endtime);
            int resultCount = 0;


            int j = 0;

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间
                var vtmp = ReadOtherDatablockAction(0,context);

                TagHisValue<T>? val = vtmp!=null?(TagHisValue<T>)vtmp:null;

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if(val.HasValue && (val.Value.IsEmpty() || val.Value.Quality == (byte)QualityConst.Close || val.Value.Quality == (byte)QualityConst.Start))
                {
                    val = null;
                    //val = new TagHisValue<T>() { Value = value[0], Quality = qulityes[0], Time = lowfirst.First() };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                                resultCount++;
                            }
                            else
                            {
                                result.Add(default(T), vtime, (byte)QualityConst.Null);
                                resultCount++;
                            }
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue && val.Value.Quality!= (byte)QualityConst.Close)
                            {
                                //如果前置不为空，说明中间没有中断，该数据区域不是第一个，否则为重启后第一个
                                result.Add(value[0], vtime, qulityes[0]);
                            }
                            else
                            {
                                result.Add(default(T), vtime, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                {
                                    var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                    var ffval = (timers[0] - vtime).TotalMilliseconds;

                                    if (ppval < ffval)
                                    {
                                        result.Add(val.Value.Value, vtime, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(value[0], vtime, qulityes[0]);
                                    }
                                    resultCount++;
                                }
                                else
                                {
                                    if (!IsBadQuality(qulityes[0]) && !IsBadQuality(val.Value.Quality))
                                    {
                                        var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                        var tval1 = (timers[0] - val.Value.Time).TotalMilliseconds;
                                        var sval1 = val.Value.Value;
                                        var sval2 = value[0];

                                        var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                        if (pval1 <= 0)
                                        {
                                            //说明数据有异常，则取第一个值
                                            result.Add((object)sval1, vtime, val.Value.Quality);
                                        }
                                        else
                                        {
                                            result.Add((object)val1, vtime, pval1<tval1?val.Value.Quality:qulityes[0]);
                                        }
                                    }
                                    else if (!IsBadQuality(qulityes[0]))
                                    {
                                        result.Add(value[0], vtime, qulityes[0]);
                                    }
                                    else if (!IsBadQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, vtime, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(default(T), vtime, (byte)QualityConst.Null);
                                    }
                                    resultCount++;
                                }
                            }
                            else
                            {
                                result.Add(default(T), vtime, (byte)QualityConst.Null);
                                resultCount++;
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (timers[0] - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(value[0], vtime, qulityes[0]);
                                }
                                
                            }
                            else
                            {
                                result.Add(default(T), vtime, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                for (int i = j; i < timers.Count - 1; i++)
                {
                    var skey = timers[i];

                    var snext = timers[i + 1];
                    j = i;

                    if ((time1==skey) ||(time1 < skey && (skey - time1).TotalSeconds<1))
                    {
                        var val = value[i];
                        result.Add(val, time1, qulityes[i]);
                        resultCount++;
                        
                        break;
                    }
                    else if (time1 > skey && time1 < snext)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = value[i];
                                result.Add(val, time1, qulityes[i]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.After:
                                val = value[i + 1];
                                result.Add(val, time1, qulityes[i+1]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (typeof(T) == typeof(bool)|| typeof(T) == typeof(string)|| typeof(T) == typeof(DateTime))
                                {
                                    var ppval = (time1 - skey).TotalMilliseconds;
                                    var ffval = (snext - time1).TotalMilliseconds;

                                    if (ppval < ffval)
                                    {
                                        val = value[i];
                                        result.Add(val, time1, qulityes[i]);
                                    }
                                    else
                                    {
                                        val = value[i + 1];
                                        result.Add(val, time1, qulityes[i + 1]);
                                    }
                                    resultCount++;
                                }
                                else
                                {
                                    if (!IsBadQuality(qulityes[i]) && !IsBadQuality(qulityes[i + 1]))
                                    {
                                        var pval1 = (time1 - skey).TotalMilliseconds;
                                        var tval1 = (snext - skey).TotalMilliseconds;
                                        var sval1 = value[i];
                                        var sval2 = value[i + 1];

                                        var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);
                                        
                                        result.Add((object)val1, time1, pval1<tval1?qulityes[i]:qulityes[i+1]);
                                    }
                                    else if (!IsBadQuality(qulityes[i]))
                                    {
                                        val = value[i];
                                        result.Add(val, time1, qulityes[i]);
                                    }
                                    else if (!IsBadQuality(qulityes[i + 1]))
                                    {
                                        val = value[i + 1];
                                        result.Add(val, time1, qulityes[i + 1]);
                                    }
                                    else
                                    {
                                        result.Add(default(T), time1, (byte)QualityConst.Null);
                                    }
                                    resultCount++;
                                }
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey).TotalMilliseconds;
                                var fval = (snext - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = value[i];
                                    result.Add(val, time1, qulityes[i]);
                                }
                                else
                                {
                                    val = value[i+1];
                                    result.Add(val, time1, qulityes[i + 1]);
                                }
                                resultCount++;
                                break;
                        }
                        break;
                    }
                    else if (time1 == snext)
                    {
                        var val =value[i + 1];
                        result.Add(val, time1, qulityes[i+1]);
                        resultCount++;
                        break;
                    }

                }
            }

            if (greatlast.Count() > 0 && !hasnext)
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);

                TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                //如果为空值，则说明跨数据文件了，则取第最后一个有效值用作后一个值
                if (val.HasValue && (val.Value.IsEmpty() || (val.Value.Quality == (byte)QualityConst.Close) || val.Value.Quality == (byte)QualityConst.Start))
                {
                    val = new TagHisValue<T>() { Value = value[value.Count - 1], Quality = qulityes[qulityes.Count - 1], Time = greatlast.Last() };
                }

                T pval = value[value.Count-1];
                byte qua = qulityes[qulityes.Count - 1];
                DateTime ptime = timers[timers.Count - 1];
                if(qua >= 100 && value.Count>1)
                {
                    pval = value[value.Count-2];
                    qua = qulityes[qulityes.Count - 2];
                    ptime = timers[timers.Count - 2];
                }
              
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(pval, vtime, qua);
                            resultCount++;
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                                resultCount++;
                            }
                            else
                            {
                                result.Add(default(T), vtime, (byte)QualityConst.Null);
                                resultCount++;
                            }
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                {
                                    var ppval = (vtime - ptime).TotalMilliseconds;
                                    var ffval = (val.Value.Time - vtime).TotalMilliseconds;

                                    if (ppval < ffval)
                                    {
                                        result.Add(pval, vtime, qua);
                                    }
                                    else
                                    {

                                        result.Add(val.Value.Value, vtime, val.Value.Quality);
                                    }
                                    resultCount++;
                                }
                                else
                                {
                                    if (!IsBadQuality(qua) && !IsBadQuality(val.Value.Quality))
                                    {
                                        var pval1 = (val.Value.Time - vtime).TotalMilliseconds;
                                        var tval1 = (val.Value.Time - timers[timers.Count - 1]).TotalMilliseconds;
                                        var sval1 = pval;
                                        var sval2 = val.Value.Value;

                                        var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                        result.Add((object)val1, vtime, pval1<tval1?qua:val.Value.Quality);
                                    }
                                    else if (!IsBadQuality(qua))
                                    {
                                        result.Add(pval, vtime, qua);
                                    }
                                    else if (!IsBadQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, vtime, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(default(T), vtime, (byte)QualityConst.Null);
                                    }
                                    resultCount++;
                                }
                            }
                            else
                            {
                                result.Add(default(T), vtime, (byte)QualityConst.Null);
                                resultCount++;
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - ptime).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (ppval < fval)
                                {
                                    result.Add(pval, vtime, qua);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(default(T), vtime, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                    }
                }
            }


            if (qulityes.Count > 0)
            {
                FillFirstLastValue(value, timers, qulityes, context, count);
            }
            return resultCount;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public override object DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type, Func<byte, QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            if (CheckTypeIsPointData(typeof(T)))
            {
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction,context);
            }
            var hasnext = (bool)context["hasnext"];
            int count = 0;
            var timers = GetTimers(source, sourceAddr + 8,  out count);
            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);
            var qusize = source.ReadInt();
            var qulityes = DeCompressQulity(source.ReadBytes(qusize));

            //对于只有一个值得情况,需要进行重新读取时间内容。
            if (qulityes[0] >= 100)
            {
                timers.Clear();
                timers.Add(0, GetForceWriteTimers(source, sourceAddr));

                var vv = value[0];
                value.Clear();
                value.Add(vv);

                var qu = (byte)(qulityes[0] - 100);
                qulityes.Clear();
                qulityes.Add(qu);

                count = 1;
            }

            DateTime endtime = DateTime.MaxValue;
            if (qulityes.Count > 0)
            {
                //说明此段数据系统退出
                if (qulityes[qulityes.Count - 1] == (byte)QualityConst.Close)
                {
                    endtime = timers.Last().Value;
                }
            }
            try
            {
                //如果超出停止时间
                if (time >= endtime) return null;

                if (timers.Count > 0 && time < timers[0])
                {
                    //如果读取的时间小于，当前数据段的起始时间
                    var vtmp = ReadOtherDatablockAction(0, context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = new TagHisValue<T>() { Value = value[0], Quality = qulityes[0], Time = time };
                    }

                    //  var val = (TagHisValue<T>)ReadOtherDatablockAction(0);
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                                return val.Value.Value;
                            else
                            {
                                return null;
                            }
                        case QueryValueMatchType.After:
                            return value[0];
                        case QueryValueMatchType.Linear:
                            if (!val.HasValue) return null;

                            if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                            {
                                var ppval = (time - val.Value.Time).TotalMilliseconds;
                                var ffval = (timers[0] - time).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    return val.Value.Value;
                                }
                                else
                                {
                                    return value[0];
                                }
                            }
                            else
                            {
                                if (!IsBadQuality(qulityes[0]) && !IsBadQuality(val.Value.Quality))
                                {
                                    var pval1 = (time - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (timers[0] - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = value[0];

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    if (pval1 < 0) val1 = Convert.ToDouble(sval1);

                                    switch (typeof(T).Name)
                                    {
                                        case "Byte":
                                            return (byte)val1;
                                        case "Int16":
                                            return (short)val1;
                                        case "UInt16":
                                            return (ushort)val1;
                                        case "Int32":
                                            return (int)val1;
                                        case "UInt32":
                                            return (uint)val1;
                                        case "Int64":
                                            return (long)val1;
                                        case "UInt64":
                                            return (ulong)val1;
                                        case "Double":
                                            return val1;
                                        case "Single":
                                            return (float)val1;
                                    }
                                }
                                else if (!IsBadQuality(val.Value.Quality))
                                {
                                    return val.Value.Value;
                                }
                                else if (!IsBadQuality(qulityes[0]))
                                {
                                    return value[0];
                                }
                                else
                                {
                                    return null;
                                }
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            if (!val.HasValue) return null;

                            var pval = (time - val.Value.Time).TotalMilliseconds;
                            var fval = (timers[0] - time).TotalMilliseconds;

                            if (pval < fval)
                            {
                                return val.Value.Value;
                            }
                            else
                            {
                                return value[0];
                            }
                    }
                    return null;
                }
                else if (timers.Count > 0 && time > timers[timers.Count - 1])
                {
                    if (hasnext) return null;

                    var vtmp = ReadOtherDatablockAction(1, context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    //var val = (TagHisValue<T>)ReadOtherDatablockAction(1);
                    var valtmp = value[value.Count - 1];
                    var timetmp = timers[timers.Count - 1];
                    var qtmp = qulityes[qulityes.Count - 1];

                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return valtmp;
                        case QueryValueMatchType.After:
                            if (!val.HasValue) return null;
                            return val.Value.Value;
                        case QueryValueMatchType.Linear:
                            if (!val.HasValue) return null;

                            if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                            {
                                var ppval = (time - timetmp).TotalMilliseconds;
                                var ffval = (val.Value.Time - time).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    return valtmp;
                                }
                                else
                                {
                                    return val.Value.Value;
                                }
                            }
                            else
                            {
                                if ((!IsBadQuality(val.Value.Quality)) && (!IsBadQuality(qtmp)))
                                {
                                    var pval1 = (time - timetmp).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - timetmp).TotalMilliseconds;
                                    var sval1 = valtmp;
                                    var sval2 = val.Value.Value;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    string tname = typeof(T).Name;
                                    switch (tname)
                                    {
                                        case "Byte":
                                            return (byte)val1;
                                        case "Int16":
                                            return (short)val1;
                                        case "UInt16":
                                            return (ushort)val1;
                                        case "Int32":
                                            return (int)val1;
                                        case "UInt32":
                                            return (uint)val1;
                                        case "Int64":
                                            return (long)val1;
                                        case "UInt64":
                                            return (ulong)val1;
                                        case "Double":
                                            return val1;
                                        case "Single":
                                            return (float)val1;
                                    }
                                                                      
                                }
                                else if (!IsBadQuality(val.Value.Quality))
                                {
                                    return val.Value.Value;
                                }
                                else if (!IsBadQuality(qtmp))
                                {
                                    return valtmp;
                                }
                                else
                                {
                                    return null;
                                }
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            if (!val.HasValue) return null;

                            var pval = (time - timetmp).TotalMilliseconds;
                            var fval = (val.Value.Time - time).TotalMilliseconds;

                            if (pval < fval)
                            {
                                return valtmp;
                            }
                            else
                            {
                                return val.Value.Value;
                            }
                    }
                    return null;
                }
                else
                {


                    int j = 0;

                    for (int i = j; i < timers.Count - 1; i++)
                    {
                        var skey = timers[i];

                        var snext = timers[i + 1];

                        if ((time == skey) || (time < skey && (skey - time).TotalSeconds < 1))
                        {
                            return value[i];
                        }
                        else if (time > skey && time < snext)
                        {
                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    return value[i];
                                case QueryValueMatchType.After:
                                    return value[i + 1];
                                case QueryValueMatchType.Linear:
                                    if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                    {
                                        var ppval = (time - skey).TotalMilliseconds;
                                        var ffval = (snext - time).TotalMilliseconds;

                                        if (ppval < ffval)
                                        {
                                            return value[i];
                                        }
                                        else
                                        {
                                            return value[i + 1];
                                        }
                                    }
                                    else
                                    {
                                        if (!IsBadQuality(qulityes[i]) && !IsBadQuality(qulityes[i + 1]))
                                        {
                                            var pval1 = (time - skey).TotalMilliseconds;
                                            var tval1 = (snext - skey).TotalMilliseconds;
                                            var sval1 = value[i];
                                            var sval2 = value[i + 1];

                                            var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                            string tname = typeof(T).Name;
                                            switch (tname)
                                            {
                                                case "Byte":
                                                    return (byte)val1;
                                                case "Int16":
                                                    return (short)val1;
                                                case "UInt16":
                                                    return (ushort)val1;
                                                case "Int32":
                                                    return (int)val1;
                                                case "UInt32":
                                                    return (uint)val1;
                                                case "Int64":
                                                    return (long)val1;
                                                case "UInt64":
                                                    return (ulong)val1;
                                                case "Double":
                                                    return val1;
                                                case "Single":
                                                    return (float)val1;
                                            }
                                        }
                                        else if (!IsBadQuality(qulityes[i]))
                                        {
                                            return value[i];
                                        }
                                        else if (!IsBadQuality(qulityes[i + 1]))
                                        {
                                            return value[i + 1];
                                        }
                                        else
                                        {
                                            return null;
                                        }
                                    }
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time - skey).TotalMilliseconds;
                                    var fval = (snext - time).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        return value[i];
                                    }
                                    else
                                    {
                                        return value[i + 1];
                                    }
                            }
                            break;
                        }
                        else if (time == snext)
                        {
                            return value[i + 1];
                        }

                    }

                    return null;
                }
            }
            finally
            {
                if (qulityes.Count > 0)
                {
                    FillFirstLastValue(value, timers, qulityes, context, count);
                }
            }
        }
        

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  object DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time1, int timeTick, QueryValueMatchType type, Func<byte, QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            int count = 0;
            var timers = GetTimers(source, sourceAddr + 8,out count);
            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

            var qusize = source.ReadInt();

            var qulityes = DeCompressQulity(source.ReadBytes(qusize));

            //对于只有一个值得情况,需要进行重新读取时间内容。
            if (qulityes[0] >= 100)
            {
                timers.Clear();
                timers.Add(0, GetForceWriteTimers(source, sourceAddr));

                var vv = value[0];
                value.Clear();
                value.Add(vv);

                var qu = (byte)(qulityes[0] - 100);
                qulityes.Clear();
                qulityes.Add(qu);

                count = 1;
            }

            DateTime endtime = DateTime.MaxValue;
            if (qulityes.Count > 0)
            {
                //说明此段数据系统退出
                if (qulityes[qulityes.Count - 1] == (byte)QualityConst.Close)
                {
                    endtime = timers.Last().Value;
                }
            }
            try
            {
                //如果超出停止时间
                if (time1 >= endtime) return null;

                bool hasnext = (bool)context["hasnext"];
                //如果查询的日期，小于开始时间
                if (timers.Count > 0 && time1 < timers[0])
                {
                    //var val = (TagHisValue<T>)ReadOtherDatablockAction(0);
                    var vtmp = ReadOtherDatablockAction(0, context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = new TagHisValue<T>() { Value = value[0], Quality = qulityes[0], Time = time1 };
                    }

                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (!val.HasValue) return null;
                            return val.Value.Value;
                        case QueryValueMatchType.After:
                            return value[0];
                        case QueryValueMatchType.Linear:
                            if (!val.HasValue) return null;

                            //if ((qulityes[0] < 20 || qulityes[0]==100) && (val.Value.Quality < 20 || val.Value.Quality==100))
                            if (IsGoodQuality(qulityes[0]) && IsGoodQuality(val.Value.Quality))
                            {
                                return (T)LinerValue(val.Value.Time, timers[0], time1, val.Value.Value, value[0]);
                            }
                            else if (IsGoodQuality(val.Value.Quality))
                            {
                                return val.Value.Value;
                            }
                            else if (IsGoodQuality(qulityes[0]))
                            {
                                return value[0];
                            }
                            return null;
                        case QueryValueMatchType.Closed:
                            if (!val.HasValue) return null;
                            var pval = (time1 - val.Value.Time).TotalMilliseconds;
                            var fval = (timers[0] - time1).TotalMilliseconds;

                            if (pval < fval)
                            {
                                return val.Value.Value;
                            }
                            else
                            {
                                return value[0];
                            }

                    }

                }
                else if (timers.Count > 0 && time1 > timers[timers.Count - 1])
                {
                    if (hasnext) return null;

                    //var val = (TagHisValue<T>)ReadOtherDatablockAction(1);
                    var vtmp = ReadOtherDatablockAction(1, context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return value[value.Count - 1];
                        case QueryValueMatchType.After:
                            if (!val.HasValue) return null;
                            return val.Value.Value;
                        case QueryValueMatchType.Linear:
                            if (!val.HasValue) return null;

                            if ((IsGoodQuality(qulityes[qulityes.Count - 1])) && IsGoodQuality(val.Value.Quality))
                            {
                                return (T)LinerValue(timers[timers.Count - 1], val.Value.Time, time1, value[value.Count - 1], val.Value.Value);
                            }
                            else if (IsGoodQuality(val.Value.Quality))
                            {
                                return val.Value.Value;
                            }
                            else if (IsGoodQuality(qulityes[qulityes.Count - 1]))
                            {
                                return value[value.Count - 1];
                            }
                            return null;
                        case QueryValueMatchType.Closed:
                            if (!val.HasValue) return null;

                            var pval = (time1 - timers[timers.Count - 1]).TotalMilliseconds;
                            var fval = (val.Value.Time - time1).TotalMilliseconds;

                            if (pval < fval)
                            {
                                return value[value.Count - 1];
                            }
                            else
                            {
                                return val.Value.Value;
                            }

                    }
                }
                else
                {
                    for (int i = 0; i < timers.Count - 1; i++)
                    {
                        var skey = timers[i];

                        var snext = timers[i + 1];

                        if ((time1 == skey) || (time1 < skey && (skey - time1).TotalSeconds < 1))
                        {
                            return value[i];

                        }
                        else if (time1 > skey && time1 < snext)
                        {
                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    return value[i];
                                case QueryValueMatchType.After:
                                    return value[i + 1];
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qulityes[i]) && IsGoodQuality(qulityes[i + 1]))
                                    {
                                        return (T)LinerValue(skey, snext, time1, value[i], value[i + 1]);
                                    }
                                    else if (IsGoodQuality(qulityes[i]))
                                    {
                                        return value[i];
                                    }
                                    else if (IsGoodQuality(qulityes[i + 1]))
                                    {
                                        return value[i + 1];
                                    }
                                    return null;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey).TotalMilliseconds;
                                    var fval = (snext - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        return value[i];
                                    }
                                    else
                                    {
                                        return value[i + 1];
                                    }

                            }
                            break;
                        }
                        else if (time1 == snext)
                        {
                            return value[i + 1];
                        }

                    }
                }
                return null;
            }
            finally
            {
                if (qulityes.Count > 0)
                {
                    FillFirstLastValue(value, timers, qulityes, context, count);
                }
            }
        }


        #region

        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="time"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        private object LinerValue<T>(DateTime startTime,DateTime endTime,DateTime time,T value1,T value2)
        {
            var pval1 = (time - startTime).TotalMilliseconds;
            var tval1 = (endTime - startTime).TotalMilliseconds;

            string tname = typeof(T).Name;
            switch (tname)
            {
                case "IntPointData":
                    var sval1 = (IntPointData)((object)value1);
                    var sval2 = (IntPointData)((object)value2);
                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                    var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                    return new IntPointData((int)val1, (int)val2);
                case "UIntPointData":
                    var usval1 = (UIntPointData)((object)value1);
                    var usval2 = (UIntPointData)((object)value2);
                    var uval1 = pval1 / tval1 * (Convert.ToDouble(usval2.X) - Convert.ToDouble(usval1.X)) + Convert.ToDouble(usval1.X);
                    var uval2 = pval1 / tval1 * (Convert.ToDouble(usval2.Y) - Convert.ToDouble(usval1.Y)) + Convert.ToDouble(usval1.Y);
                    return new UIntPointData((uint)uval1, (uint)uval2);
                case "LongPointData":
                    var lsval1 = (LongPointData)((object)value1);
                    var lsval2 = (LongPointData)((object)value2);
                    var lval1 = pval1 / tval1 * (Convert.ToDouble(lsval2.X) - Convert.ToDouble(lsval1.X)) + Convert.ToDouble(lsval1.X);
                    var lval2 = pval1 / tval1 * (Convert.ToDouble(lsval2.Y) - Convert.ToDouble(lsval1.Y)) + Convert.ToDouble(lsval1.Y);
                    return new LongPointData((long)lval1, (long)lval2);
                case "ULongPointData":
                    var ulsval1 = (ULongPointData)((object)value1);
                    var ulsval2 = (ULongPointData)((object)value2);
                    var ulval1 = pval1 / tval1 * (Convert.ToDouble(ulsval2.X) - Convert.ToDouble(ulsval1.X)) + Convert.ToDouble(ulsval1.X);
                    var ulval2 = pval1 / tval1 * (Convert.ToDouble(ulsval2.Y) - Convert.ToDouble(ulsval1.Y)) + Convert.ToDouble(ulsval1.Y);
                    return new ULongPointData((ulong)ulval1, (ulong)ulval2);
                case "IntPoint3Data":
                    var s3val1 = (IntPoint3Data)((object)value1);
                    var s3val2 = (IntPoint3Data)((object)value2);
                    var v3al1 = pval1 / tval1 * (Convert.ToDouble(s3val2.X) - Convert.ToDouble(s3val1.X)) + Convert.ToDouble(s3val1.X);
                    var v3al2 = pval1 / tval1 * (Convert.ToDouble(s3val2.Y) - Convert.ToDouble(s3val1.Y)) + Convert.ToDouble(s3val1.Y);
                    var v3al3 = pval1 / tval1 * (Convert.ToDouble(s3val2.Z) - Convert.ToDouble(s3val1.Z)) + Convert.ToDouble(s3val1.Z);
                    return new IntPoint3Data((int)v3al1, (int)v3al2, (int)v3al3);
                case "UIntPoint3Data":
                    var us3val1 = (UIntPoint3Data)((object)value1);
                    var us3val2 = (UIntPoint3Data)((object)value2);
                    var uv3al1 = pval1 / tval1 * (Convert.ToDouble(us3val2.X) - Convert.ToDouble(us3val1.X)) + Convert.ToDouble(us3val1.X);
                    var uva3l2 = pval1 / tval1 * (Convert.ToDouble(us3val2.Y) - Convert.ToDouble(us3val1.Y)) + Convert.ToDouble(us3val1.Y);
                    var uva3l3 = pval1 / tval1 * (Convert.ToDouble(us3val2.Z) - Convert.ToDouble(us3val1.Z)) + Convert.ToDouble(us3val1.Z);
                    return new UIntPoint3Data((uint)uv3al1, (uint)uva3l2, (uint)uva3l3);
                case "LongPoint3Data":
                    var lpsval1 = (LongPoint3Data)((object)value1);
                    var lpsval2 = (LongPoint3Data)((object)value2);
                    var lpval1 = pval1 / tval1 * (Convert.ToDouble(lpsval2.X) - Convert.ToDouble(lpsval1.X)) + Convert.ToDouble(lpsval1.X);
                    var lpval2 = pval1 / tval1 * (Convert.ToDouble(lpsval2.Y) - Convert.ToDouble(lpsval1.Y)) + Convert.ToDouble(lpsval1.Y);
                    var lpval3 = pval1 / tval1 * (Convert.ToDouble(lpsval2.Z) - Convert.ToDouble(lpsval1.Z)) + Convert.ToDouble(lpsval1.Z);
                    return new LongPoint3Data((long)lpval1, (long)lpval2, (long)lpval3);
                case "ULongPoint3Data":
                    var ulpsval1 = (ULongPoint3Data)((object)value1);
                    var ulpsval2 = (ULongPoint3Data)((object)value2);
                    var ulpval1 = pval1 / tval1 * (Convert.ToDouble(ulpsval2.X) - Convert.ToDouble(ulpsval1.X)) + Convert.ToDouble(ulpsval1.X);
                    var ulpval2 = pval1 / tval1 * (Convert.ToDouble(ulpsval2.Y) - Convert.ToDouble(ulpsval1.Y)) + Convert.ToDouble(ulpsval1.Y);
                    var ulpval3 = pval1 / tval1 * (Convert.ToDouble(ulpsval2.Z) - Convert.ToDouble(ulpsval1.Z)) + Convert.ToDouble(ulpsval1.Z);
                    return new ULongPoint3Data((ulong)ulpval1, (ulong)ulpval2, (ulong)ulpval3);
            }

            return default(T);
        }

        
       
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte, QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            int count = 0;
            var timers = GetTimers(source, sourceAddr + 8,out count);

            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

            var qusize = source.ReadInt();

            var qulityes = DeCompressQulity(source.ReadBytes(qusize));

            //对于只有一个值得情况,需要进行重新读取时间内容。
            if (qulityes[0] >= 100)
            {
                timers.Clear();
                timers.Add(0, GetForceWriteTimers(source, sourceAddr));

                var vv = value[0];
                value.Clear();
                value.Add(vv);

                var qu = (byte)(qulityes[0] - 100);
                qulityes.Clear();
                qulityes.Add(qu);

                count = 1;
            }

            DateTime endtime = DateTime.MaxValue;
            if (qulityes.Count > 0)
            {
                //说明此段数据系统退出
                if (qulityes[qulityes.Count - 1] == (byte)QualityConst.Close)
                {
                    endtime = timers.Last().Value;
                }
            }

            int resultCount = 0;

            var lowfirst = time.Where(e => e < timers[0] && e < endtime);
            var greatlast = time.Where(e => e > timers[timers.Count - 1] && e < endtime);
            var times = time.Where(e => e >= timers[0] && e <= timers[timers.Count - 1] && e < endtime);
            

            int j = 0;
            byte qua = 0;
            if (lowfirst.Count() > 0)
            {
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<T>() { Value = value[0], Quality = qulityes[0], Time = lowfirst.First() };
                }
                

                foreach (var time1 in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, time1, val.Value.Quality);
                            }
                            else
                            {
                                result.Add(default(T), time1, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(value[0], time1, qulityes[0]);
                            resultCount++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if ((IsGoodQuality(qulityes[0])) && (IsGoodQuality(val.Value.Quality)))
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (timers[0] - time1).TotalMilliseconds;
                                 
                                    if (pval < fval)
                                    {
                                        qua = val.Value.Quality;
                                    }
                                    else
                                    {
                                        qua = qulityes[0];
                                    }

                                    result.Add(LinerValue(val.Value.Time, timers[0], time1, val.Value.Value, value[0]), time1, qua);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value, time1, val.Value.Quality);
                                }
                                else if ( IsGoodQuality(qulityes[0]))
                                {
                                    result.Add(value[0], time1, qulityes[0]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(default(T), time1, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                var fval = (timers[0] - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(value[0], time1, qulityes[0]);
                                }
                            }
                            else
                            {
                                result.Add(default(T), time1, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                for (int i = j; i < timers.Count - 1; i++)
                {
                    var skey = timers[i];

                    var snext = timers[i + 1];

                    if ((time1 == skey) || (time1 < skey && (skey - time1).TotalSeconds < 1))
                    {
                        var val = value[i];
                        result.Add(val, time1, qulityes[i]);
                        resultCount++;

                        break;
                    }
                    else if (time1 > skey && time1 < snext)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = value[i];
                                result.Add(val, time1, qulityes[i]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.After:
                                val = value[i + 1];
                                result.Add(val, time1, qulityes[i + 1]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.Linear:
                                if ( IsGoodQuality(qulityes[i]) &&  IsGoodQuality(qulityes[i + 1]))
                                {
                                    var tpval = (time1 - skey).TotalMilliseconds;
                                    var tfval = (snext - time1).TotalMilliseconds;

                                    if (tpval < tfval)
                                    {
                                        qua = qulityes[i];
                                    }
                                    else
                                    {
                                        qua = qulityes[i + 1];
                                    }
                                    result.Add(LinerValue(skey, snext, time1, value[i], value[i + 1]), time1, qua);
                                }
                                else if ( IsGoodQuality(qulityes[i]))
                                {
                                    val = value[i];
                                    result.Add(val, time1, qulityes[i]);
                                }
                                else if ( IsGoodQuality(qulityes[i + 1]))
                                {
                                    val = value[i + 1];
                                    result.Add(val, time1, qulityes[i + 1]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                resultCount++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey).TotalMilliseconds;
                                var fval = (snext - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = value[i];
                                    result.Add(val, time1, qulityes[i]);
                                }
                                else
                                {
                                    val = value[i + 1];
                                    result.Add(val, time1, qulityes[i + 1]);
                                }
                                resultCount++;
                                break;
                        }
                        break;
                    }
                    else if (time1 == snext)
                    {
                        var val = value[i + 1];
                        result.Add(val, time1, qulityes[i + 1]);
                        resultCount++;
                        break;
                    }

                }
            }

            if(greatlast.Count()>0)
            {
                //var val = (TagHisValue<T>)ReadOtherDatablockAction(1);

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                foreach (var time1 in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(value[value.Count-1], time1, qulityes[qulityes.Count-1]);
                            resultCount++;
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue)
                            {
                                result.Add(val.Value, time1, val.Value.Quality);
                            }
                            else
                            {
                                result.Add(default(T), time1, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qulityes[qulityes.Count - 1]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval = (time1 - timers[timers.Count - 1]).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        qua = qulityes[qulityes.Count - 1];
                                    }
                                    else
                                    {
                                        qua = val.Value.Quality;
                                    }
                                    result.Add(LinerValue(timers[timers.Count - 1], val.Value.Time, time1, value[value.Count - 1], val.Value.Value), time1, qua);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else if (IsGoodQuality(qulityes[qulityes.Count - 1]))
                                {
                                    result.Add(value[value.Count - 1], time1, qulityes[qulityes.Count - 1]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(default(T), time1, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (time1 - timers[timers.Count - 1]).TotalMilliseconds;
                                var fval = (val.Value.Time - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(value[value.Count - 1], time1, qulityes[qulityes.Count - 1]);

                                }
                                else
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(default(T), time1, (byte)QualityConst.Null);
                            }
                            resultCount++;
                            break;
                    }
                }
            }

            return resultCount;
        }
        
        #endregion


    }
}
