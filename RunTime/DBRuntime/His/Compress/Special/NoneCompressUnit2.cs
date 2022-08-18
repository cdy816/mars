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
using DBRuntime.His;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class NoneCompressUnit2 : CompressUnitbase2
    {
        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 0;

        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "无压缩";

        /// <summary>
        /// 
        /// </summary>
        protected CustomQueue<int> emptys = new CustomQueue<int>(604);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase2 Clone()
        {
            return new NoneCompressUnit2();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="startAddr"></param>
        /// <param name="count"></param>
        /// <param name="value"></param>
        private void CalAvgValue(IMemoryFixedBlock source, long startAddr, out int count, out double value, long size, TagType type)
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
                            var val = source.ReadShort(valueoffset + vid * 2);
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
        private void CalMaxMinValue(IMemoryFixedBlock source, long startAddr, out double maxValue, out DateTime maxTime, out double minValue, out DateTime minTime, long size, TagType type)
        {

            maxValue = double.MinValue; minValue = double.MaxValue;
            maxTime = minTime = DateTime.Now;
            int maxtimeId = -1, mintimeId = -1;

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

            if (maxtimeId > -1 && mintimeId > -1)
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
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="startAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetaddr"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        protected override int NumberStatistic(IMemoryFixedBlock source, long startAddr, IMemoryBlock target, long targetaddr, long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);

            int avgcount = 0;
            double avgvalue = 0, maxvalue = 0, minvalue = 0;
            DateTime maxtime = DateTime.Now, mintime = DateTime.Now;

            CalAvgValue(source, startAddr, out avgcount, out avgvalue, size, type);
            CalMaxMinValue(source, startAddr, out maxvalue, out maxtime, out minvalue, out mintime, size, type);

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
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public override long CompressWithNoStatistic(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size)
        {
            target.WriteDatetime(targetAddr, this.StartTime);
            target.Write((int)(size - this.QulityOffset));//写入值的个数
            target.Write((int)TimeTick);//写入时间间隔
            target.WriteByte((byte)(source as HisDataMemoryBlock).TimeLen);//写入时间字段长度

            if (size > 0)
                source.CopyTo(target, sourceAddr, targetAddr + 17, size);

            return size + 17;
        }

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
        public override long Compress(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, IMemoryBlock statisticTarget, long statisticAddr)
        {
           // LoggerService.Service.Erro("NoneCompressUnit", "目标地址:"+targetAddr +" 数值地址: " + (targetAddr+10) +" 变量个数: "+ (size - this.QulityOffset));
            target.WriteDatetime(targetAddr,this.StartTime);
            target.Write((int)(size - this.QulityOffset));//写入值的个数
            target.Write((int)TimeTick);//写入时间间隔
            target.WriteByte((byte)(source as HisDataMemoryBlock).TimeLen);//写入时间字段长度

            if (size > 0)
                source.CopyTo(target, sourceAddr, targetAddr + 17, size);

            NumberStatistic(source, sourceAddr, statisticTarget, statisticAddr, size, TagType);

            return size + 17;
        }

        /// <summary>
        /// 读取时间戳
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="timeTick"></param>
        /// <param name="startTime"></param>
        /// <returns></returns>
        private Dictionary<int,Tuple<DateTime,bool>> ReadTimeQulity(MarshalMemoryBlock source, int sourceAddr,out int valueCount, out DateTime startTime,out byte timelen)
        {
            source.Position = sourceAddr;

            startTime = source.ReadDateTime();

            //读取值的个数
            int qoffset = source.ReadInt();
            valueCount = qoffset;

            //读取时间单位
            int timeTick = source.ReadInt();

            timelen = source.ReadByte();
            Dictionary<int, Tuple<DateTime, bool>> timeQulities;
            if (timelen == 2)
            {
                var times = source.ReadUShorts(source.Position, qoffset);

                timeQulities = new Dictionary<int, Tuple<DateTime, bool>>(qoffset);

                for (int i = 0; i < times.Count; i++)
                {
                    if (times[i] == 0 && i > 0)
                    {
                        timeQulities.Add(i, new Tuple<DateTime, bool>(startTime, false));
                    }
                    else
                    {
                        timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(times[i] * timeTick), true));
                    }
                }
            }
            else
            {
                var times = source.ReadInts(source.Position, qoffset);

                timeQulities = new Dictionary<int, Tuple<DateTime, bool>>(qoffset);

                for (int i = 0; i < times.Count; i++)
                {
                    if (times[i] == 0 && i > 0)
                    {
                        timeQulities.Add(i, new Tuple<DateTime, bool>(startTime, false));
                    }
                    else
                    {
                        timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(times[i] * timeTick), true));
                    }
                }
            }
            return timeQulities;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
        {

            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time,out timelen);

            //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+1) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadBytes(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;

            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen + 1) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadBytes(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+2) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadShorts(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+2) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadUShorts(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+4) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadInts(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen + 4) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadUInts(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+8) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadLongs(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            DateTime time;

            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadULongs(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+4) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadFloats(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+8) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadDoubleByMemory(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals.Span[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }


            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * (timelen+8) + 17 + sourceAddr, valuecount);

            var valaddr = valuecount * timelen + 17 + sourceAddr;

            var vals = source.ReadDateTimes(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }

            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            DateTime time;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);

            var valaddr = qs.Count * timelen + 17 + sourceAddr;

            int i = 0;
            int rcount = 0;

            List<string> vals = new List<string>();

            source.Position = valaddr;
            for (int ic=0;ic< valuecount; ic++)
            {
                vals.Add(source.ReadStringbyFixSize());
            }

            var qq = source.ReadBytes(valuecount);

            for (i = 0; i < valuecount; i++)
            {
                if (qs[i].Item2 && qq[i] < 100 && qs[i].Item1 >= startTime && qs[i].Item1 < endTime)
                {
                    result.Add(vals[i], qs[i].Item1, qq[i]);
                    rcount++;
                }
            }
            return rcount;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public bool? DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
           
            using (HisQueryResult<bool> re = new HisQueryResult<bool>(1))
            {
                var count = DeCompressBoolValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime);

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);

            var qq = source.ReadBytes(valuecount * (timelen+1) + 17 + sourceAddr, valuecount);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen+ 17 + sourceAddr;

            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<bool>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<bool>? val = vtmp != null ? (TagHisValue<bool>)vtmp : null;

                var valtmp = source.ReadByte(valaddr + findex) > 0;

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<bool>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                                count++;
                            }
                            else
                            {
                                result.Add(false, vtime, (byte)QualityConst.Null);
                            }
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(false, vtime, (byte)QualityConst.Null);
                            }
                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(false, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + icount) > 0;
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {
                        
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadByte(valaddr + icount) > 0;
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                                
                            case QueryValueMatchType.After:
                                val = source.ReadByte(valaddr + icount1) > 0;
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;
                                
                                if (pval < fval)
                                {
                                    val = source.ReadByte(valaddr + icount) > 0;
                                    result.Add(val, time1, qq[skey.Key]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadByte(valaddr + icount1) > 0;
                                    result.Add(val, time1, qq[snext.Key]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + icount1) > 0;
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }

                }
            }

            if (greatlast.Count() > 0 &&!((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<bool>)ReadOtherDatablockAction(1);
                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<bool>? val = vtmp != null ? (TagHisValue<bool>)vtmp : null;

                var valtmp = source.ReadByte(valaddr + flast) > 0;
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            }
                            else
                            {
                                result.Add(false, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var ffval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {

                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(false, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(false, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            return count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="qa"></param>
        /// <returns></returns>
        protected bool IsBadQuality(byte qa)
        {
            return qa >= (byte)QualityConst.Bad && qa <= (byte)QualityConst.Bad + 20;
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
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  byte? DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<byte> re = new HisQueryResult<byte>(1))
            {
                var count = DeCompressByteValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            //var qs = ReadTimeQulity(source, sourceAddr,out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * (timelen+1)+ 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;

            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<byte>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<byte>? val = vtmp != null ? (TagHisValue<byte>)vtmp : null;
                var valtmp = source.ReadByte(valaddr + findex);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<byte>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            }
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if ( IsGoodQuality( qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(0, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }
                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                //for (int i = 0; i < vv.Length - 1; i++)
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + icount);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadByte(valaddr + icount);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = source.ReadByte(valaddr + icount1);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadByte(valaddr + icount);
                                    var sval2 = source.ReadByte(valaddr + icount1);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadByte(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadByte(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadByte(valaddr + icount);
                                    result.Add(val, time1, qq[skey.Key]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadByte(valaddr + icount1);
                                    result.Add(val, time1, qq[snext.Key]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + icount1);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }

                }
            }

            if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<byte>)ReadOtherDatablockAction(1);
                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<byte>? val = vtmp != null ? (TagHisValue<byte>)vtmp : null;
                var valtmp = source.ReadByte(valaddr + flast);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    //if ( qq[flast] < 20 && val.Value.Quality < 20)
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time-vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(0, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
           // FillFirstLastValue<byte>(val)
            return count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  DateTime? DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<DateTime> re = new HisQueryResult<DateTime>(1))
            {
                var count = DeCompressDateTimeValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr,out valuecount, out stime);

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);

            var qq = source.ReadBytes(qs.Count * (timelen+8) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;

            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<DateTime>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<DateTime>? val = vtmp != null ? (TagHisValue<DateTime>)vtmp : null;

                var valtmp = source.ReadDateTime(valaddr + findex * 8);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<DateTime>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            }
                            else
                            {
                                result.Add(DateTime.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(DateTime.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(DateTime.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                // for (int i = 0; i < vv.Length - 1; i++)
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadDateTime(valaddr + icount * 8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadDateTime(valaddr + icount * 8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = source.ReadDateTime(valaddr + icount1 * 8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadDateTime(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadDateTime(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadDateTime(valaddr + icount1 * 8);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }

                }
            }

            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<DateTime>)ReadOtherDatablockAction(1);
                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<DateTime>? val = vtmp != null ? (TagHisValue<DateTime>)vtmp : null;
                var valtmp = source.ReadDateTime(valaddr + flast * 8);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(DateTime.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var ffval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {

                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(DateTime.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(DateTime.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  double? DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<double> re = new HisQueryResult<double>(1))
            {
                var count = DeCompressDoubleValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime);

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);

            var qq = source.ReadBytes(qs.Count * (timelen+8) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;

            int count = 0;

            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<double>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<double>? val = vtmp != null ? (TagHisValue<double>)vtmp : null;

                var valtmp = source.ReadDouble(valaddr + findex * 8);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<double>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                            {
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            }
                            else
                            {
                                result.Add(double.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(0, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(double.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(double.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                //for (int i = 0; i < vv.Length - 1; i++)
                while(icount<vv.Length-1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }
               
                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadDouble(valaddr + icount * 8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadDouble(valaddr + icount * 8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadDouble(valaddr + icount1 * 8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadDouble(valaddr + icount * 8);
                                    var sval2 = source.ReadDouble(valaddr + icount1 * 8);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadDouble(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadDouble(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadDouble(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadDouble(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }
                       
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadDouble(valaddr + icount1 * 8);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<double>)ReadOtherDatablockAction(1);
                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<double>? val = vtmp != null ? (TagHisValue<double>)vtmp : null;
                var valtmp = source.ReadDouble(valaddr + flast * 8);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(double.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(double.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(double.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(double.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="ReadOtherDatablockAction"></param>
        /// <returns></returns>
        public  float? DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<float> re = new HisQueryResult<float>(1))
            {
                var count = DeCompressFloatValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<float> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            //var qs = ReadTimeQulity(source, sourceAddr,out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * (timelen+4) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;

            int count = 0;
            int icount = 0;
            int icount1 = 0;
            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<float>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<float>? val = vtmp != null ? (TagHisValue<float>)vtmp : null;

                var valtmp = source.ReadFloat(valaddr + findex * 4);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<float>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadFloat(valaddr + icount * 4);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadFloat(valaddr + icount * 4);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadFloat(valaddr + icount1 * 4);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadFloat(valaddr + icount * 4);
                                    var sval2 = source.ReadFloat(valaddr + icount1 * 4);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((float)val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadFloat(valaddr + icount * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadFloat(valaddr + icount1 * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadFloat(valaddr + icount * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadFloat(valaddr + icount1 * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadFloat(valaddr + icount1 * 4);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }
            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<float>? val = vtmp != null ? (TagHisValue<float>)vtmp : null;
                var valtmp = source.ReadFloat(valaddr + flast * 4);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(float.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  int? DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<int> re = new HisQueryResult<int>(1))
            {
                var count = DeCompressIntValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<int> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            var qq = source.ReadBytes(qs.Count * (timelen+4) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;
            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<int>? val = vtmp != null ? (TagHisValue<int>)vtmp : null;
                var valtmp = source.ReadInt(valaddr + findex * 4);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<int>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }
                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;
                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadInt(valaddr + icount * 4);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadInt(valaddr + icount * 4);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadInt(valaddr + icount1 * 4);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadInt(valaddr + icount * 4);
                                    var sval2 = source.ReadInt(valaddr + icount1 * 4);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((int)val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadInt(valaddr + icount * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadInt(valaddr + icount1 * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadInt(valaddr + icount * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadInt(valaddr + icount1 * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadInt(valaddr + icount1 * 4);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<int>? val = vtmp != null ? (TagHisValue<int>)vtmp : null;
                var valtmp = source.ReadInt(valaddr + flast * 4);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(int.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  long? DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<long> re = new HisQueryResult<long>(1))
            {
                var count = DeCompressLongValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<long> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            var qq = source.ReadBytes(qs.Count * (timelen+8) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;

            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<long>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<long>? val = vtmp != null ? (TagHisValue<long>)vtmp : null;
                var valtmp = source.ReadLong(valaddr + findex * 8);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<long>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(long.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(0, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(0, vtime, (byte)QualityConst.Null);
                            }

                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                //for (int i = 0; i < vv.Length - 1; i++)
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadLong(valaddr + icount * 8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadLong(valaddr + icount * 8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadLong(valaddr + icount1 * 8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadLong(valaddr + icount * 8);
                                    var sval2 = source.ReadLong(valaddr + icount1 * 8);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadLong(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadLong(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadLong(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadLong(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadLong(valaddr + icount1 * 8);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<long>? val = vtmp != null ? (TagHisValue<long>)vtmp : null;
                var valtmp = source.ReadLong(valaddr + flast * 8);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(long.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(long.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(long.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(long.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  short? DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<short> re = new HisQueryResult<short>(1))
            {
                var count = DeCompressShortValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        private int FindFirstAvaiableIndex(KeyValuePair<int, Tuple<DateTime, bool>>[] times)
        {
            int i = 0;
            foreach(var vv in times)
            {
                if(vv.Value.Item2)
                {
                    return i;
                }
                i++;
            }
            return -1;
        }

        private int FindLastAvaiableIndex(KeyValuePair<int,Tuple<DateTime,bool>>[] times)
        {
            int i = 0;
            for(i=times.Length-1;i>=0;i--)
            {
                if(times[i].Value.Item2)
                {
                    return i;
                }
            }
            return -1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public int DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {

            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            var qq = source.ReadBytes(qs.Count * (timelen + 2) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;
            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<short>? val = vtmp != null ? (TagHisValue<short>)vtmp : null;
                var valtmp = source.ReadShort(valaddr + findex * 2);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<short>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                //for (int i = 0; i < vv.Length - 1; i++)
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadShort(valaddr + icount * 2);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadShort(valaddr + icount * 2);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadShort(valaddr + icount1 * 2);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadShort(valaddr + icount * 2);
                                    var sval2 = source.ReadShort(valaddr + icount1 * 2);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadShort(valaddr + icount * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadShort(valaddr + icount1 * 2);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadShort(valaddr + icount * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadShort(valaddr + icount1 * 2);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadShort(valaddr + icount1 * 2);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<short>? val = vtmp != null ? (TagHisValue<short>)vtmp : null;
                var valtmp = source.ReadShort(valaddr + flast * 2);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;



        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  string DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<string> re = new HisQueryResult<string>(1))
            {
                var count = DeCompressStringValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {

            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);

            //var dtmp = source.ToStringList(sourceAddr + 12, Encoding.Unicode);

            var valaddr = qs.Count * timelen + 17 + sourceAddr;
            Dictionary<int, string> dtmp = new Dictionary<int, string>();
            source.Position = valaddr;

            for (int i = 0; i < qs.Count; i++)
            {
                dtmp.Add(i, source.ReadString());
            }

            var qq = source.ReadBytes(qs.Count);

            var vv = qs.ToArray();
            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<string>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<string>? val = vtmp != null ? (TagHisValue<string>)vtmp : null;

                var valtmp = dtmp[findex];
                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<string>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add("", vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add("", vtime, (byte)QualityConst.Null);
                            }
                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add("", vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                //for (int i = 0; i < vv.Length - 1; i++)
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = dtmp[icount];
                        // var val = source.ReadShort(valaddr + icount * 2);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = dtmp[icount];

                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = dtmp[icount1];
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = dtmp[icount];
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = dtmp[icount1];
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = dtmp[icount1];
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<string>? val = vtmp != null ? (TagHisValue<string>)vtmp : null;
                var valtmp = dtmp[flast];
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if(val.HasValue)
                            result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add("", vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var ffval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {

                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add("", vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add("", vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
            
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  uint? DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<uint> re = new HisQueryResult<uint>(1))
            {
                var count = DeCompressUIntValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            var qq = source.ReadBytes(qs.Count * (timelen+4) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;
            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<uint>? val = vtmp != null ? (TagHisValue<uint>)vtmp : null;
                var valtmp = source.ReadUInt(valaddr + findex * 4);
                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<uint>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }
                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadUInt(valaddr + icount * 4);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadUInt(valaddr + icount * 4);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadUInt(valaddr + icount1 * 4);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadUInt(valaddr + icount * 4);
                                    var sval2 = source.ReadUInt(valaddr + icount1 * 4);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((uint)val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadUInt(valaddr + icount * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadUInt(valaddr + icount1 * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadUInt(valaddr + icount * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadUInt(valaddr + icount1 * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadUInt(valaddr + icount1 * 4);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<uint>? val = vtmp != null ? (TagHisValue<uint>)vtmp : null;
                var valtmp = source.ReadUInt(valaddr + flast * 4);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(uint.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }

            return count;
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  ulong? DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<ulong> re = new HisQueryResult<ulong>(1))
            {
                var count = DeCompressULongValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            var qq = source.ReadBytes(qs.Count * (timelen+8) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;
            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                //var val = (TagHisValue<long>)ReadOtherDatablockAction(0);
                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<ulong>? val = vtmp != null ? (TagHisValue<ulong>)vtmp : null;
                var valtmp = source.ReadULong(valaddr + findex * 8);

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<ulong>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }

                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                //for (int i = 0; i < vv.Length - 1; i++)
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadULong(valaddr + icount * 8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadULong(valaddr + icount * 8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadULong(valaddr + icount1 * 8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear: 
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadULong(valaddr + icount * 8);
                                    var sval2 = source.ReadULong(valaddr + icount1 * 8);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadULong(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadULong(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadULong(valaddr + icount * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadULong(valaddr + icount1 * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadULong(valaddr + icount1 * 8);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<ulong>? val = vtmp != null ? (TagHisValue<ulong>)vtmp : null;
                var valtmp = source.ReadULong(valaddr + flast * 8);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(ulong.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  ushort? DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<ushort> re = new HisQueryResult<ushort>(1))
            {
                var count = DeCompressUShortValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return null;
                }
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);
            var qq = source.ReadBytes(qs.Count * (timelen+2) + 17 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;
            int count = 0;
            int icount = 0;
            int icount1 = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(0,context);
                TagHisValue<ushort>? val = vtmp != null ? (TagHisValue<ushort>)vtmp : null;
                var valtmp = source.ReadUShort(valaddr + findex * 2);
                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && val.Value.IsEmpty())
                {
                    val = new TagHisValue<ushort>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) };
                }
                foreach (var vtime in lowfirst)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(ushort.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            result.Add(valtmp, vtime, qq[findex]);
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                var ffval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (vv[findex].Value.Item1 - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[findex]))
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(ushort.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(ushort.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;

                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - val.Value.Time).TotalMilliseconds;
                                var fval = (vv[findex].Value.Item1 - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(valtmp, vtime, qq[findex]);
                                }
                            }
                            else
                            {
                                result.Add(ushort.MinValue, vtime, (byte)QualityConst.Null);
                            }

                            count++;
                            break;
                    }
                }
            }

            foreach (var time1 in times)
            {
                while (icount < vv.Length - 1)
                {
                    while (!vv[icount].Value.Item2)
                    {
                        icount++;
                        if (icount > (vv.Length - 1))
                            return count;
                    }

                    var skey = vv[icount];

                    icount1 = icount + 1;

                    while (!vv[icount1].Value.Item2)
                    {
                        icount1++;
                        if (icount1 > (vv.Length))
                            return count;
                    }

                    var snext = vv[icount1];

                    if (time1 <= skey.Value.Item1)
                    {
                        var val = source.ReadUShort(valaddr + icount * 2);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadUShort(valaddr + icount * 2);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadUShort(valaddr + icount1 * 2);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadUShort(valaddr + icount * 2);
                                    var sval2 = source.ReadUShort(valaddr + icount1 * 2);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (IsGoodQuality(qq[skey.Key]))
                                {
                                    val = source.ReadUShort(valaddr + icount * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (IsGoodQuality(qq[snext.Key]))
                                {
                                    val = source.ReadUShort(valaddr + icount1 * 2);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadUShort(valaddr + icount * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadUShort(valaddr + icount1 * 2);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadUShort(valaddr + icount1 * 2);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }
                    icount = icount1;
                }
            }

            if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1,context);
                TagHisValue<ushort>? val = vtmp != null ? (TagHisValue<ushort>)vtmp : null;
                var valtmp = source.ReadUShort(valaddr + flast * 2);
                foreach (var vtime in greatlast)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            result.Add(valtmp, vtime, qq[flast]);
                            count++;
                            break;
                        case QueryValueMatchType.After:
                            if (val.HasValue)
                                result.Add(val.Value.Value, vtime, val.Value.Quality);
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                {
                                    var pval1 = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                    var tval1 = (val.Value.Time - vv[flast].Value.Item1).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = valtmp;

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval1) - Convert.ToDouble(sval2)) + Convert.ToDouble(sval2);

                                    result.Add((object)val1, vtime, 0);
                                }
                                else if (IsGoodQuality(qq[flast]))
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else if (IsGoodQuality(val.Value.Quality))
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                                }
                            }
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                        case QueryValueMatchType.Closed:
                            if (val.HasValue)
                            {
                                var pval = (vtime - vv[flast].Value.Item1).TotalMilliseconds;
                                var fval = (val.Value.Time - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(valtmp, vtime, qq[flast]);
                                }
                                else
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                            }
                            else
                            {
                                result.Add(short.MinValue, vtime, (byte)QualityConst.Null);
                            }
                            count++;
                            break;
                    }
                }
            }
            return count;
            
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
            if (typeof(T) == typeof(bool))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<bool>);
            }
            else if (typeof(T) == typeof(short))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<short>);
            }
            else if (typeof(T) == typeof(ushort))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<ushort>);
            }
            else if (typeof(T) == typeof(int))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<int>);
            }
            else if (typeof(T) == typeof(uint))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<uint>);
            }
            else if (typeof(T) == typeof(long))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<long>);
            }
            else if (typeof(T) == typeof(ulong))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<ulong>);
            }
            else if (typeof(T) == typeof(double))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<double>);
            }
            else if (typeof(T) == typeof(float))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<float>);
            }
            else if (typeof(T) == typeof(byte))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<byte>);
            }
            else if (typeof(T) == typeof(string))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<string>);
            }
            else if (typeof(T) == typeof(DateTime))
            {
                return DeCompressAllValue(source, sourceAddr, startTime, endTime, timeTick, result as HisQueryResult<DateTime>);
            }
            else
            {
                return DeCompressAllPointValue(source, sourceAddr, startTime, endTime, timeTick, result);
            }
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
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);
            if (typeof(T) == typeof(bool))
            {
                #region bool
                //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 1) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadBytes(valaddr, valuecount);
                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close )
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<bool>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i]>0 });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<bool>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i]>0 });
                        }
                    }
                }
                #endregion
            }
            else if(typeof(T) == typeof(byte))
            {
                #region byte
                //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 1) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadBytes(valaddr, valuecount);
                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<byte>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<byte>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(short))
            {
                #region short
                //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 2) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadShorts(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<short>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<short>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(ushort))
            {
                #region ushort
                //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 2) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadUShorts(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ushort>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ushort>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(int))
            {
                #region int
                //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 4) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadInts(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<int>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<int>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(uint))
            {
                #region unit
                //读取质量戳,时间戳2/4个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 4) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadUInts(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<uint>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<uint>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(long))
            {
                #region long
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadLongs(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<long>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close )
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<long>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(ulong))
            {
                #region ulong
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadULongs(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ulong>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close )
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ulong>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(DateTime))
            {
                #region datetime
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadDateTimes(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close )
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<DateTime>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<DateTime>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(double))
            {
                #region double
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadDoubleByMemory(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<double>() { Quality = qq[i], Time = qs[i].Item1, Value = vals.Span[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<double>() { Quality = qq[i], Time = qs[i].Item1, Value = vals.Span[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(float))
            {
                #region float
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 4) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadFloats(valaddr, valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<double>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<double>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(string))
            {
                #region string
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var valaddr = qs.Count * timelen + 17 + sourceAddr;

                List<string> vals = new List<string>();

                source.Position = valaddr;
                for (int ic = 0; ic < valuecount; ic++)
                {
                    vals.Add(source.ReadStringbyFixSize());
                }

                var qq = source.ReadBytes(valuecount);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i--)
                    {
                        if (qq[i] == (byte)QualityConst.Close )
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<string>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i++)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<string>() { Quality = qq[i], Time = qs[i].Item1, Value = vals[i] });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(IntPointData))
            {
                #region IntPointData
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadInts(valaddr, valuecount * 2);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i=i-2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<IntPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new IntPointData(vals[i-1], vals[i])});
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i=i+2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<IntPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new IntPointData(vals[i], vals[i + 1]) });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                #region UIntPointData
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadUInts(valaddr, valuecount * 2);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i = i - 2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }

                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<UIntPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new UIntPointData(vals[i - 1], vals[i]) });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i = i + 2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<UIntPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new UIntPointData(vals[i], vals[i + 1]) });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                #region IntPoint3Data
                //读取质量戳,时间戳2个字节，值12个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 12) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadInts(valaddr, valuecount * 3);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i = i - 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<IntPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new IntPoint3Data(vals[i - 2], vals[i - 1], vals[i]) });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i = i + 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<IntPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new IntPoint3Data(vals[i], vals[i + 1], vals[i + 2]) });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                #region UIntPoint3Data
                //读取质量戳,时间戳2个字节，值12个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 12) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadUInts(valaddr, valuecount * 3);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i = i - 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<UIntPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new UIntPoint3Data(vals[i - 2], vals[i - 1], vals[i]) });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i = i + 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<UIntPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new UIntPoint3Data(vals[i], vals[i + 1], vals[i + 2]) });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                #region LongPointData
                //读取质量戳,时间戳2个字节，值16个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 16) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadLongs(valaddr, valuecount * 2);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i = i - 2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<LongPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new LongPointData(vals[i - 1], vals[i]) });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i = i + 2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<LongPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new LongPointData(vals[i], vals[i + 1]) });
                        }
                    }
                }
                #endregion
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 16) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadULongs(valaddr, valuecount * 2);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i = i - 2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ULongPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new ULongPointData(vals[i - 1], vals[i]) });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i = i + 2)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ULongPointData>() { Quality = qq[i], Time = qs[i].Item1, Value = new ULongPointData(vals[i], vals[i + 1]) });
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                //读取质量戳,时间戳2个字节，值24个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 24) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadLongs(valaddr, valuecount * 3);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i = i - 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<LongPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new LongPoint3Data(vals[i - 2], vals[i - 1], vals[i]) });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i = i + 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<LongPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new LongPoint3Data(vals[i], vals[i + 1], vals[i + 2]) });
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 24) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadULongs(valaddr, valuecount * 3);

                if (tp == 0)
                {
                    //读最后一个
                    for (int i = valuecount - 1; i >= 0; i = i - 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i] < 100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ULongPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new ULongPoint3Data(vals[i - 2], vals[i - 1], vals[i]) });
                        }
                    }
                }
                else
                {
                    for (int i = 0; i < valuecount; i = i + 3)
                    {
                        if (qq[i] == (byte)QualityConst.Close)
                        {
                            return TagHisValue<T>.MinValue;
                        }
                        if (qs[i].Item2 && qq[i]<100)
                        {
                            return (TagHisValue<T>)((object)new TagHisValue<ULongPoint3Data>() { Quality = qq[i], Time = qs[i].Item1, Value = new ULongPoint3Data(vals[i], vals[i + 1], vals[i + 2]) });
                        }
                    }
                }
            }


            return TagHisValue<T>.Empty;
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
        public override object DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte, QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
           
            if (typeof(T) == typeof(bool))
            {
                return ((object)DeCompressBoolValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));
            }
            else if (typeof(T) == typeof(byte))
            {
                return ((object)DeCompressByteValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(short))
            {
                return ((object)DeCompressShortValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(ushort))
            {
                return ((object)DeCompressUShortValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(int))
            {
                return ((object)DeCompressIntValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(uint))
            {
                return ((object)DeCompressUIntValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(long))
            {
                return ((object)DeCompressLongValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(ulong))
            {
                return ((object)DeCompressULongValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(double))
            {
                return DeCompressDoubleValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context);
            }
            else if (typeof(T) == typeof(float))
            {
                return DeCompressFloatValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context);
            }
            else if (typeof(T) == typeof(DateTime))
            {
                return ((object)DeCompressDateTimeValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));

            }
            else if (typeof(T) == typeof(string))
            {
                return ((object)DeCompressStringValue(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context));
            }
            else
            {
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context);
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
        /// <param name="result"></param>
        /// <returns></returns>
        public override int DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            if (typeof(T) == typeof(bool))
            {
                return DeCompressBoolValue(source, sourceAddr, time, timeTick,type, result as HisQueryResult<bool>, ReadOtherDatablockAction, context);
            }
            else if (typeof(T) == typeof(byte))
            {
                return DeCompressByteValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<byte>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(short))
            {
                return DeCompressShortValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<short>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(ushort))
            {
                return DeCompressUShortValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<ushort>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(int))
            {
                return DeCompressIntValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<int>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(uint))
            {
                return DeCompressUIntValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<uint>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(long))
            {
                return DeCompressLongValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<long>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(ulong))
            {
                return DeCompressULongValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<ulong>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(DateTime))
            {
                return DeCompressDateTimeValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<DateTime>, ReadOtherDatablockAction, context);

            }
            else if (typeof(T) == typeof(string))
            {
                return DeCompressStringValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<string>, ReadOtherDatablockAction, context);
            }
            else if (typeof(T) == typeof(double))
            {
                return DeCompressDoubleValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<double>, ReadOtherDatablockAction, context);
            }
            else if (typeof(T) == typeof(float))
            {
                return DeCompressFloatValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<float>, ReadOtherDatablockAction, context);
            }
            else
            {
                return DeCompressPointValue(source, sourceAddr, time,timeTick,type, result, ReadOtherDatablockAction, context);
            }

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
        public  int DeCompressAllPointValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result)
        {
            DateTime time;

            int valuecount = 0;

            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out time, out timelen);


            int i = 0;
            int rcount = 0;

            if (typeof(T) == typeof(IntPointData))
            {
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen+8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadInts(valaddr, valuecount * 2);

                for (i = 0; i < vals.Count; i=i+2)
                {
                    int j = i / 2;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i],  vals[i + 1] , qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 8) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadUInts(valaddr, valuecount * 2);

                for (i = 0; i < vals.Count; i = i + 2)
                {
                    int j = i / 2;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i], vals[i + 1], qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                //读取质量戳,时间戳2个字节，值12个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 12) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadInts(valaddr, valuecount * 3);

                for (i = 0; i < vals.Count; i = i + 3)
                {
                    int j = i / 3;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i], vals[i + 1],vals[i + 2], qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                //读取质量戳,时间戳2个字节，值12个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 12) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadUInts(valaddr, valuecount * 3);

                for (i = 0; i < vals.Count; i = i + 3)
                {
                    int j = i / 3;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i], vals[i + 1], vals[i + 2], qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                //读取质量戳,时间戳2个字节，值16个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 16) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadLongs(valaddr, valuecount * 2);

                for (i = 0; i < vals.Count; i = i + 2)
                {
                    int j = i / 2;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i], vals[i + 1], qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 16) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadULongs(valaddr, valuecount * 2);

                for (i = 0; i < vals.Count; i = i + 2)
                {
                    int j = i / 2;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i], vals[i + 1], qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                //读取质量戳,时间戳2个字节，值24个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 24) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadLongs(valaddr, valuecount * 3);

                for (i = 0; i < vals.Count; i = i + 3)
                {
                    int j = i / 3;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i], vals[i + 1], vals[i + 2], qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * (timelen + 24) + 17 + sourceAddr, valuecount);

                var valaddr = valuecount * timelen + 17 + sourceAddr;

                var vals = source.ReadULongs(valaddr, valuecount * 3);

                for (i = 0; i < vals.Count; i = i + 3)
                {
                    int j = i / 3;
                    if (qs[j].Item2 && qq[j] < 100 && qs[j].Item1 >= startTime && qs[j].Item1 < endTime)
                    {
                        result.AddPoint(vals[i], vals[i + 1], vals[i + 2], qs[j].Item1, qq[j]);
                        rcount++;
                    }
                }
            }
            return rcount;
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
        public  T DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            using (HisQueryResult<T> re = new HisQueryResult<T>(1))
            {
                var count = DeCompressPointValue(source, sourceAddr, new List<DateTime>() { time }, timeTick, type, re, ReadOtherDatablockAction,context);
                if (count > 0 && re.GetQuality(0) != (byte)QualityConst.Null)
                {
                    return re.GetValue(0);
                }
                else
                {
                    return default(T);
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
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result,  Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        {
            DateTime stime;
            int valuecount = 0;
            byte timelen = 0;

            var qs = ReadTimeQulity(source, sourceAddr, out valuecount, out stime, out timelen);

            var vv = qs.ToArray();

            var valaddr = qs.Count * timelen + 17 + sourceAddr;

            int count = 0;

            var findex = FindFirstAvaiableIndex(vv);
            var flast = FindLastAvaiableIndex(vv);

            var lowfirst = time.Where(e => e < vv[findex].Value.Item1);
            var greatlast = time.Where(e => e > vv[flast].Value.Item1);
            var times = time.Where(e => e >= vv[findex].Value.Item1 && e <= vv[flast].Value.Item1);

            if (typeof(T) == typeof(IntPointData))
            {
                var qq = source.ReadBytes(qs.Count * (timelen+8) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    //var val = (TagHisValue<T>)ReadOtherDatablockAction(0);
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new IntPointData(source.ReadInt(valaddr + findex * 4), source.ReadInt(valaddr + (findex + 1) * 4));

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<IntPointData>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }

                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if(val.HasValue)
                                result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadInt(valaddr + i * 4);
                            var val2 = source.ReadInt(valaddr + (i+1) * 4);
                            result.AddPoint(val,val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadInt(valaddr + i * 4);
                                    var val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:
                                    val = source.ReadInt(valaddr + (i+2) * 4);
                                    val2 = source.ReadInt(valaddr + (i + 3) * 4);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadInt(valaddr + i * 4);
                                        var sval12 = source.ReadInt(valaddr + (i + 1) * 4);
                                        var sval2 = source.ReadInt(valaddr + (i + 2) * 4);
                                        var sval22 = source.ReadInt(valaddr + (i + 3) * 4);
                                        var x1 = (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (int)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        result.AddPoint(x1, x2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadInt(valaddr + i * 4);
                                        val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadInt(valaddr + (i + 2) * 4);
                                        val2 = source.ReadInt(valaddr + (i + 3) * 4);
                                        result.AddPoint(val, val2, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadInt(valaddr + i * 4);
                                        val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadInt(valaddr + (i + 2) * 4);
                                        val2 = source.ReadInt(valaddr + (i + 3) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadInt(valaddr + (i + 2) * 4);
                            var val2 = source.ReadInt(valaddr + (i + 3) * 4);
                            result.AddPoint(val, val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }

                if (greatlast.Count() > 0&& !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new IntPointData(source.ReadInt(valaddr + flast * 4), source.ReadInt(valaddr + (flast + 1) * 4));

                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * (timelen + 12) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    var valtmp = new IntPoint3Data(source.ReadInt(valaddr + findex * 4), source.ReadInt(valaddr + (findex + 1) * 4), source.ReadInt(valaddr + (findex + 2) * 4));

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<IntPoint3Data>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }

                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if (val.HasValue)
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadInt(valaddr + i * 4);
                            var val2 = source.ReadInt(valaddr + (i + 1) * 4);
                            var val3 = source.ReadInt(valaddr + (i + 2) * 4);
                            result.AddPoint(val, val2,val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadInt(valaddr + i * 4);
                                    var val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                    var val3 = source.ReadInt(valaddr + (i + 2) * 4);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:

                                    val = source.ReadInt(valaddr + (i+3) * 4);
                                    val2 = source.ReadInt(valaddr + (i +4) * 4);
                                    val3 = source.ReadInt(valaddr + (i + 5) * 4);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadInt(valaddr + i * 4);
                                        var sval12 = source.ReadInt(valaddr + (i + 1) * 4);
                                        var sval13 = source.ReadInt(valaddr + (i + 2) * 4);

                                        var sval2 = source.ReadInt(valaddr + (i + 3) * 4);
                                        var sval22 = source.ReadInt(valaddr + (i + 4) * 4);
                                        var sval23 = source.ReadInt(valaddr + (i + 5) * 4);

                                        var x1 = (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (int)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        var x3 = (int)(pval1 / tval1 * (sval23 - sval13) + sval12);
                                        result.AddPoint(x1, x2, x3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadInt(valaddr + i * 4);
                                        val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                        val3 = source.ReadInt(valaddr + (i + 2) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadInt(valaddr + (i + 3) * 4);
                                        val2 = source.ReadInt(valaddr + (i + 4) * 4);
                                        val3 = source.ReadInt(valaddr + (i + 5) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadInt(valaddr + i * 4);
                                        val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                        val3 = source.ReadInt(valaddr + (i + 2) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadInt(valaddr + (i + 3) * 4);
                                        val2 = source.ReadInt(valaddr + (i + 4) * 4);
                                        val3 = source.ReadInt(valaddr + (i + 5) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadInt(valaddr + (i + 3) * 4);
                            var val2 = source.ReadInt(valaddr + (i + 4) * 4);
                            var val3 = source.ReadInt(valaddr + (i + 5) * 4);
                            result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }

                if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new IntPoint3Data(source.ReadInt(valaddr + flast * 4), source.ReadInt(valaddr + (flast + 1) * 4), source.ReadInt(valaddr + (flast + 2) * 4));
                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * (timelen + 12) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    var valtmp = new UIntPoint3Data(source.ReadUInt(valaddr + findex * 4), source.ReadUInt(valaddr + (findex + 1) * 4), source.ReadUInt(valaddr + (findex + 2) * 4));

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<UIntPoint3Data>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }
                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if (val.HasValue)
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadUInt(valaddr + i * 4);
                            var val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                            var val3 = source.ReadUInt(valaddr + (i + 2) * 4);
                            result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadUInt(valaddr + i * 4);
                                    var val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                    var val3 = source.ReadUInt(valaddr + (i + 2) * 4);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:

                                    val = source.ReadUInt(valaddr + (i + 3) * 4);
                                    val2 = source.ReadUInt(valaddr + (i + 4) * 4);
                                    val3 = source.ReadUInt(valaddr + (i + 5) * 4);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadUInt(valaddr + i * 4);
                                        var sval12 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        var sval13 = source.ReadUInt(valaddr + (i + 2) * 4);

                                        var sval2 = source.ReadUInt(valaddr + (i + 3) * 4);
                                        var sval22 = source.ReadUInt(valaddr + (i + 4) * 4);
                                        var sval23 = source.ReadUInt(valaddr + (i + 5) * 4);

                                        var x1 = (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (uint)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        var x3 = (uint)(pval1 / tval1 * (sval23 - sval13) + sval12);
                                        result.AddPoint(x1, x2, x3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadUInt(valaddr + i * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        val3 = source.ReadUInt(valaddr + (i + 2) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadUInt(valaddr + (i + 3) * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 4) * 4);
                                        val3 = source.ReadUInt(valaddr + (i + 5) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadUInt(valaddr + i * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        val3 = source.ReadUInt(valaddr + (i + 2) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadUInt(valaddr + (i + 3) * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 4) * 4);
                                        val3 = source.ReadUInt(valaddr + (i + 5) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadUInt(valaddr + (i + 3) * 4);
                            var val2 = source.ReadUInt(valaddr + (i + 4) * 4);
                            var val3 = source.ReadUInt(valaddr + (i + 5) * 4);
                            result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }

                if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new UIntPoint3Data(source.ReadUInt(valaddr + flast * 4), source.ReadUInt(valaddr + (flast + 1) * 4), source.ReadUInt(valaddr + (flast + 2) * 4));
                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                var qq = source.ReadBytes(qs.Count * (timelen + 8) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    var valtmp = new UIntPointData(source.ReadUInt(valaddr + findex * 4), source.ReadUInt(valaddr + (findex + 1) * 4));

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<UIntPointData>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }

                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if (val.HasValue)
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadUInt(valaddr + i * 4);
                            var val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                            result.AddPoint(val, val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadUInt(valaddr + i * 4);
                                    var val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:
                                    val = source.ReadUInt(valaddr + (i + 2) * 4);
                                    val2 = source.ReadUInt(valaddr + (i + 3) * 4);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadUInt(valaddr + i * 4);
                                        var sval12 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        var sval2 = source.ReadUInt(valaddr + (i + 2) * 4);
                                        var sval22 = source.ReadUInt(valaddr + (i + 3) * 4);
                                        var x1 = (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (uint)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        result.AddPoint(x1, x2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadUInt(valaddr + i * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadUInt(valaddr + (i + 2) * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 3) * 4);
                                        result.AddPoint(val, val2, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadUInt(valaddr + i * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadUInt(valaddr + (i + 2) * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 3) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadUInt(valaddr + (i + 2) * 4);
                            var val2 = source.ReadUInt(valaddr + (i + 3) * 4);
                            result.AddPoint(val, val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }

                if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new UIntPointData(source.ReadUInt(valaddr + flast * 4), source.ReadUInt(valaddr + (flast + 1) * 4));
                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                var qq = source.ReadBytes(qs.Count * (timelen + 16) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    var valtmp = new LongPointData(source.ReadLong(valaddr + findex * 8), source.ReadLong(valaddr + (findex + 1) * 8));

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<LongPointData>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }
                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if (val.HasValue)
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadLong(valaddr + i * 8);
                            var val2 = source.ReadLong(valaddr + (i + 1) * 8);
                            result.AddPoint(val, val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadLong(valaddr + i * 8);
                                    var val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:
                                    val = source.ReadLong(valaddr + (i + 2) * 8);
                                    val2 = source.ReadLong(valaddr + (i + 3) * 8);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadLong(valaddr + i * 8);
                                        var sval12 = source.ReadLong(valaddr + (i + 1) * 8);
                                        var sval2 = source.ReadLong(valaddr + (i + 2) * 8);
                                        var sval22 = source.ReadLong(valaddr + (i + 3) * 8);
                                        var x1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (long)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        result.AddPoint(x1, x2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadLong(valaddr + i * 8);
                                        val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadLong(valaddr + (i + 2) * 8);
                                        val2 = source.ReadLong(valaddr + (i + 3) * 8);
                                        result.AddPoint(val, val2, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadLong(valaddr + i * 8);
                                        val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadLong(valaddr + (i + 2) * 8);
                                        val2 = source.ReadLong(valaddr + (i + 3) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadLong(valaddr + (i + 2) * 8);
                            var val2 = source.ReadLong(valaddr + (i + 3) * 8);
                            result.AddPoint(val, val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }

                if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new LongPointData(source.ReadLong(valaddr + flast * 8), source.ReadLong(valaddr + (flast + 1) * 8));
                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                var qq = source.ReadBytes(qs.Count * (timelen + 16) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    var valtmp = new ULongPointData(source.ReadULong(valaddr + findex * 8), source.ReadULong(valaddr + (findex + 1) * 8));

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<ULongPointData>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }
                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if (val.HasValue)
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadULong(valaddr + i * 8);
                            var val2 = source.ReadULong(valaddr + (i + 1) * 8);
                            result.AddPoint(val, val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadULong(valaddr + i * 8);
                                    var val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:
                                    val = source.ReadULong(valaddr + (i + 2) * 8);
                                    val2 = source.ReadULong(valaddr + (i + 3) * 8);
                                    result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadULong(valaddr + i * 8);
                                        var sval12 = source.ReadULong(valaddr + (i + 1) * 8);
                                        var sval2 = source.ReadULong(valaddr + (i + 2) * 8);
                                        var sval22 = source.ReadULong(valaddr + (i + 3) * 8);
                                        var x1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (long)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        result.AddPoint(x1, x2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadULong(valaddr + i * 8);
                                        val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadULong(valaddr + (i + 2) * 8);
                                        val2 = source.ReadULong(valaddr + (i + 3) * 8);
                                        result.AddPoint(val, val2, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadULong(valaddr + i * 8);
                                        val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadULong(valaddr + (i + 2) * 8);
                                        val2 = source.ReadULong(valaddr + (i + 3) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadULong(valaddr + (i + 2) * 8);
                            var val2 = source.ReadULong(valaddr + (i + 3) * 8);
                            result.AddPoint(val, val2, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }
                if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new ULongPointData(source.ReadULong(valaddr + flast * 8), source.ReadULong(valaddr + (flast + 1) * 8));

                   

                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * (timelen + 24) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    var valtmp = new LongPoint3Data(source.ReadLong(valaddr + findex * 8), source.ReadLong(valaddr + (findex + 1) * 8), source.ReadLong(valaddr + (findex + 2) * 8));

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<LongPoint3Data>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }
                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if (val.HasValue)
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadLong(valaddr + i * 8);
                            var val2 = source.ReadLong(valaddr + (i + 1) * 8);
                            var val3 = source.ReadLong(valaddr + (i + 2) * 8);
                            result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadLong(valaddr + i * 8);
                                    var val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                    var val3 = source.ReadLong(valaddr + (i + 2) * 8);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:

                                    val = source.ReadLong(valaddr + (i + 3) * 8);
                                    val2 = source.ReadLong(valaddr + (i + 4) * 8);
                                    val3 = source.ReadLong(valaddr + (i + 5) * 8);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadLong(valaddr + i * 8);
                                        var sval12 = source.ReadLong(valaddr + (i + 1) * 8);
                                        var sval13 = source.ReadLong(valaddr + (i + 2) * 8);

                                        var sval2 = source.ReadLong(valaddr + (i + 3) * 8);
                                        var sval22 = source.ReadLong(valaddr + (i + 4) * 8);
                                        var sval23 = source.ReadLong(valaddr + (i + 5) * 8);

                                        var x1 = (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (uint)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        var x3 = (uint)(pval1 / tval1 * (sval23 - sval13) + sval12);
                                        result.AddPoint(x1, x2, x3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadLong(valaddr + i * 8);
                                        val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                        val3 = source.ReadLong(valaddr + (i + 2) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadLong(valaddr + (i + 3) * 8);
                                        val2 = source.ReadLong(valaddr + (i + 4) * 8);
                                        val3 = source.ReadLong(valaddr + (i + 5) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadLong(valaddr + i * 8);
                                        val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                        val3 = source.ReadLong(valaddr + (i + 2) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadLong(valaddr + (i + 3) * 8);
                                        val2 = source.ReadLong(valaddr + (i + 4) * 8);
                                        val3 = source.ReadLong(valaddr + (i + 5) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadLong(valaddr + (i + 3) * 8);
                            var val2 = source.ReadLong(valaddr + (i + 4) * 8);
                            var val3 = source.ReadLong(valaddr + (i + 5) * 8);
                            result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }
                if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new LongPoint3Data(source.ReadLong(valaddr + flast * 8), source.ReadLong(valaddr + (flast + 1) * 8), source.ReadLong(valaddr + (flast + 2) * 8));
                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * (timelen + 24) + 17 + sourceAddr, qs.Count);
                if (lowfirst.Count() > 0)
                {
                    var vtmp = ReadOtherDatablockAction(0,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    var valtmp = new ULongPoint3Data(source.ReadULong(valaddr + findex * 8), source.ReadULong(valaddr + (findex + 1) * 8), source.ReadULong(valaddr + (findex + 2) * 8));
                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = (TagHisValue<T>)((object)new TagHisValue<ULongPoint3Data>() { Value = valtmp, Quality = qq[findex], Time = vv[findex].Value.Item1.AddMilliseconds(-vv[findex].Value.Item1.Millisecond) });
                    }

                    foreach (var time1 in lowfirst)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                if (val.HasValue)
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                result.Add(valtmp, time1, qq[findex]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[findex]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(val.Value.Time, qs[findex].Item1, time1, val.Value.Value, (T)((object)valtmp)), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[findex]))
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - val.Value.Time).TotalMilliseconds;
                                    var fval = (qs[findex].Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(valtmp, time1, qq[findex]);
                                    }
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                        }
                    }
                }
                foreach (var time1 in times)
                {
                    for (int i = 0; i < vv.Length - 1; i++)
                    {
                        var skey = vv[i];

                        var snext = vv[i + 1];

                        if (time1 == skey.Value.Item1)
                        {
                            var val = source.ReadULong(valaddr + i * 8);
                            var val2 = source.ReadULong(valaddr + (i + 1) * 8);
                            var val3 = source.ReadULong(valaddr + (i + 2) * 8);
                            result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                        else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                        {

                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    var val = source.ReadULong(valaddr + i * 8);
                                    var val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                    var val3 = source.ReadULong(valaddr + (i + 2) * 8);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.After:

                                    val = source.ReadULong(valaddr + (i + 3) * 8);
                                    val2 = source.ReadULong(valaddr + (i + 4) * 8);
                                    val3 = source.ReadULong(valaddr + (i + 5) * 8);
                                    result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    count++;
                                    break;
                                case QueryValueMatchType.Linear:
                                    if (IsGoodQuality(qq[skey.Key]) && IsGoodQuality(qq[snext.Key]))
                                    {
                                        var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                        var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                        var sval1 = source.ReadULong(valaddr + i * 8);
                                        var sval12 = source.ReadULong(valaddr + (i + 1) * 8);
                                        var sval13 = source.ReadULong(valaddr + (i + 2) * 8);

                                        var sval2 = source.ReadULong(valaddr + (i + 3) * 8);
                                        var sval22 = source.ReadULong(valaddr + (i + 4) * 8);
                                        var sval23 = source.ReadULong(valaddr + (i + 5) * 8);

                                        var x1 = (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                        var x2 = (uint)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                        var x3 = (uint)(pval1 / tval1 * (sval23 - sval13) + sval12);
                                        result.AddPoint(x1, x2, x3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[skey.Key]))
                                    {
                                        val = source.ReadULong(valaddr + i * 8);
                                        val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                        val3 = source.ReadULong(valaddr + (i + 2) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (IsGoodQuality(qq[snext.Key]))
                                    {
                                        val = source.ReadULong(valaddr + (i + 3) * 8);
                                        val2 = source.ReadULong(valaddr + (i + 4) * 8);
                                        val3 = source.ReadULong(valaddr + (i + 5) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[snext.Key]);
                                    }
                                    else
                                    {
                                        result.Add(0, time1, (byte)QualityConst.Null);
                                    }
                                    count++;
                                    break;
                                case QueryValueMatchType.Closed:
                                    var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        val = source.ReadULong(valaddr + i * 8);
                                        val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                        val3 = source.ReadULong(valaddr + (i + 2) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else
                                    {
                                        val = source.ReadULong(valaddr + (i + 3) * 8);
                                        val2 = source.ReadULong(valaddr + (i + 4) * 8);
                                        val3 = source.ReadULong(valaddr + (i + 5) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    count++;
                                    break;
                            }

                            break;
                        }
                        else if (time1 == snext.Value.Item1)
                        {
                            var val = source.ReadULong(valaddr + (i + 3) * 8);
                            var val2 = source.ReadULong(valaddr + (i + 4) * 8);
                            var val3 = source.ReadULong(valaddr + (i + 5) * 8);
                            result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                            count++;
                            break;
                        }
                    }
                }
                if (greatlast.Count() > 0 && !((bool)context["hasnext"]))
                {
                    //如果读取的时间小于，当前数据段的起始时间

                    var vtmp = ReadOtherDatablockAction(1,context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;
                    var valtmp = new ULongPoint3Data(source.ReadULong(valaddr + flast * 8), source.ReadULong(valaddr + (flast + 1) * 8), source.ReadULong(valaddr + (flast + 2) * 8));
                    foreach (var time1 in greatlast)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                result.Add(valtmp, time1, qq[flast]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                if (val.HasValue)
                                {
                                    result.Add(val.Value.Value, time1, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(default(T), time1, (byte)QualityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (val.HasValue)
                                {
                                    if (IsGoodQuality(qq[flast]) && IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(LinerValue(qs[flast].Item1, val.Value.Time, time1, (T)((object)valtmp), val.Value.Value), time1, 0);
                                    }
                                    else if (IsGoodQuality(val.Value.Quality))
                                    {
                                        result.Add(val.Value.Value, time1, val.Value.Quality);
                                    }
                                    else if (IsGoodQuality(qq[flast]))
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                if (val.HasValue)
                                {
                                    var pval = (time1 - qs[flast].Item1).TotalMilliseconds;
                                    var fval = (val.Value.Time - time1).TotalMilliseconds;

                                    if (pval < fval)
                                    {
                                        result.Add(valtmp, time1, qq[flast]);
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
                                count++;
                                break;
                        }
                    }
                }
            }
            return count;
        }

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
        private object LinerValue<T>(DateTime startTime, DateTime endTime, DateTime time, T value1, T value2)
        {
            var pval1 = (time - startTime).TotalMilliseconds;
            var tval1 = (endTime - startTime).TotalMilliseconds;

            if (typeof(T) == typeof(IntPointData))
            {
                var sval1 = (IntPointData)((object)value1);
                var sval2 = (IntPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new IntPointData((int)val1, (int)val2);
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                var sval1 = (UIntPointData)((object)value1);
                var sval2 = (UIntPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new UIntPointData((uint)val1, (uint)val2);
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                var sval1 = (LongPointData)((object)value1);
                var sval2 = (LongPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new LongPointData((long)val1, (long)val2);
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                var sval1 = (ULongPointData)((object)value1);
                var sval2 = (ULongPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new ULongPointData((ulong)val1, (ulong)val2);
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                var sval1 = (IntPoint3Data)((object)value1);
                var sval2 = (IntPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new IntPoint3Data((int)val1, (int)val2, (int)val3);
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                var sval1 = (UIntPoint3Data)((object)value1);
                var sval2 = (UIntPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new UIntPoint3Data((uint)val1, (uint)val2, (uint)val3);
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                var sval1 = (LongPoint3Data)((object)value1);
                var sval2 = (LongPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new LongPoint3Data((long)val1, (long)val2, (long)val3);
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                var sval1 = (ULongPoint3Data)((object)value1);
                var sval2 = (ULongPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new ULongPoint3Data((ulong)val1, (ulong)val2, (ulong)val3);
            }

            return default(T);
        }
    }
}
