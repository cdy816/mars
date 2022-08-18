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
    public class NoneCompressUnit3 : CompressUnitbase2
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
        public override NoneCompressUnit3 Clone()
        {
            return new NoneCompressUnit3();
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
        /// 获取时间
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="timeTick"></param>
        /// <param name="startTime"></param>
        /// <returns></returns>
        protected Dictionary<int, DateTime> GetTimers(MarshalMemoryBlock source, int sourceAddr, out int valueCount,out byte timelen)
        {
            source.Position = sourceAddr;


            var startTime = source.ReadDateTime();

            //读取值的个数
            int qoffset = source.ReadInt();
            valueCount = qoffset;

            //读取时间单位
            int timeTick = source.ReadInt();

            timelen = source.ReadByte();
            Dictionary<int, DateTime> re;
            if (timelen == 2)
            {
                var times = source.ReadUShorts(source.Position, qoffset);

                re = new Dictionary<int, DateTime>();

                for (int i = 0; i < times.Count; i++)
                {
                    if (times[i] == 0)
                    {
                        if (i == 0 || i == 1)
                        {
                            if (times[0] == 0 && times[1] == 0)
                            {
                                if (i == 1)
                                {
                                    re.Add(i, startTime);
                                }
                            }
                            else
                            {
                                if (i == 0)
                                {
                                    re.Add(i, startTime);
                                }
                                else
                                {

                                }
                            }
                        }
                    }
                    else
                    {
                        if (timeTick == 100)
                        {
                            re.Add(i, startTime.AddMilliseconds((short)times[i] * timeTick));
                        }
                        else
                        {
                            re.Add(i, startTime.AddMilliseconds(times[i] * timeTick));
                        }
                    }
                }
            }
            else
            {
                var times = source.ReadInts(source.Position, qoffset);

                re = new Dictionary<int, DateTime>();

                for (int i = 0; i < times.Count; i++)
                {
                    if (times[i] == 0)
                    {
                        if(i==0||i==1)
                        {
                            if(times[0]==0 && times[1]==0)
                            {
                                if(i==1)
                                {
                                    re.Add(i, startTime);
                                }
                            }
                            else
                            {
                                if(i==0)
                                {
                                    re.Add(i, startTime);
                                }
                                else
                                {

                                }
                            }
                        }
                    }
                    else
                    {
                        if (timeTick == 100)
                        {
                            re.Add(i, startTime.AddMilliseconds((short)times[i] * timeTick));
                        }
                        else
                        {
                            re.Add(i, startTime.AddMilliseconds(times[i] * timeTick));
                        }
                        //re.Add(i, startTime.AddMilliseconds(times[i] * timeTick));
                        // timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(times[i] * timeTick), true));
                    }
                }
            }
            return re;
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
                        if(timeTick==100)
                        {
                            timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds((short)times[i] * timeTick), true));
                        }
                        else
                        {
                            timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(times[i] * timeTick), true));
                        }
                       
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
                        if (timeTick == 100)
                        {
                            timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds((short)times[i] * timeTick), true));
                        }
                        else
                        {
                            timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(times[i] * timeTick), true));
                        }
                    }
                }
            }
            return timeQulities;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="qa"></param>
        /// <returns></returns>
        protected bool IsBadQuality(byte qa)
        {
            return (qa >= (byte)QualityConst.Bad && qa <= (byte)QualityConst.Bad + 20)||qa == (byte)QualityConst.Close;
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
        /// <param name="times"></param>
        /// <returns></returns>
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="times"></param>
        /// <returns></returns>
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
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="valueaddress"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        protected virtual List<T> DeCompressValue<T>(MarshalMemoryBlock source,long valueaddress, int count)
        {
            string tname = typeof(T).Name;
            switch (tname)
            {
                case "bool":
                case "Boolean":
                    List<bool> bre = new List<bool>(count);
                    var rtmp = source.ReadBytes(valueaddress,count);
                    bre.AddRange(rtmp.Select(e => e > 0));
                    return bre as List<T>;
                case "Byte":
                    List<byte> bbre = new List<byte>(count);
                    rtmp = source.ReadBytes(valueaddress, count);
                    bbre.AddRange(rtmp);
                    return bbre as List<T>;
                case "Int16":
                    return source.ReadShorts(valueaddress,count) as List<T>;
                case "UInt16":
                    return source.ReadUShorts(valueaddress, count) as List<T>;
                case "Int32":
                    return source.ReadInts(valueaddress, count) as List<T>;
                case "UInt32":
                    return source.ReadUInts(valueaddress, count) as List<T>;
                case "Int64":
                    return source.ReadLongs(valueaddress, count) as List<T>;
                case "UInt64":
                    return source.ReadULongs(valueaddress, count) as List<T>;
                case "Double":
                    return source.ReadDoubles(valueaddress, count) as List<T>;
                case "Single":
                    return source.ReadFloats(valueaddress, count) as List<T>;
                case "String":
                    List<string> vals = new List<string>();

                    source.Position = valueaddress;
                    for (int ic = 0; ic < count; ic++)
                    {
                        vals.Add(source.ReadStringbyFixSize());
                    }
                    return vals as List<T>;
                case "DateTime":
                    return source.ReadDateTimes(valueaddress, count) as List<T>;
                case "IntPointData":
                    List<IntPointData> lpre = new List<IntPointData>();
                    var ivals = source.ReadInts(valueaddress, count * 2);

                    for (int i = 0; i < ivals.Count; i = i + 2)
                    {
                        lpre.Add(new IntPointData(ivals[i], ivals[i + 1]));
                    }
                    return lpre as List<T>;
                case "UIntPointData":
                    List<UIntPointData> ulpre = new List<UIntPointData>();
                    var uvals = source.ReadUInts(valueaddress, count * 2);

                    for (int i = 0; i < uvals.Count; i = i + 2)
                    {
                        ulpre.Add(new UIntPointData(uvals[i], uvals[i + 1]));
                    }
                    return ulpre as List<T>;
                case "LongPointData":
                    List<LongPointData> lppre = new List<LongPointData>();
                    var lvals = source.ReadLongs(valueaddress, count * 2);
                    for (int i = 0; i < lvals.Count; i = i + 2)
                    {
                        lppre.Add(new LongPointData(lvals[i], lvals[i + 1]));
                    }
                    return lppre as List<T>;
                case "ULongPointData":
                    List<ULongPointData> ulppre = new List<ULongPointData>();
                    var ulvals = source.ReadULongs(valueaddress, count * 2);
                    for (int i = 0; i < ulvals.Count; i = i + 2)
                    {
                        ulppre.Add(new ULongPointData(ulvals[i], ulvals[i + 1]));
                    }
                    return ulppre as List<T>;
                case "IntPoint3Data":
                    List<IntPoint3Data> ip3re = new List<IntPoint3Data>();
                    ivals = source.ReadInts(valueaddress, count * 3);

                    for (int i = 0; i < ivals.Count; i = i + 3)
                    {
                        ip3re.Add(new IntPoint3Data(ivals[i], ivals[i + 1], ivals[i + 2]));
                    }
                    return ip3re as List<T>;
                case "UIntPoint3Data":
                    List<UIntPoint3Data> uip3re = new List<UIntPoint3Data>();
                    uvals = source.ReadUInts(valueaddress, count * 3);

                    for (int i = 0; i < uvals.Count; i = i + 3)
                    {
                        uip3re.Add(new UIntPoint3Data(uvals[i], uvals[i + 1], uvals[i + 2]));
                    }
                    return uip3re as List<T>;
                case "LongPoint3Data":
                    List<LongPoint3Data> lpp3re = new List<LongPoint3Data>();
                    lvals = source.ReadLongs(valueaddress, count * 3);
                    for (int i = 0; i < lvals.Count; i = i + 3)
                    {
                        lpp3re.Add(new LongPoint3Data(lvals[i], lvals[i + 1], lvals[i + 2]));
                    }
                    return lpp3re as List<T>;
                case "ULongPoint3Data":
                    List<ULongPoint3Data> ulpp3re = new List<ULongPoint3Data>();
                    ulvals = source.ReadULongs(valueaddress, count * 3);
                    for (int i = 0; i < ulvals.Count; i = i + 3)
                    {
                        ulpp3re.Add(new ULongPoint3Data(ulvals[i], ulvals[i + 1], ulvals[i + 2]));
                    }
                    return ulpp3re as List<T>;
            }
            return null;

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
            var timers = GetTimers(source, sourceAddr,  out count,out byte timelen);
            long quaaddress = 0;
            byte[] qualitys=null;
            if (timers.Count > 0)
            {
                var valaddr = count * timelen + 17 + sourceAddr;
                var value = DeCompressValue<T>(source,valaddr,count);

                if (typeof(T) == typeof(string))
                {
                    source.Position = valaddr;

                    for (int i = 0; i < count; i++)
                    {
                        source.ReadString();
                    }

                    qualitys = source.ReadBytes(count);
                }
                else
                {
                    quaaddress = GetQualityAddress<T>(count, timelen, sourceAddr);
                    //读取
                    qualitys = source.ReadBytes(quaaddress, count);
                }

                

                int resultCount = 0;
                for (int i = 0; i < count; i++)
                {
                    //if ((qualitys.Length > i && qualitys[i] < 100 && timers.ContainsKey(i) && qualitys[i] != (byte)QualityConst.Close))
                        if ((qualitys.Length > i && (qualitys[i] < 100 || qualitys[i] == (100 + (byte)QualityConst.Init)) && timers.ContainsKey(i)))
                    {
                        result.Add<T>(value[i], timers[i], qualitys[i]);
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
            var timers = GetTimers(source, sourceAddr, out count,out byte timelen);
            long quaaddress = GetQualityAddress<T>(count, timelen, sourceAddr);
            byte[] qualitys;
            if (timers.Count > 0)
            {
                var valaddr = count * timelen + 17 + sourceAddr;

                if (typeof(T) == typeof(string))
                {
                    source.Position = valaddr;

                    for (int i = 0; i < count; i++)
                    {
                        source.ReadString();
                    }

                    qualitys = source.ReadBytes(count);
                }
                else
                {
                    //读取
                    qualitys = source.ReadBytes(quaaddress, count);
                }

                var value = DeCompressValue<T>(source, valaddr, count);
                try
                {
                    if (tp == 0)
                    {
                        //读取最后一个
                        //查找最后一个非辅助记录点
                        for (int i = count - 1; i >= 0; i--)
                        {
                       
                            if (qualitys[i] == (byte)QualityConst.Close)
                            {
                                return TagHisValue<T>.MinValue;
                            }
                            else if (timers.ContainsKey(i) && qualitys[i] < 100)
                            {
                                return new TagHisValue<T>() { Time = timers[i], Quality = qualitys[i], Value = value[i] };
                            }

                        }
                        //查找最后一个点
                        for (int i = count - 1; i >= 0; i--)
                        {
                           
                            if (timers.ContainsKey(i))
                            {
                                return new TagHisValue<T>() { Time = timers[i], Quality = qualitys[i] >= 100 ? (byte)(qualitys[i] - 100) : qualitys[i], Value = value[i] };
                            }
                        }
                    }
                    else
                    {
                        //读取第一个非辅助记录点
                        for (int i = 0; i < count; i++)
                        {
                           
                            if (qualitys[i] == (byte)QualityConst.Close)
                            {
                                return TagHisValue<T>.MinValue;
                            }
                            else if (timers.ContainsKey(i) && qualitys[i] < 100)
                            {
                                return new TagHisValue<T>() { Time = timers[i], Quality = qualitys[i], Value = value[i] };
                            }

                        }
                        //读取第一个点的值
                        for (int i = 0; i < count; i++)
                        {
                          
                            if (timers.ContainsKey(i))
                            {
                                return new TagHisValue<T>() { Time = timers[i], Quality = qualitys[i] >= 100 ? (byte)(qualitys[i] - 100) : qualitys[i], Value = value[i] };
                            }
                        }

                    }
                }
                catch
                {

                }
                finally
                {
                    FillFirstLastValue(value, timers, qualitys, context, count);
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

            if (CheckTypeIsPointData(typeof(T)))
            {
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type, ReadOtherDatablockAction, context);
            }

            var hasnext = (bool)context["hasnext"];
            int count = 0;
            var timers = GetTimers(source, sourceAddr + 8, out count,out byte timelen);
            long quaaddress = GetQualityAddress<T>(count, timelen, sourceAddr);
            byte[] qualitys = null;

            var valaddr = count * timelen + 17 + sourceAddr;

            if (typeof(T) == typeof(string))
            {
                source.Position = valaddr;
                for (int i = 0; i < count; i++)
                {
                    source.ReadString();
                }
                qualitys = source.ReadBytes(count);
            }
            else
            {
                //读取
                qualitys = source.ReadBytes(quaaddress, count);
            }

            var value = DeCompressValue<T>(source, valaddr, count);

            ////读取
            //var qualitys = source.ReadBytes(quaaddress, count);

           

            DateTime endtime = DateTime.MaxValue;
            if (qualitys.Length > 0)
            {
                var vskey = timers.Last().Key;
                //说明此段数据系统退出
                if (qualitys[vskey] == (byte)QualityConst.Close)
                {
                    endtime = timers.Last().Value;
                }
            }
            try
            {
                //如果超出停止时间
                if (time >= endtime) return null;

                var vskey = timers.First().Key;
                var nskey = timers.Last().Key;

                if (timers.Count > 0 && time < timers[vskey])
                {
                    //如果读取的时间小于，当前数据段的起始时间
                    var vtmp = ReadOtherDatablockAction(0, context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                    if (val.HasValue && val.Value.IsEmpty())
                    {
                        val = new TagHisValue<T>() { Value = value[vskey], Quality = qualitys[vskey], Time = time };
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
                                var ffval = (timers[vskey] - time).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    return val.Value.Value;
                                }
                                else
                                {
                                    return value[vskey];
                                }
                            }
                            else
                            {
                                if (!IsBadQuality(qualitys[vskey]) && !IsBadQuality(val.Value.Quality))
                                {
                                    var pval1 = (time - val.Value.Time).TotalMilliseconds;
                                    var tval1 = (timers[vskey] - val.Value.Time).TotalMilliseconds;
                                    var sval1 = val.Value.Value;
                                    var sval2 = value[vskey];

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

                                    //if (typeof(T) == typeof(double))
                                    //{
                                    //    return val1;
                                    //}
                                    //else if (typeof(T) == typeof(float))
                                    //{
                                    //    return (float)val1;
                                    //}
                                    //else if (typeof(T) == typeof(short))
                                    //{
                                    //    return (short)val1;
                                    //}
                                    //else if (typeof(T) == typeof(ushort))
                                    //{
                                    //    return (ushort)val1;
                                    //}
                                    //else if (typeof(T) == typeof(int))
                                    //{
                                    //    return (int)val1;
                                    //}
                                    //else if (typeof(T) == typeof(uint))
                                    //{
                                    //    return (uint)val1;
                                    //}
                                    //else if (typeof(T) == typeof(long))
                                    //{
                                    //    return (long)val1;
                                    //}
                                    //else if (typeof(T) == typeof(ulong))
                                    //{
                                    //    return (ulong)val1;
                                    //}
                                    //else if (typeof(T) == typeof(byte))
                                    //{
                                    //    return (byte)val1;
                                    //}
                                }
                                else if (!IsBadQuality(val.Value.Quality))
                                {
                                    return val.Value.Value;
                                }
                                else if (!IsBadQuality(qualitys[vskey]))
                                {
                                    return value[vskey];
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
                            var fval = (timers[vskey] - time).TotalMilliseconds;

                            if (pval < fval)
                            {
                                return val.Value.Value;
                            }
                            else
                            {
                                return value[vskey];
                            }
                    }
                    return null;
                }
                else if (timers.Count > 0 && time > timers[nskey])
                {
                    if (hasnext) return null;

                    var vtmp = ReadOtherDatablockAction(1, context);
                    TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                    //var val = (TagHisValue<T>)ReadOtherDatablockAction(1);
                    var valtmp = value[nskey];
                    var timetmp = timers[nskey];
                    var qtmp = qualitys[nskey];

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

                                    //if (typeof(T) == typeof(double))
                                    //{
                                    //    return val1;
                                    //}
                                    //else if (typeof(T) == typeof(float))
                                    //{
                                    //    return (float)val1;
                                    //}
                                    //else if (typeof(T) == typeof(short))
                                    //{
                                    //    return (short)val1;
                                    //}
                                    //else if (typeof(T) == typeof(ushort))
                                    //{
                                    //    return (ushort)val1;
                                    //}
                                    //else if (typeof(T) == typeof(int))
                                    //{
                                    //    return (int)val1;
                                    //}
                                    //else if (typeof(T) == typeof(uint))
                                    //{
                                    //    return (uint)val1;
                                    //}
                                    //else if (typeof(T) == typeof(long))
                                    //{
                                    //    return (long)val1;
                                    //}
                                    //else if (typeof(T) == typeof(ulong))
                                    //{
                                    //    return (ulong)val1;
                                    //}
                                    //else if (typeof(T) == typeof(byte))
                                    //{
                                    //    return (byte)val1;
                                    //}
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
                    var vtvs = timers.ToList();
                    for (int i = j; i < timers.Count - 1; i++)
                    {

                        var sk = vtvs[i].Key;
                        var sn = vtvs[i + 1].Key;

                        var skey = vtvs[i].Value;

                        var snext = vtvs[i + 1].Value;

                        //var skey = timers[i];

                        //var snext = timers[i + 1];

                        if ((time == skey) || (time < skey && (skey - time).TotalSeconds < 1))
                        {
                            return value[sk];
                        }
                        else if (time > skey && time < snext)
                        {
                            switch (type)
                            {
                                case QueryValueMatchType.Previous:
                                    return value[sk];
                                case QueryValueMatchType.After:
                                    return value[sn];
                                case QueryValueMatchType.Linear:
                                    if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                    {
                                        var ppval = (time - skey).TotalMilliseconds;
                                        var ffval = (snext - time).TotalMilliseconds;

                                        if (ppval < ffval)
                                        {
                                            return value[sk];
                                        }
                                        else
                                        {
                                            return value[sn];
                                        }
                                    }
                                    else
                                    {
                                        if (!IsBadQuality(qualitys[i]) && !IsBadQuality(qualitys[i + 1]))
                                        {
                                            var pval1 = (time - skey).TotalMilliseconds;
                                            var tval1 = (snext - skey).TotalMilliseconds;
                                            var sval1 = value[sk];
                                            var sval2 = value[sn];

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
                                        else if (!IsBadQuality(qualitys[sk]))
                                        {
                                            return value[sk];
                                        }
                                        else if (!IsBadQuality(qualitys[sn]))
                                        {
                                            return value[sn];
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
                                        return value[sk];
                                    }
                                    else
                                    {
                                        return value[sn];
                                    }
                            }
                            break;
                        }
                        else if (time == snext)
                        {
                            return value[sn];
                        }

                    }

                    return null;
                }
            }
            finally
            {
                if (qualitys.Length > 0)
                {
                    FillFirstLastValue(value, timers, qualitys, context, count);
                }
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
        /// <param name="count"></param>
        /// <param name="timelen"></param>
        /// <param name="sourceAddr"></param>
        /// <returns></returns>
        private long GetQualityAddress<T>(int count,int timelen,long sourceAddr)
        {
            string tname = typeof(T).Name;
            switch (tname)
            {
                case "bool":
                case "Boolean":
                case "Byte":
                    return count * (timelen + 1) + 17 + sourceAddr;
                case "Int16":
                case "UInt16":
                    return count * (timelen + 2) + 17 + sourceAddr;
                case "Single":
                case "Int32":
                case "UInt32":
                    return count * (timelen + 4) + 17 + sourceAddr;
                case "Int64":
                case "UInt64":
                case "Double":
                case "DateTime":
                    return count * (timelen + 8) + 17 + sourceAddr;
                case "String":
                    return count * (timelen + 256) + 17 + sourceAddr;
                case "IntPointData":
                    return count * (timelen + 8) + 17 + sourceAddr;
                case "UIntPointData":
                    return count * (timelen + 8) + 17 + sourceAddr;
                case "LongPointData":
                    return count * (timelen + 16) + 17 + sourceAddr;
                case "ULongPointData":
                    return count * (timelen + 16) + 17 + sourceAddr;
                case "IntPoint3Data":
                    return count * (timelen + 12) + 17 + sourceAddr;
                case "UIntPoint3Data":
                    return count * (timelen + 12) + 17 + sourceAddr;
                case "LongPoint3Data":
                    return count * (timelen + 24) + 17 + sourceAddr;
                case "ULongPoint3Data":
                    return count * (timelen + 24) + 17 + sourceAddr;
            }
            return 0;
           
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
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type, result, ReadOtherDatablockAction, context);
            }

            var hasnext = (bool)context["hasnext"];
            int count = 0;
            var timers = GetTimers(source, sourceAddr, out count,out byte timelen);

            long quaaddress = GetQualityAddress<T>(count,timelen, sourceAddr);

            var valaddr = count * timelen + 17 + sourceAddr;
            var value = DeCompressValue<T>(source, valaddr, count);

            //读取
            var qualitys = source.ReadBytes(quaaddress, count);

            DateTime endtime = DateTime.MaxValue;
            if (qualitys.Length > 0)
            {
                //说明此段数据系统退出
                if (qualitys[qualitys.Length - 1] == (byte)QualityConst.Close)
                {
                    endtime = timers.Last().Value;
                }
            }

            var vff = timers.FirstOrDefault().Value;
            var lff = timers.LastOrDefault().Value;

            var lowfirst = time.Where(e => e < vff && e < endtime);
            var greatlast = time.Where(e => e < endtime && e > lff);
            var times = time.Where(e => e >= vff && e <= lff && e < endtime);
            int resultCount = 0;


            int j = 0;

            if (lowfirst.Count() > 0)
            {
                //如果读取的时间小于，当前数据段的起始时间
                var vtmp = ReadOtherDatablockAction(0, context);

                TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                //如果为空值，则说明跨数据文件了，则取第一个有效值用作前一个值
                if (val.HasValue && (val.Value.IsEmpty() || val.Value.Quality == (byte)QualityConst.Close))
                {
                    val = null;
                    //val = new TagHisValue<T>() { Value = value[0], Quality = qulityes[0], Time = lowfirst.First() };
                }

                var skey = timers.First().Key;

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
                            result.Add(value[skey], vtime, qualitys[skey]);
                            resultCount++;
                            break;
                        case QueryValueMatchType.Linear:
                            if (val.HasValue)
                            {
                                if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                {
                                    var ppval = (vtime - val.Value.Time).TotalMilliseconds;
                                    var ffval = (timers[skey] - vtime).TotalMilliseconds;

                                    if (ppval < ffval)
                                    {
                                        result.Add(val.Value.Value, vtime, val.Value.Quality);
                                    }
                                    else
                                    {
                                        result.Add(value[skey], vtime, qualitys[skey]);
                                    }
                                    resultCount++;
                                }
                                else
                                {
                                    if (!IsBadQuality(qualitys[skey]) && !IsBadQuality(val.Value.Quality))
                                    {
                                        var pval1 = (vtime - val.Value.Time).TotalMilliseconds;
                                        var tval1 = (timers[skey] - val.Value.Time).TotalMilliseconds;
                                        var sval1 = val.Value.Value;
                                        var sval2 = value[skey];

                                        var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                        if (pval1 <= 0)
                                        {
                                            //说明数据有异常，则取第一个值
                                            result.Add((object)sval1, vtime, val.Value.Quality);
                                        }
                                        else
                                        {
                                            result.Add((object)val1, vtime, pval1 < tval1 ? val.Value.Quality : qualitys[skey]);
                                        }
                                    }
                                    else if (!IsBadQuality(qualitys[skey]))
                                    {
                                        result.Add(value[skey], vtime, qualitys[skey]);
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
                                var fval = (timers[skey] - vtime).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    result.Add(val.Value.Value, vtime, val.Value.Quality);
                                }
                                else
                                {
                                    result.Add(value[skey], vtime, qualitys[skey]);
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

            var vtvs = timers.ToList();

            foreach (var time1 in times)
            {
                for (int i = j; i < vtvs.Count - 1; i++)
                {
                    var sk = vtvs[i].Key;
                    var sn = vtvs[i+1].Key;

                    var skey = vtvs[i].Value;

                    var snext = vtvs[i + 1].Value;
                    j = i;

                    if ((time1 == skey) || (time1 < skey && (skey - time1).TotalSeconds < 1))
                    {
                        
                        var val = value[sk];
                        result.Add(val, time1, qualitys[sk]);
                        resultCount++;

                        break;
                    }
                    else if (time1 > skey && time1 < snext)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = value[sk];
                                result.Add(val, time1, qualitys[sk]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.After:
                                val = value[sn];
                                result.Add(val, time1, qualitys[sn]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                                {
                                    var ppval = (time1 - skey).TotalMilliseconds;
                                    var ffval = (snext - time1).TotalMilliseconds;

                                    if (ppval < ffval)
                                    {
                                        val = value[sk];
                                        result.Add(val, time1, qualitys[sk]);
                                    }
                                    else
                                    {
                                        val = value[sn];
                                        result.Add(val, time1, qualitys[sn]);
                                    }
                                    resultCount++;
                                }
                                else
                                {
                                    if (!IsBadQuality(qualitys[sk]) && !IsBadQuality(qualitys[sn]))
                                    {
                                        var pval1 = (time1 - skey).TotalMilliseconds;
                                        var tval1 = (snext - skey).TotalMilliseconds;
                                        var sval1 = value[sk];
                                        var sval2 = value[sn];

                                        var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                        result.Add((object)val1, time1, pval1 < tval1 ? qualitys[sk] : qualitys[sn]);
                                    }
                                    else if (!IsBadQuality(qualitys[sk]))
                                    {
                                        val = value[sk];
                                        result.Add(val, time1, qualitys[sk]);
                                    }
                                    else if (!IsBadQuality(qualitys[sn]))
                                    {
                                        val = value[sn];
                                        result.Add(val, time1, qualitys[sn]);
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
                                    val = value[sk];
                                    result.Add(val, time1, qualitys[sk]);
                                }
                                else
                                {
                                    val = value[sn];
                                    result.Add(val, time1, qualitys[sn]);
                                }
                                resultCount++;
                                break;
                        }
                        break;
                    }
                    else if (time1 == snext)
                    {
                        var val = value[sn];
                        result.Add(val, time1, qualitys[sn]);
                        resultCount++;
                        break;
                    }

                }
            }

            if (greatlast.Count() > 0 && !hasnext)
            {
                //如果读取的时间小于，当前数据段的起始时间

                var vtmp = ReadOtherDatablockAction(1, context);

                TagHisValue<T>? val = vtmp != null ? (TagHisValue<T>)vtmp : null;

                //如果为空值，则说明跨数据文件了，则取第最后一个有效值用作后一个值
                if (val.HasValue && (val.Value.IsEmpty() || val.Value.Quality == (byte)QualityConst.Close))
                {
                    var vskey = timers.Last().Key;
                    val = new TagHisValue<T>() { Value = value[vskey], Quality = qualitys[vskey], Time = greatlast.Last() };
                }

                var skey = timers.Last().Key;
                DateTime ptime = timers[skey];
                T pval = value[skey];
                byte qua = qualitys[skey];

                
                if (qua >= 100 && value.Count > 1)
                {
                    var dtt = timers.ToList();
                    skey = dtt[dtt.Count - 2].Key;

                    pval = value[skey];
                    qua = qualitys[skey];
                    ptime = timers[skey];
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

                                        result.Add((object)val1, vtime, pval1 < tval1 ? qua : val.Value.Quality);
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


            if (qualitys.Length > 0)
            {
                FillFirstLastValue(value, timers, qualitys, context, count);
            }
            return resultCount;
        }

        private void FillFirstLastValue<T>(List<T> value, Dictionary<int, DateTime> timers, byte[] qulityes, QueryContext context, int count)
        {
            bool islasthase = false;
            for (int i = count - 1; i >= 0; i--)
            {
                if (timers.ContainsKey(i) && qulityes[i] == (byte)QualityConst.Close)
                {
                    context.LastValue = null;
                    context.LastTime = timers[i];
                    context.LastQuality = qulityes[i];
                    islasthase = true;
                    break;
                }
                else if (timers.ContainsKey(i) && qulityes[i] < 100)
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
                        context.LastQuality = qulityes[i] >= 100 ? (byte)(qulityes[i] - 100) : qulityes[i];
                        islasthase = true;
                        break;
                    }
                }
            }

            bool isfirsthas = false;

            //读取第一个非辅助记录点
            for (int i = 0; i < count; i++)
            {
                if (timers.ContainsKey(i) && qulityes[i] == (byte)QualityConst.Close)
                {
                    context.FirstValue = null;
                    context.FirstTime = timers[i];
                    context.FirstQuality = qulityes[i];
                    isfirsthas = true;
                    break;
                }
                else if (timers.ContainsKey(i) && qulityes[i] < 100)
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
                        context.FirstQuality = qulityes[i] >= 100 ? (byte)(qulityes[i] - 100) : qulityes[i];
                        isfirsthas = true;
                    }
                }
            }
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="source"></param>
        ///// <param name="sourceAddr"></param>
        ///// <param name="time"></param>
        ///// <param name="timeTick"></param>
        ///// <param name="type"></param>
        ///// <param name="result"></param>
        ///// <returns></returns>
        //public override int DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte,QueryContext, object> ReadOtherDatablockAction, QueryContext context)
        //{
        //    if (typeof(T) == typeof(bool))
        //    {
        //        return DeCompressBoolValue(source, sourceAddr, time, timeTick,type, result as HisQueryResult<bool>, ReadOtherDatablockAction, context);
        //    }
        //    else if (typeof(T) == typeof(byte))
        //    {
        //        return DeCompressByteValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<byte>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(short))
        //    {
        //        return DeCompressShortValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<short>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(ushort))
        //    {
        //        return DeCompressUShortValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<ushort>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(int))
        //    {
        //        return DeCompressIntValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<int>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(uint))
        //    {
        //        return DeCompressUIntValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<uint>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(long))
        //    {
        //        return DeCompressLongValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<long>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(ulong))
        //    {
        //        return DeCompressULongValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<ulong>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(DateTime))
        //    {
        //        return DeCompressDateTimeValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<DateTime>, ReadOtherDatablockAction, context);

        //    }
        //    else if (typeof(T) == typeof(string))
        //    {
        //        return DeCompressStringValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<string>, ReadOtherDatablockAction, context);
        //    }
        //    else if (typeof(T) == typeof(double))
        //    {
        //        return DeCompressDoubleValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<double>, ReadOtherDatablockAction, context);
        //    }
        //    else if (typeof(T) == typeof(float))
        //    {
        //        return DeCompressFloatValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<float>, ReadOtherDatablockAction, context);
        //    }
        //    else
        //    {
        //        return DeCompressPointValue(source, sourceAddr, time,timeTick,type, result, ReadOtherDatablockAction, context);
        //    }

        //}


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
