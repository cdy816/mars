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

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class NoneCompressUnit : CompressUnitbase
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
        /// <returns></returns>
        public override CompressUnitbase Clone()
        {
            return new NoneCompressUnit();
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
        public override long Compress(MarshalMemoryBlock source, long sourceAddr, MarshalMemoryBlock target, long targetAddr, long size)
        {
            target.WriteDatetime(targetAddr,this.StartTime);
            target.Write((ushort)(size - this.QulityOffset));//写入值的个数
            if (size > 0)
                source.CopyTo(target, sourceAddr, targetAddr + 10, size);
            return size + 10;
        }

        /// <summary>
        /// 读取时间戳
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="timeTick"></param>
        /// <param name="startTime"></param>
        /// <returns></returns>
        private Dictionary<int,Tuple<DateTime,bool>> ReadTimeQulity(MarshalMemoryBlock source, int sourceAddr, int timeTick,out int valueCount, out DateTime startTime)
        {
            source.Position = sourceAddr;

            startTime = source.ReadDateTime();

            //读取值的个数
            int qoffset = source.ReadUShort();
            valueCount = qoffset;

            var times = source.ReadUShorts(source.Position, qoffset);

            Dictionary<int, Tuple<DateTime, bool>> timeQulities = new Dictionary<int, Tuple<DateTime, bool>>(qoffset);

            for(int i=0;i<times.Count;i++)
            {
                if (times[i] == 0&&i>0)
                {
                    timeQulities.Add(i, new Tuple<DateTime, bool>(startTime, false));
                }
                else
                {
                    timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(times[i]*timeTick), true));
                }
            }

            //for(int i=0;i<qoffset;i++)
            //{
            //    var vtmp = source.ReadUShort() * timeTick;
            //    if (i > 0 && vtmp == 0)
            //    {
            //        timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(vtmp), false));
            //    }
            //    else
            //    {
            //        timeQulities.Add(i, new Tuple<DateTime, bool>(startTime.AddMilliseconds(vtmp), true));
            //    }
            //}
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
        {

            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 3 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 3 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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

            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值1个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 3+10, qs.Count);

            //var valaddr = valuecount * 2 + 10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadByte(valaddr + i);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 4 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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

            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值2个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 4+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadShort(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 4 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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

            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值2个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 4+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadUShort(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 6 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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

            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值4个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 6+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadInt(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 6 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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
            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值4个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 6+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadUInt(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 10 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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
            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 10+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadLong(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 10 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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

            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 10+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadULong(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 6 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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
            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值4个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 6+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadFloat(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick,out valuecount ,out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 10+10+ sourceAddr, valuecount);

            var valaddr = valuecount * 2+10 + sourceAddr;

            var vals = source.ReadDoubles(valaddr, valuecount);

            int i = 0;
            int rcount = 0;
            for (i=0;i<valuecount;i++)
            {
                if(qs[i].Item2&&qq[i]<100 && qs[i].Item1>=startTime&&qs[i].Item1<endTime)
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            DateTime time;

            int valuecount = 0;

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(valuecount * 10 + 10 + sourceAddr, valuecount);

            var valaddr = valuecount * 2 + 10 + sourceAddr;

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

            //DateTime time;
            //int valuecount = 0;
            //var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            ////读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            //var qq = source.ReadBytes(valuecount * 10+10, qs.Count);

            //var valaddr = valuecount * 2+10;

            //int i = 0;
            //int rcount = 0;
            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }
            //        var bval = source.ReadDateTime(valaddr + i * 2);
            //        result.Add(bval, vv.Value.Item1, qq[vv.Key]);
            //        rcount++;
            //    }
            //    i++;
            //}
            //return rcount;
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            DateTime time;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);

            var valaddr = qs.Count * 2+10 + sourceAddr;

            int i = 0;
            int rcount = 0;

            List<string> vals = new List<string>();

            source.Position = valaddr;
            for (int ic=0;ic< valuecount; ic++)
            {
                vals.Add(source.ReadString());
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

            //foreach (var vv in qs)
            //{
            //    if (qq[vv.Key] < 100)
            //    {
            //        if (vv.Value.Item1 < startTime || vv.Value.Item1 > endTime || !vv.Value.Item2)
            //        {
            //            continue;
            //        }

            //        result.Add(ls[i],vv.Value.Item1,qq[vv.Key]);

            //        //var bval = source.ReadUShort(valaddr + i * 8);
            //        //result.Add(bval, vv.Key, qq[vv.Value]);
            //        rcount++;
            //    }
            //    i++;
            //}
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
        public override bool? DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);

            var valaddr = qs.Count * 2+10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadByte(valaddr + i)>0;
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadByte(valaddr + i) > 0;
                        case QueryValueMatchType.After:
                            return source.ReadByte(valaddr + i+1) > 0;
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadByte(valaddr + i) > 0;
                            }
                            else
                            {
                                return source.ReadByte(valaddr + i + 1) > 0;
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadByte(valaddr + i+1) > 0;
                }

            }
            return null;
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
        public override int DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(valuecount * 3+10+sourceAddr, valuecount);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2+10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + i) > 0;
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {
                        
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadByte(valaddr + i) > 0;
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                                
                            case QueryValueMatchType.After:
                                val = source.ReadByte(valaddr + i+1) > 0;
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;
                                
                                if (pval < fval)
                                {
                                    val = source.ReadByte(valaddr + i) > 0;
                                    result.Add(val, time1, qq[skey.Key]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadByte(valaddr + i + 1) > 0;
                                    result.Add(val, time1, qq[snext.Key]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + i + 1) > 0;
                        result.Add(val, time1, qq[snext.Key]);
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
        public override byte? DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);

            var valaddr = qs.Count * 2+10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadByte(valaddr + i);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadByte(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadByte(valaddr + i + 1);
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadByte(valaddr + i);
                            }
                            else
                            {
                                return source.ReadByte(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadByte(valaddr + i + 1);
                }

            }
            return null;
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
        public override int DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 3+10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2+10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + i);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadByte(valaddr + i);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = source.ReadByte(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadByte(valaddr + i);
                                    result.Add(val, time1, qq[skey.Key]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadByte(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Key]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadByte(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override DateTime? DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);

            var valaddr = qs.Count * 2+10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadDateTime(valaddr + i*8);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadDateTime(valaddr + i*8);
                        case QueryValueMatchType.After:
                            return source.ReadDateTime(valaddr + (i + 1) * 8);
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadDateTime(valaddr + i*8);
                            }
                            else
                            {
                                return source.ReadDateTime(valaddr + (i + 1) * 8);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadDateTime(valaddr + (i + 1) * 8);
                }

            }
            return null;
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
        public override int DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 10+10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2+10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadDateTime(valaddr + i*8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadDateTime(valaddr + i * 8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = source.ReadDateTime(valaddr + (i + 1)*8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadDateTime(valaddr + i * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadDateTime(valaddr + (i + 1) * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadDateTime(valaddr + (i + 1) * 8);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override double? DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 10+10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2+10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadDouble(valaddr + i*8);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadDouble(valaddr + i * 8);
                        case QueryValueMatchType.After:
                            return source.ReadDouble(valaddr + (i + 1)*8);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadDouble(valaddr + i * 8);
                                var sval2 = source.ReadDouble(valaddr + (i + 1) * 8);
                                return (double)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadDouble(valaddr + i * 8);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadDouble(valaddr + (i + 1) * 8);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadDouble(valaddr + i * 8);
                            }
                            else
                            {
                                return source.ReadDouble(valaddr + (i + 1) * 8);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadDouble(valaddr + (i + 1) * 8);
                }

            }
            return null;

            
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
        public override int DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 10+10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2+10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadDouble(valaddr + i*8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadDouble(valaddr + i*8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadDouble(valaddr + (i + 1)*8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadDouble(valaddr + i*8);
                                    var sval2 = source.ReadDouble(valaddr + (i + 1)*8);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadDouble(valaddr + i*8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadDouble(valaddr + (i + 1)*8);
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
                                    val = source.ReadDouble(valaddr + i*8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadDouble(valaddr + (i + 1)*8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }
                       
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadDouble(valaddr + (i + 1)*8);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }

                }
            }
            return count;
        }


        public override float? DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 6 + 10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadFloat(valaddr + i*4);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadFloat(valaddr + i * 4);
                        case QueryValueMatchType.After:
                            return source.ReadFloat(valaddr + (i + 1)*4);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadFloat(valaddr + i * 4);
                                var sval2 = source.ReadFloat(valaddr + (i + 1) * 4);
                                return (float)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadFloat(valaddr + i * 4);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadFloat(valaddr + (i + 1) * 4);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadFloat(valaddr + i * 4);
                            }
                            else
                            {
                                return source.ReadFloat(valaddr + (i + 1) * 4);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadFloat(valaddr + (i + 1) * 4);
                }

            }
            return null;
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
        public override int DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<float> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 6 + 10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadFloat(valaddr + i*4);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadFloat(valaddr + i * 4);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadFloat(valaddr + (i + 1) * 4);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadFloat(valaddr + i * 4);
                                    var sval2 = source.ReadFloat(valaddr + (i + 1) * 4);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((float)val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadFloat(valaddr + i * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadFloat(valaddr + (i + 1) * 4);
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
                                    val = source.ReadFloat(valaddr + i * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadFloat(valaddr + (i + 1) * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadFloat(valaddr + (i + 1) * 4);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override int? DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 6 + 10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadInt(valaddr + i * 4);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadInt(valaddr + i * 4);
                        case QueryValueMatchType.After:
                            return source.ReadInt(valaddr + (i + 1) * 4);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadInt(valaddr + i * 4);
                                var sval2 = source.ReadInt(valaddr + (i + 1) * 4);
                                return (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadInt(valaddr + i * 4);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadInt(valaddr + (i + 1) * 4);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadInt(valaddr + i*4);
                            }
                            else
                            {
                                return source.ReadInt(valaddr + (i + 1) * 4);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadInt(valaddr + (i + 1) * 4);
                }

            }
            return null;
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
        public override int DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<int> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 6 + 10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadInt(valaddr + i*4);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadInt(valaddr + i * 4);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadInt(valaddr + (i + 1) * 4);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadInt(valaddr + i * 4);
                                    var sval2 = source.ReadInt(valaddr + (i + 1) * 4);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((int)val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadInt(valaddr + i * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadInt(valaddr + (i + 1) * 4);
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
                                    val = source.ReadInt(valaddr + i * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadInt(valaddr + (i + 1) * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadInt(valaddr + (i + 1) * 4);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override long? DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadLong(valaddr + i*8);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadLong(valaddr + i * 8);
                        case QueryValueMatchType.After:
                            return source.ReadLong(valaddr + (i + 1)*8);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadLong(valaddr + i * 8);
                                var sval2 = source.ReadLong(valaddr + (i + 1) * 8);
                                return (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadLong(valaddr + i * 8);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadLong(valaddr + (i + 1) * 8);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadLong(valaddr + i * 8);
                            }
                            else
                            {
                                return source.ReadLong(valaddr + (i + 1) * 8);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadLong(valaddr + (i + 1) * 8);
                }

            }
            return null;
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
        public override int DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<long> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadLong(valaddr + i*8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadLong(valaddr + i * 8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadLong(valaddr + (i + 1) * 8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadLong(valaddr + i * 8);
                                    var sval2 = source.ReadLong(valaddr + (i + 1) * 8);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((long)val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadLong(valaddr + i * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadLong(valaddr + (i + 1) * 8);
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
                                    val = source.ReadLong(valaddr + i * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadLong(valaddr + (i + 1) * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadLong(valaddr + (i + 1) * 8);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override short? DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 4 + 10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadShort(valaddr + i*2);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadShort(valaddr + i * 2);
                        case QueryValueMatchType.After:
                            return source.ReadShort(valaddr + (i + 1)*2);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadShort(valaddr + i * 2);
                                var sval2 = source.ReadShort(valaddr + (i + 1) * 2);
                                return (short)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadShort(valaddr + i * 2);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadShort(valaddr + (i + 1) * 2);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadShort(valaddr + i * 2);
                            }
                            else
                            {
                                return source.ReadShort(valaddr + (i + 1) * 2);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadShort(valaddr + (i + 1) * 2);
                }

            }
            return null;
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
        public override int DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
        {

            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 4 + 10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadShort(valaddr + i*2);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadShort(valaddr + i * 2);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadShort(valaddr + (i + 1) * 2);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadShort(valaddr + i * 2);
                                    var sval2 = source.ReadShort(valaddr + (i + 1) * 2);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((short)val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadShort(valaddr + i * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadShort(valaddr + (i + 1) * 2);
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
                                    val = source.ReadShort(valaddr + i * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadShort(valaddr + (i + 1) * 2);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadShort(valaddr + (i + 1) * 2);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override string DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);

            var valaddr = qs.Count * 2 + sourceAddr;
            Dictionary<int, string> dtmp = new Dictionary<int, string>();
            source.Position = valaddr;

            for (int i = 0; i < qs.Count; i++)
            {
                dtmp.Add(i, source.ReadString());
            }

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return dtmp[i];
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return dtmp[i];
                        case QueryValueMatchType.After:
                            return dtmp[i + 1];
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return dtmp[i];
                            }
                            else
                            {
                                return dtmp[i + 1];
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return dtmp[i + 1];
                }

            }
            return string.Empty;
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
        public override int DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
        {

            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);

            //var dtmp = source.ToStringList(sourceAddr + 12, Encoding.Unicode);

            var valaddr = qs.Count * 2 + sourceAddr;
            Dictionary<int, string> dtmp = new Dictionary<int, string>();
            source.Position = valaddr;

            for (int i = 0; i < qs.Count; i++)
            {
                dtmp.Add(i, source.ReadString());
            }

            var qq = source.ReadBytes(qs.Count);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = dtmp[i];
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = dtmp[i];
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = dtmp[i + 1];
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = dtmp[i];
                                    result.Add(val, time1, qq[skey.Key]);
                                    break;
                                }
                                else
                                {
                                    val = dtmp[i + 1];
                                    result.Add(val, time1, qq[snext.Key]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = dtmp[i];
                        result.Add(val, time1, qq[snext.Key]);
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
        public override uint? DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 6 + 10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadUInt(valaddr + i*4);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadUInt(valaddr + i * 4);
                        case QueryValueMatchType.After:
                            return source.ReadUInt(valaddr + (i + 1)*4);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadUInt(valaddr + i * 4);
                                var sval2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                return (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadUInt(valaddr + i * 4);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadUInt(valaddr + (i + 1) * 4);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadUInt(valaddr + i * 4);
                            }
                            else
                            {
                                return source.ReadUInt(valaddr + (i + 1) * 4);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadUInt(valaddr + (i + 1) * 4);
                }

            }
            return null;
            
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
        public override int DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 6 + 10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadUInt(valaddr + i*4);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadUInt(valaddr + i * 4);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadUInt(valaddr + (i + 1) * 4);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadUInt(valaddr + i * 4);
                                    var sval2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((uint)val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadUInt(valaddr + i * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadUInt(valaddr + (i + 1) * 4);
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
                                    val = source.ReadUInt(valaddr + i * 4);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadUInt(valaddr + (i + 1) * 4);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadUInt(valaddr + (i + 1) * 4);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override ulong? DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadULong(valaddr + i*8);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadULong(valaddr + i*8);
                        case QueryValueMatchType.After:
                            return source.ReadULong(valaddr + (i + 1)*8);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadULong(valaddr + i * 8);
                                var sval2 = source.ReadULong(valaddr + (i + 1) * 8);
                                return (ulong)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadULong(valaddr + i * 8);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadULong(valaddr + (i + 1) * 8);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadULong(valaddr + i * 8);
                            }
                            else
                            {
                                return source.ReadULong(valaddr + (i + 1) * 8);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadULong(valaddr + (i + 1) * 8);
                }

            }
            return null;

           
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
        public override int DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadULong(valaddr + i * 8);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadULong(valaddr + i * 8);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadULong(valaddr + (i + 1) * 8);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadULong(valaddr + i * 8);
                                    var sval2 = source.ReadULong(valaddr + (i + 1) * 8);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((ulong)val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadULong(valaddr + i * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadULong(valaddr + (i + 1) * 8);
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
                                    val = source.ReadULong(valaddr + i * 8);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadULong(valaddr + (i + 1) * 8);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadULong(valaddr + (i + 1) * 8);
                        result.Add(val, time1, qq[snext.Key]);
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
        public override ushort? DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var qq = source.ReadBytes(qs.Count * 4 + 10 + sourceAddr, qs.Count);

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Value.Item1)
                {
                    return source.ReadUShort(valaddr + i);
                }
                else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadUShort(valaddr + i*2);
                        case QueryValueMatchType.After:
                            return source.ReadUShort(valaddr + (i + 1)*2);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                            {
                                var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                var sval1 = source.ReadUShort(valaddr + i * 2);
                                var sval2 = source.ReadUShort(valaddr + (i + 1) * 2);
                                return (ushort)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Key] < 20)
                            {
                                return source.ReadUShort(valaddr + i * 2);
                            }
                            else if (qq[snext.Key] < 20)
                            {
                                return source.ReadUShort(valaddr + (i + 1) * 2);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Value.Item1).TotalMilliseconds;
                            var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadUShort(valaddr + i * 2);
                            }
                            else
                            {
                                return source.ReadUShort(valaddr + (i + 1) * 2);
                            }
                    }
                }
                else if (time == snext.Value.Item1)
                {
                    return source.ReadUShort(valaddr + (i + 1) * 2);
                }

            }
            return null;

            
        }

        public override int DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
            var qq = source.ReadBytes(qs.Count * 4 + 10 + sourceAddr, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Value.Item1)
                    {
                        var val = source.ReadUShort(valaddr + i * 2);
                        result.Add(val, time1, qq[skey.Key]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Value.Item1 && time1 < snext.Value.Item1)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadUShort(valaddr + i * 2);
                                result.Add(val, time1, qq[skey.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadUShort(valaddr + (i + 1) * 2);
                                result.Add(val, time1, qq[snext.Key]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadUShort(valaddr + i * 2);
                                    var sval2 = source.ReadUShort(valaddr + (i + 1) * 2);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add((ushort)val1, time1, 0);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    val = source.ReadUShort(valaddr + i * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    val = source.ReadUShort(valaddr + (i + 1) * 2);
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
                                    val = source.ReadUShort(valaddr + i * 2);
                                    result.Add(val, time1, qq[skey.Key]);
                                }
                                else
                                {
                                    val = source.ReadUShort(valaddr + (i + 1) * 2);
                                    result.Add(val, time1, qq[snext.Key]);
                                }
                                count++;
                                break;
                        }

                        break;
                    }
                    else if (time1 == snext.Value.Item1)
                    {
                        var val = source.ReadUShort(valaddr + (i + 1) * 2);
                        result.Add(val, time1, qq[snext.Key]);
                        count++;
                        break;
                    }

                }
            }
            return count;
        }
    }
}
