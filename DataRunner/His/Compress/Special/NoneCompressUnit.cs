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
           // LoggerService.Service.Erro("NoneCompressUnit", "目标地址:"+targetAddr +" 数值地址: " + (targetAddr+10) +" 变量个数: "+ (size - this.QulityOffset));
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
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
        public  int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
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
        public  bool? DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result)
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
        public  byte? DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result)
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
        public  DateTime? DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result)
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
        public  double? DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
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


        public  float? DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<float> result)
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
        public  int? DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<int> result)
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
        public  long? DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<long> result)
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
        public  short? DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
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
        public  string DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
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
        public  uint? DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result)
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
        public  ulong? DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result)
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
        public  ushort? DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
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
        public  int DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
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
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public override object DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            if (typeof(T) == typeof(bool))
            {
                return ((object)DeCompressBoolValue(source, sourceAddr, time, timeTick, type));
            }
            else if (typeof(T) == typeof(byte))
            {
                return ((object)DeCompressByteValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(short))
            {
                return ((object)DeCompressShortValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(ushort))
            {
                return ((object)DeCompressUShortValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(int))
            {
                return ((object)DeCompressIntValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(uint))
            {
                return ((object)DeCompressUIntValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(long))
            {
                return ((object)DeCompressLongValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(ulong))
            {
                return ((object)DeCompressULongValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(DateTime))
            {
                return ((object)DeCompressDateTimeValue(source, sourceAddr, time, timeTick, type));

            }
            else if (typeof(T) == typeof(string))
            {
                return ((object)DeCompressStringValue(source, sourceAddr, time, timeTick, type));
            }
            else
            {
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type);
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
        public override int DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result)
        {
            if (typeof(T) == typeof(bool))
            {
                return DeCompressBoolValue(source, sourceAddr, time, timeTick,type, result as HisQueryResult<bool>);
            }
            else if (typeof(T) == typeof(byte))
            {
                return DeCompressByteValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<byte>);

            }
            else if (typeof(T) == typeof(short))
            {
                return DeCompressShortValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<short>);

            }
            else if (typeof(T) == typeof(ushort))
            {
                return DeCompressUShortValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<ushort>);

            }
            else if (typeof(T) == typeof(int))
            {
                return DeCompressIntValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<int>);

            }
            else if (typeof(T) == typeof(uint))
            {
                return DeCompressUIntValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<uint>);

            }
            else if (typeof(T) == typeof(long))
            {
                return DeCompressLongValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<long>);

            }
            else if (typeof(T) == typeof(ulong))
            {
                return DeCompressULongValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<ulong>);

            }
            else if (typeof(T) == typeof(DateTime))
            {
                return DeCompressDateTimeValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<DateTime>);

            }
            else if (typeof(T) == typeof(string))
            {
                return DeCompressStringValue(source, sourceAddr, time, timeTick, type, result as HisQueryResult<string>);
            }
            else
            {
                return DeCompressPointValue(source, sourceAddr, time,timeTick,type, result);
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

            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time);
                      

            int i = 0;
            int rcount = 0;

            if (typeof(T) == typeof(IntPointData))
            {
                //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
                var qq = source.ReadBytes(valuecount * 10 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
                var qq = source.ReadBytes(valuecount * 10 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
                var qq = source.ReadBytes(valuecount * 14 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
                var qq = source.ReadBytes(valuecount * 14 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
                var qq = source.ReadBytes(valuecount * 18 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
                var qq = source.ReadBytes(valuecount * 18 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
                var qq = source.ReadBytes(valuecount * 26 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
                var qq = source.ReadBytes(valuecount * 26 + 10 + sourceAddr, valuecount);

                var valaddr = valuecount * 2 + 10 + sourceAddr;

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
        public  T DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            int valuecount = 0;
            object reval = null;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out time1);
            var vv = qs.ToArray();

            if (typeof(T) == typeof(IntPointData))
            {

                var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 2)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new IntPointData(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new IntPointData(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4));
                                break;
                            case QueryValueMatchType.After:
                                reval = new IntPointData(source.ReadInt(valaddr + (i + 2) * 4), source.ReadInt(valaddr + (i + 3) * 4));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadInt(valaddr + i * 4);
                                    var sval12 = source.ReadInt(valaddr + (i + 1) * 4);
                                    var sval2 = source.ReadInt(valaddr + (i + 2) * 4);
                                    var sval22 = source.ReadInt(valaddr + (i + 3) * 4);
                                    var x1 = (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    var x2 = (int)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                    reval = new IntPointData(x1, x2);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new IntPointData(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new IntPointData(source.ReadInt(valaddr + (i + 2) * 4), source.ReadInt(valaddr + (i + 3) * 4));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new IntPointData(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4));
                                }
                                else
                                {
                                    reval = new IntPointData(source.ReadInt(valaddr + (i + 2) * 4), source.ReadInt(valaddr + (i + 3) * 4));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new IntPointData(source.ReadInt(valaddr + (i + 2) * 4), source.ReadInt(valaddr + (i + 3) * 4));
                    }
                }
            }
            else if (typeof(T) == typeof(UIntPointData))
            {

                var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 2)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new UIntPointData(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new UIntPointData(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4));
                                break;
                            case QueryValueMatchType.After:
                                reval = new UIntPointData(source.ReadUInt(valaddr + (i + 2) * 4), source.ReadUInt(valaddr + (i + 3) * 4));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadUInt(valaddr + i * 4);
                                    var sval12 = source.ReadUInt(valaddr + (i + 1) * 4);
                                    var sval2 = source.ReadUInt(valaddr + (i + 2) * 4);
                                    var sval22 = source.ReadUInt(valaddr + (i + 3) * 4);
                                    var x1 = (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    var x2 = (uint)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                    reval = new UIntPointData(x1, x2);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new UIntPointData(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new UIntPointData(source.ReadUInt(valaddr + (i + 2) * 4), source.ReadUInt(valaddr + (i + 3) * 4));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new UIntPointData(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4));
                                }
                                else
                                {
                                    reval = new UIntPointData(source.ReadUInt(valaddr + (i + 2) * 4), source.ReadUInt(valaddr + (i + 3) * 4));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new UIntPointData(source.ReadUInt(valaddr + (i + 2) * 4), source.ReadUInt(valaddr + (i + 3) * 4));
                    }
                }
            }
            else if (typeof(T) == typeof(LongPointData))
            {

                var qq = source.ReadBytes(qs.Count * 18 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 2)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new LongPointData(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new LongPointData(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8));
                                break;
                            case QueryValueMatchType.After:
                                reval = new LongPointData(source.ReadLong(valaddr + (i + 2) * 8), source.ReadLong(valaddr + (i + 3) * 8));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadLong(valaddr + i * 8);
                                    var sval12 = source.ReadLong(valaddr + (i + 1) * 8);
                                    var sval2 = source.ReadLong(valaddr + (i + 2) * 8);
                                    var sval22 = source.ReadLong(valaddr + (i + 3) * 8);
                                    var x1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    var x2 = (long)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                    reval = new LongPointData(x1, x2);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new LongPointData(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new LongPointData(source.ReadLong(valaddr + (i + 2) * 8), source.ReadLong(valaddr + (i + 3) * 8));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new LongPointData(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8));
                                }
                                else
                                {
                                    reval = new LongPointData(source.ReadLong(valaddr + (i + 2) * 8), source.ReadLong(valaddr + (i + 3) * 8));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new LongPointData(source.ReadLong(valaddr + (i + 2) * 8), source.ReadLong(valaddr + (i + 3) * 8));
                    }
                }
            }
            else if (typeof(T) == typeof(ULongPointData))
            {

                var qq = source.ReadBytes(qs.Count * 18 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 2)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new ULongPointData(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new ULongPointData(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8));
                                break;
                            case QueryValueMatchType.After:
                                reval = new ULongPointData(source.ReadULong(valaddr + (i + 2) * 8), source.ReadULong(valaddr + (i + 3) * 8));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadLong(valaddr + i * 8);
                                    var sval12 = source.ReadLong(valaddr + (i + 1) * 8);
                                    var sval2 = source.ReadLong(valaddr + (i + 2) * 8);
                                    var sval22 = source.ReadLong(valaddr + (i + 3) * 8);
                                    var x1 = (ulong)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    var x2 = (ulong)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                    reval = new ULongPointData(x1, x2);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new ULongPointData(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new ULongPointData(source.ReadULong(valaddr + (i + 2) * 8), source.ReadULong(valaddr + (i + 3) * 8));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new ULongPointData(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8));
                                }
                                else
                                {
                                    reval = new ULongPointData(source.ReadULong(valaddr + (i + 2) * 8), source.ReadULong(valaddr + (i + 3) * 8));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new ULongPointData(source.ReadULong(valaddr + (i + 2) * 8), source.ReadULong(valaddr + (i + 3) * 8));
                    }
                }
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {

                var qq = source.ReadBytes(qs.Count * 14 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 3)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new IntPoint3Data(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4), source.ReadInt(valaddr + (i + 2) * 4));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new IntPoint3Data(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4), source.ReadInt(valaddr + (i + 2) * 4));
                                break;
                            case QueryValueMatchType.After:
                                reval = new IntPoint3Data(source.ReadInt(valaddr + (i + 3) * 4), source.ReadInt(valaddr + (i + 4) * 4), source.ReadInt(valaddr + (i + 5) * 4));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    var x3 = (int)(pval1 / tval1 * (sval23 - sval13) + sval13);
                                    reval = new IntPoint3Data(x1, x2, x3);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new IntPoint3Data(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4), source.ReadInt(valaddr + (i + 2) * 4));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new IntPoint3Data(source.ReadInt(valaddr + (i + 3) * 4), source.ReadInt(valaddr + (i + 4) * 4), source.ReadInt(valaddr + (i + 5) * 4));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new IntPoint3Data(source.ReadInt(valaddr + i * 4), source.ReadInt(valaddr + (i + 1) * 4), source.ReadInt(valaddr + (i + 2) * 4));
                                }
                                else
                                {
                                    reval = new IntPoint3Data(source.ReadInt(valaddr + (i + 3) * 4), source.ReadInt(valaddr + (i + 4) * 4), source.ReadInt(valaddr + (i + 5) * 4));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new IntPoint3Data(source.ReadInt(valaddr + (i + 3) * 4), source.ReadInt(valaddr + (i + 4) * 4), source.ReadInt(valaddr + (i + 5) * 4));
                    }
                }
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {

                var qq = source.ReadBytes(qs.Count * 26 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 3)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new LongPoint3Data(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8), source.ReadLong(valaddr + (i + 2) * 8));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new LongPoint3Data(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8), source.ReadLong(valaddr + (i + 2) * 8));
                                break;
                            case QueryValueMatchType.After:
                                reval = new LongPoint3Data(source.ReadLong(valaddr + (i + 3) * 8), source.ReadLong(valaddr + (i + 4) * 8), source.ReadLong(valaddr + (i + 5) * 8));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadLong(valaddr + i * 8);
                                    var sval12 = source.ReadLong(valaddr + (i + 1) * 8);
                                    var sval13 = source.ReadLong(valaddr + (i + 2) * 8);

                                    var sval2 = source.ReadLong(valaddr + (i + 3) * 8);
                                    var sval22 = source.ReadLong(valaddr + (i + 4) * 8);
                                    var sval23 = source.ReadLong(valaddr + (i + 5) * 8);
                                    var x1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    var x2 = (long)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                    var x3 = (long)(pval1 / tval1 * (sval23 - sval13) + sval13);
                                    reval = new LongPoint3Data(x1, x2, x3);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new LongPoint3Data(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8), source.ReadLong(valaddr + (i + 2) * 8));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new LongPoint3Data(source.ReadLong(valaddr + (i + 3) * 8), source.ReadLong(valaddr + (i + 4) * 8), source.ReadLong(valaddr + (i + 5) * 8));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new LongPoint3Data(source.ReadLong(valaddr + i * 8), source.ReadLong(valaddr + (i + 1) * 8), source.ReadLong(valaddr + (i + 2) * 8));
                                }
                                else
                                {
                                    reval = new LongPoint3Data(source.ReadLong(valaddr + (i + 3) * 8), source.ReadLong(valaddr + (i + 4) * 8), source.ReadLong(valaddr + (i + 5) * 8));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new LongPoint3Data(source.ReadLong(valaddr + (i + 3) * 8), source.ReadLong(valaddr + (i + 4) * 8), source.ReadLong(valaddr + (i + 5) * 8));
                    }
                }
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {

                var qq = source.ReadBytes(qs.Count * 14 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 3)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new UIntPoint3Data(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4), source.ReadUInt(valaddr + (i + 2) * 4));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new UIntPoint3Data(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4), source.ReadUInt(valaddr + (i + 2) * 4));
                                break;
                            case QueryValueMatchType.After:
                                reval = new UIntPoint3Data(source.ReadUInt(valaddr + (i + 3) * 4), source.ReadUInt(valaddr + (i + 4) * 4), source.ReadUInt(valaddr + (i + 5) * 4));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    var x3 = (uint)(pval1 / tval1 * (sval23 - sval13) + sval13);
                                    reval = new UIntPoint3Data(x1, x2, x3);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new UIntPoint3Data(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4), source.ReadUInt(valaddr + (i + 2) * 4));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new UIntPoint3Data(source.ReadUInt(valaddr + (i + 3) * 4), source.ReadUInt(valaddr + (i + 4) * 4), source.ReadUInt(valaddr + (i + 5) * 4));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new UIntPoint3Data(source.ReadUInt(valaddr + i * 4), source.ReadUInt(valaddr + (i + 1) * 4), source.ReadUInt(valaddr + (i + 2) * 4));
                                }
                                else
                                {
                                    reval = new UIntPoint3Data(source.ReadUInt(valaddr + (i + 3) * 4), source.ReadUInt(valaddr + (i + 4) * 4), source.ReadUInt(valaddr + (i + 5) * 4));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new UIntPoint3Data(source.ReadUInt(valaddr + (i + 3) * 4), source.ReadUInt(valaddr + (i + 4) * 4), source.ReadUInt(valaddr + (i + 5) * 4));
                    }
                }
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {

                var qq = source.ReadBytes(qs.Count * 26 + 10 + sourceAddr, qs.Count);
                var valaddr = qs.Count * 2 + 10 + sourceAddr;

                for (int i = 0; i < vv.Length - 1; i = i + 3)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time == skey.Value.Item1)
                    {
                        reval = new ULongPoint3Data(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8), source.ReadULong(valaddr + (i + 2) * 8));
                        break;
                    }
                    else if (time > skey.Value.Item1 && time < snext.Value.Item1)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                reval = new ULongPoint3Data(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8), source.ReadULong(valaddr + (i + 2) * 8));
                                break;
                            case QueryValueMatchType.After:
                                reval = new ULongPoint3Data(source.ReadULong(valaddr + (i + 3) * 8), source.ReadULong(valaddr + (i + 4) * 8), source.ReadULong(valaddr + (i + 5) * 8));
                                break;
                            case QueryValueMatchType.Linear:

                                if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
                                {
                                    var pval1 = (time1 - skey.Value.Item1).TotalMilliseconds;
                                    var tval1 = (snext.Value.Item1 - skey.Value.Item1).TotalMilliseconds;
                                    var sval1 = source.ReadULong(valaddr + i * 8);
                                    var sval12 = source.ReadULong(valaddr + (i + 1) * 8);
                                    var sval13 = source.ReadULong(valaddr + (i + 2) * 8);

                                    var sval2 = source.ReadULong(valaddr + (i + 3) * 8);
                                    var sval22 = source.ReadULong(valaddr + (i + 4) * 8);
                                    var sval23 = source.ReadULong(valaddr + (i + 5) * 8);
                                    var x1 = (ulong)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    var x2 = (ulong)(pval1 / tval1 * (sval22 - sval12) + sval12);
                                    var x3 = (ulong)(pval1 / tval1 * (sval23 - sval13) + sval13);
                                    reval = new ULongPoint3Data(x1, x2, x3);
                                }
                                else if (qq[skey.Key] < 20)
                                {
                                    reval = new ULongPoint3Data(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8), source.ReadULong(valaddr + (i + 2) * 8));
                                }
                                else if (qq[snext.Key] < 20)
                                {
                                    reval = new ULongPoint3Data(source.ReadULong(valaddr + (i + 3) * 8), source.ReadULong(valaddr + (i + 4) * 8), source.ReadULong(valaddr + (i + 5) * 8));
                                }
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time - skey.Value.Item1).TotalMilliseconds;
                                var fval = (snext.Value.Item1 - time).TotalMilliseconds;
                                if (pval < fval)
                                {
                                    reval = new ULongPoint3Data(source.ReadULong(valaddr + i * 8), source.ReadULong(valaddr + (i + 1) * 8), source.ReadULong(valaddr + (i + 2) * 8));
                                }
                                else
                                {
                                    reval = new ULongPoint3Data(source.ReadULong(valaddr + (i + 3) * 8), source.ReadULong(valaddr + (i + 4) * 8), source.ReadULong(valaddr + (i + 5) * 8));
                                }
                                break;
                        }
                    }
                    else if (time == snext.Value.Item1)
                    {
                        reval = new ULongPoint3Data(source.ReadULong(valaddr + (i + 3) * 8), source.ReadULong(valaddr + (i + 4) * 8), source.ReadULong(valaddr + (i + 5) * 8));
                    }
                }
            }

            return (T)reval;
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
        public  int DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result)
        {
            DateTime stime;
            int valuecount = 0;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out valuecount, out stime);
           
            var vv = qs.ToArray();

            var valaddr = qs.Count * 2 + 10 + sourceAddr;

            int count = 0;

            if (typeof(T) == typeof(IntPointData))
            {
                var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadInt(valaddr + i * 4);
                                        val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadInt(valaddr + (i + 2) * 4);
                                        val2 = source.ReadInt(valaddr + (i + 3) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
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
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * 14 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadInt(valaddr + i * 4);
                                        val2 = source.ReadInt(valaddr + (i + 1) * 4);
                                        val3 = source.ReadInt(valaddr + (i + 2) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadInt(valaddr + (i + 3) * 4);
                                        val2 = source.ReadInt(valaddr + (i + 4) * 4);
                                        val3 = source.ReadInt(valaddr + (i + 5) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
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
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * 14 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadUInt(valaddr + i * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        val3 = source.ReadUInt(valaddr + (i + 2) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadUInt(valaddr + (i + 3) * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 4) * 4);
                                        val3 = source.ReadUInt(valaddr + (i + 5) * 4);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
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
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                var qq = source.ReadBytes(qs.Count * 10 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadUInt(valaddr + i * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 1) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadUInt(valaddr + (i + 2) * 4);
                                        val2 = source.ReadUInt(valaddr + (i + 3) * 4);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
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
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                var qq = source.ReadBytes(qs.Count * 18 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadLong(valaddr + i * 8);
                                        val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadLong(valaddr + (i + 2) * 8);
                                        val2 = source.ReadLong(valaddr + (i + 3) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
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
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                var qq = source.ReadBytes(qs.Count * 18 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadULong(valaddr + i * 8);
                                        val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadULong(valaddr + (i + 2) * 8);
                                        val2 = source.ReadULong(valaddr + (i + 3) * 8);
                                        result.AddPoint(val, val2, time1, qq[skey.Key]);
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
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * 26 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadLong(valaddr + i * 8);
                                        val2 = source.ReadLong(valaddr + (i + 1) * 8);
                                        val3 = source.ReadLong(valaddr + (i + 2) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadLong(valaddr + (i + 3) * 8);
                                        val2 = source.ReadLong(valaddr + (i + 4) * 8);
                                        val3 = source.ReadLong(valaddr + (i + 5) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
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
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                var qq = source.ReadBytes(qs.Count * 26 + 10 + sourceAddr, qs.Count);

                foreach (var time1 in time)
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
                                    if (qq[skey.Key] < 20 && qq[snext.Key] < 20)
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
                                    else if (qq[skey.Key] < 20)
                                    {
                                        val = source.ReadULong(valaddr + i * 8);
                                        val2 = source.ReadULong(valaddr + (i + 1) * 8);
                                        val3 = source.ReadULong(valaddr + (i + 2) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
                                    }
                                    else if (qq[snext.Key] < 20)
                                    {
                                        val = source.ReadULong(valaddr + (i + 3) * 8);
                                        val2 = source.ReadULong(valaddr + (i + 4) * 8);
                                        val3 = source.ReadULong(valaddr + (i + 5) * 8);
                                        result.AddPoint(val, val2, val3, time1, qq[skey.Key]);
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
            }
            return count;
        }
    }
}
