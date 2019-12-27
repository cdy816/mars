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
        /// <returns></returns>
        public override int Compress(RecordMemory source, int sourceAddr, RecordMemory target, int targetAddr, int size)
        {

            //写入起始时间
            target.Write(this.StartTime);

            var qcount = source.Length - QulityOffset;
            Dictionary<int, byte> mqulitys = new Dictionary<int, byte>(qcount);
            var qulitys = source.ReadBytes(QulityOffset, qcount);

            for(int i=0;i<qulitys.Length;i++)
            {
                if(qulitys[i]!= (byte)QulityConst.Tick)
                {
                    mqulitys.Add(i, qulitys[i]);
                }
            }

            RecordMemory bval;
            switch(this.TagType)
            {
                case (byte)Cdy.Tag.TagType.Bool:
                case (byte)Cdy.Tag.TagType.Byte:
                    bval = new RecordMemory(mqulitys.Count);
                    foreach (var vv in mqulitys)
                    {
                        bval.Write(source.ReadByte(sourceAddr+vv.Key));
                    }
                    break;
                case (byte)Cdy.Tag.TagType.Short:
                case (byte)Cdy.Tag.TagType.UShort:
                    bval = new RecordMemory(mqulitys.Count*2);
                    foreach (var vv in mqulitys)
                    {
                        bval.Write(source.ReadBytes(sourceAddr+vv.Key*2));
                    }
                    break;
                case (byte)Cdy.Tag.TagType.Int:
                case (byte)Cdy.Tag.TagType.UInt:
                case (byte)Cdy.Tag.TagType.Float:
                    bval = new RecordMemory(mqulitys.Count * 4);
                    foreach(var vv in mqulitys)
                    {
                        bval.Write(source.ReadBytes(sourceAddr + vv.Key * 4));
                    }
                    break;
                case (byte)Cdy.Tag.TagType.Long:
                case (byte)Cdy.Tag.TagType.ULong:
                case (byte)Cdy.Tag.TagType.DateTime:
                case (byte)Cdy.Tag.TagType.Double:
                    bval = new RecordMemory(mqulitys.Count * 8);
                    foreach (var vv in mqulitys)
                    {
                        bval.Write(source.ReadBytes(sourceAddr + vv.Key * 8));
                    }
                    break;
                case (byte)Cdy.Tag.TagType.String:
                    bval = new RecordMemory(mqulitys.Count * RealEnginer.StringSize);
                    foreach (var vv in mqulitys)
                    {
                        bval.Write(source.ReadBytes(sourceAddr + vv.Key * 8));
                    }
                    break;
                default:
                    bval = new RecordMemory(0);
                    break;
            }

            long count = bval.Position;
            //写入质量戳偏移地址
            target.Write((int)bval.Position);
            //写入数值
            bval.CopyTo(target, 0, (int)target.Position, (int)count);
            count += 4 + 8;

            //写入时间戳,质量戳
            foreach (var vv in mqulitys)
            {
                target.Write((ushort)vv.Key);
                target.WriteByte(vv.Value);
            }
            count += mqulitys.Count * 3;
            return (int)count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        private Dictionary<DateTime, byte> ReadQulity(RecordMemory source,int sourceAddr,int timeTick, out DateTime startTime)
        {
            source.Position = sourceAddr;

            startTime = source.ReadDateTime();

            //读取质量戳偏移
            int qoffset = source.ReadInt();

            Dictionary<DateTime, byte> timeQulities = new Dictionary<DateTime, byte>();
            while(source.Position<source.Length)
            {
                timeQulities.Add(startTime.AddTicks(source.ReadUShort()* timeTick), source.ReadByte());
            }
            return timeQulities;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="startTime"></param>
        /// <returns></returns>
        private Dictionary<int, byte> ReadQulity2(RecordMemory source, int sourceAddr, out DateTime startTime)
        {
            source.Position = sourceAddr;

            startTime = source.ReadDateTime();

            //读取质量戳偏移
            int qoffset = source.ReadInt();

            Dictionary<int, byte> timeQulities = new Dictionary<int, byte>();
            while (source.Position < source.Length)
            {
                timeQulities.Add(source.ReadUShort(), source.ReadByte());
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);
            
            int i = 0;
            int rcount = 0;
            foreach(var vv in qs)
            {
                if(vv.Value <100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }

                    var bval = source.ReadByte(8 + 4 + i)>0;
                    result.Add(bval,vv.Key, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadByte(8 + 4 + i);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadShort(8 + 4 + i*2);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadUShort(8 + 4 + i * 2);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadInt(8 + 4 + i * 4);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadUInt(8 + 4 + i * 4);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadLong(8 + 4 + i * 8);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadULong(8 + 4 + i * 8);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadFloat(8 + 4 + i * 4);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadDouble(8 + 4 + i * 8);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadDateTime(8 + 4 + i * 8);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
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
        public override int DeCompressAllValue(RecordMemory source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            DateTime time;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time);

            int i = 0;
            int rcount = 0;
            source.Position = 12;
            foreach (var vv in qs)
            {
                if (vv.Value < 100)
                {
                    DateTime stime = vv.Key;
                    if (stime < startTime || stime > endTime)
                    {
                        i++;
                        continue;
                    }
                    var bval = source.ReadString(Encoding.Unicode);
                    result.Add(bval, stime, vv.Value);
                    rcount++;
                }
                i++;
            }
            return rcount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        private bool ReadBoolValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadByte(8 + 4 + sourceAddr + index) > 0 ? true : false;
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
        public override bool DeCompressBoolValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadBoolValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadBoolValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadBoolValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadBoolValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadBoolValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadBoolValue(source, sourceAddr, i+1);
                }

            }
            return false;
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
        public override int DeCompressBoolValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadBoolValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadBoolValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                                
                            case QueryValueMatchType.After:
                                val = ReadBoolValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;
                                
                                if (pval < fval)
                                {
                                    val = ReadBoolValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadBoolValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadBoolValue(source, sourceAddr, i+1);
                        result.Add(val, time1, snext.Value);
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
        /// <param name="index"></param>
        /// <returns></returns>
        private byte ReadByteValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadByte(8 + 4 + sourceAddr + index);
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
        public override byte DeCompressByteValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadByteValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadByteValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadByteValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadByteValue(source, sourceAddr, i);
                                var sval2 = ReadByteValue(source, sourceAddr, i + 1);
                                return (byte)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadByteValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadByteValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadByteValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadByteValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadByteValue(source, sourceAddr, i + 1);
                }

            }
            return byte.MinValue;
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
        public override int DeCompressByteValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadByteValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadByteValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = ReadByteValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadByteValue(source, sourceAddr, i);
                                    var sval2 = ReadByteValue(source, sourceAddr, i + 1);
                                    var val1 = (byte)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadByteValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadByteValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadByteValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadByteValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadByteValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
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
        /// <param name="index"></param>
        /// <returns></returns>
        private DateTime ReadDateTimeValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadDateTime(8 + 4 + sourceAddr + index * 8);
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
        public override DateTime DeCompressDateTimeValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadDateTimeValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadDateTimeValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadDateTimeValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadDateTimeValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadDateTimeValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadDateTimeValue(source, sourceAddr, i + 1);
                }

            }
            return DateTime.MinValue;
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
        public override int DeCompressDateTimeValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadDateTimeValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadDateTimeValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = ReadDateTimeValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadDateTimeValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadDateTimeValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadDateTimeValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
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
        /// <param name="index"></param>
        /// <returns></returns>
        private double ReadDoubleValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadDouble(8 + 4 + sourceAddr + index * 8);
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
        public override double DeCompressDoubleValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadDoubleValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadDoubleValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadDoubleValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadDoubleValue(source, sourceAddr, i);
                                var sval2 = ReadDoubleValue(source, sourceAddr, i + 1);
                                return (double)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadDoubleValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadDoubleValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadDoubleValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadDoubleValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadDoubleValue(source, sourceAddr, i + 1);
                }

            }
            return double.MinValue;
        }

        public override int DeCompressDoubleValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadDoubleValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadDoubleValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadDoubleValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadDoubleValue(source, sourceAddr, i);
                                    var sval2 = ReadDoubleValue(source, sourceAddr, i + 1);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadDoubleValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadDoubleValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadDoubleValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadDoubleValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadDoubleValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
                        count++;
                        break;
                    }

                }
            }
            return count;
        }


        private float ReadFloatValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadFloat(8 + 4 + sourceAddr + index * 4);
        }

        public override float DeCompressFloatValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadFloatValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadFloatValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadFloatValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadFloatValue(source, sourceAddr, i);
                                var sval2 = ReadFloatValue(source, sourceAddr, i + 1);
                                return (float)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadFloatValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadFloatValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadFloatValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadFloatValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadFloatValue(source, sourceAddr, i + 1);
                }

            }
            return float.MinValue;
        }

        public override int DeCompressFloatValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<float> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadFloatValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadFloatValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadFloatValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadFloatValue(source, sourceAddr, i);
                                    var sval2 = ReadFloatValue(source, sourceAddr, i + 1);
                                    var val1 = (float)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadFloatValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadFloatValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadFloatValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadFloatValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadFloatValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
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
        /// <param name="index"></param>
        /// <returns></returns>
        private int ReadIntValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadInt(8 + 4 + sourceAddr + index * 4);
        }

        public override int DeCompressIntValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadIntValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadIntValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadIntValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadIntValue(source, sourceAddr, i);
                                var sval2 = ReadIntValue(source, sourceAddr, i + 1);
                                return (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadIntValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadIntValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadIntValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadIntValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadIntValue(source, sourceAddr, i + 1);
                }

            }
            return int.MinValue;
        }

        public override int DeCompressIntValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<int> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadIntValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadIntValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadIntValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadIntValue(source, sourceAddr, i);
                                    var sval2 = ReadIntValue(source, sourceAddr, i + 1);
                                    var val1 = (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadIntValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadIntValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadIntValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadIntValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadFloatValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
                        count++;
                        break;
                    }

                }
            }
            return count;
        }

        private long ReadLongValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadLong(8 + 4 + sourceAddr + index * 8);
        }

        public override long DeCompressLongValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadLongValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadLongValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadLongValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadLongValue(source, sourceAddr, i);
                                var sval2 = ReadLongValue(source, sourceAddr, i + 1);
                                return (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadLongValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadLongValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadLongValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadLongValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadLongValue(source, sourceAddr, i + 1);
                }

            }
            return long.MinValue;
        }

        public override int DeCompressLongValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<long> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadLongValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadLongValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadLongValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadLongValue(source, sourceAddr, i);
                                    var sval2 = ReadLongValue(source, sourceAddr, i + 1);
                                    var val1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadLongValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadLongValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadLongValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadLongValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadLongValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
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
        /// <param name="index"></param>
        /// <returns></returns>
        private short ReadShortValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadShort(8 + 4 + sourceAddr + index * 2);
        }

        public override short DeCompressShortValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadShortValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadShortValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadShortValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadShortValue(source, sourceAddr, i);
                                var sval2 = ReadShortValue(source, sourceAddr, i + 1);
                                return (short)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadShortValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadShortValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadShortValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadShortValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadShortValue(source, sourceAddr, i + 1);
                }

            }
            return short.MinValue;
        }

        public override int DeCompressShortValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadShortValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadShortValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadShortValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadShortValue(source, sourceAddr, i);
                                    var sval2 = ReadShortValue(source, sourceAddr, i + 1);
                                    var val1 = (short)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadShortValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadShortValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadShortValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadShortValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadShortValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
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
        public override string DeCompressStringValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);
            var dtmp = source.ToStringList(sourceAddr + 12, Encoding.Unicode);
            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return dtmp[i];
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return dtmp[i];
                        case QueryValueMatchType.After:
                            return dtmp[i+1];
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
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
                else if (time == snext.Key)
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
        public override int DeCompressStringValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var dtmp = source.ToStringList(sourceAddr+12,Encoding.Unicode);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = dtmp[i];
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = dtmp[i];
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = dtmp[i+1];
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = dtmp[i];
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = dtmp[i + 1];
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = dtmp[i];
                        result.Add(val, time1, snext.Value);
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
        /// <param name="index"></param>
        /// <returns></returns>
        private uint ReadUIntValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadUInt(8 + 4 + sourceAddr + index * 4);
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
        public override uint DeCompressUIntValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadUIntValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadUIntValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadUIntValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadUIntValue(source, sourceAddr, i);
                                var sval2 = ReadUIntValue(source, sourceAddr, i + 1);
                                return (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadUIntValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadUIntValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadUIntValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadUIntValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadUIntValue(source, sourceAddr, i + 1);
                }

            }
            return uint.MinValue;
        }

        public override int DeCompressUIntValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val =  ReadUIntValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadUIntValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadUIntValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadUIntValue(source, sourceAddr, i);
                                    var sval2 = ReadUIntValue(source, sourceAddr, i + 1);
                                    var val1 = (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadUIntValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadUIntValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadUIntValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadUIntValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadUIntValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
                        count++;
                        break;
                    }

                }
            }
            return count;
        }


        private ulong ReadULongValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadULong(8 + 4 + sourceAddr + index * 4);
        }

        public override ulong DeCompressULongValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadUIntValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadULongValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadULongValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadULongValue(source, sourceAddr, i);
                                var sval2 = ReadULongValue(source, sourceAddr, i + 1);
                                return (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadULongValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadULongValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadULongValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadULongValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadULongValue(source, sourceAddr, i + 1);
                }

            }
            return ulong.MinValue;
        }

        public override int DeCompressULongValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadShortValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadULongValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadULongValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadULongValue(source, sourceAddr, i);
                                    var sval2 = ReadULongValue(source, sourceAddr, i + 1);
                                    var val1 = (ulong)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadULongValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadULongValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadULongValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadULongValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadULongValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
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
        /// <param name="index"></param>
        /// <returns></returns>
        private ushort ReadUShortValue(RecordMemory source, int sourceAddr, int index)
        {
            return source.ReadUShort(8 + 4 + sourceAddr + index * 2);
        }

        public override ushort DeCompressUShortValue(RecordMemory source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadQulity(source, sourceAddr, timeTick, out time1);

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return ReadUShortValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return ReadUShortValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return ReadUShortValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                            if (skey.Value < 20 && snext.Value < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = ReadUShortValue(source, sourceAddr, i);
                                var sval2 = ReadUShortValue(source, sourceAddr, i + 1);
                                return (ushort)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (skey.Value < 20)
                            {
                                return ReadUShortValue(source, sourceAddr, i);
                            }
                            else if (snext.Value < 20)
                            {
                                return ReadUShortValue(source, sourceAddr, i + 1);
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return ReadUShortValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return ReadUShortValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return ReadUShortValue(source, sourceAddr, i + 1);
                }

            }
            return ushort.MinValue;
        }

        public override int DeCompressUShortValue(RecordMemory source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            DateTime stime;
            var qs = ReadQulity(source, sourceAddr, timeTick, out stime);

            var vv = qs.ToArray();
            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = ReadUShortValue(source, sourceAddr, i);
                        result.Add(val, time1, skey.Value);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = ReadUShortValue(source, sourceAddr, i);
                                result.Add(val, time1, skey.Value);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = ReadUShortValue(source, sourceAddr, i + 1);
                                result.Add(val, time1, snext.Value);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (skey.Value < 20 && snext.Value < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = ReadUShortValue(source, sourceAddr, i);
                                    var sval2 = ReadUShortValue(source, sourceAddr, i + 1);
                                    var val1 = (ushort)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (skey.Value < 20)
                                {
                                    val = ReadUShortValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                }
                                else if (snext.Value < 20)
                                {
                                    val = ReadUShortValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QulityConst.Null);
                                }
                                count++;
                                break;

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = ReadUShortValue(source, sourceAddr, i);
                                    result.Add(val, time1, skey.Value);
                                    break;
                                }
                                else
                                {
                                    val = ReadUShortValue(source, sourceAddr, i + 1);
                                    result.Add(val, time1, snext.Value);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = ReadUShortValue(source, sourceAddr, i + 1);
                        result.Add(val, time1, snext.Value);
                        count++;
                        break;
                    }

                }
            }
            return count;
        }
    }
}
