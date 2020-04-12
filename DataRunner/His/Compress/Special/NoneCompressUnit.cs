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
            target.Write(this.StartTime);
            target.Write((ushort)(size - this.QulityOffset));//写入值的个数
            if (size > 0)
                source.CopyTo(target, sourceAddr, targetAddr + 10, size);
            return size + 10;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="source"></param>
        ///// <param name="sourceAddr"></param>
        ///// <param name="target"></param>
        ///// <param name="targetAddr"></param>
        ///// <returns></returns>
        //public override int Compress(MarshalMemoryBlock source, int sourceAddr, MarshalMemoryBlock target, int targetAddr, int size)
        //{

        //    //写入起始时间
        //    target.Write(this.StartTime);

        //    var qcount = source.Length - QulityOffset;
        //    Dictionary<int, byte> mqulitys = new Dictionary<int, byte>(qcount);
        //    var qulitys = source.ReadBytes(QulityOffset, qcount);

        //    for(int i=0;i<qulitys.Length;i++)
        //    {
        //        if(qulitys[i]!= (byte)QulityConst.Tick)
        //        {
        //            mqulitys.Add(i, qulitys[i]);
        //        }
        //    }

        //    MarshalMemoryBlock bval;
        //    switch(this.TagType)
        //    {
        //        case (byte)Cdy.Tag.TagType.Bool:
        //        case (byte)Cdy.Tag.TagType.Byte:
        //            bval = new MarshalMemoryBlock(mqulitys.Count);
        //            foreach (var vv in mqulitys)
        //            {
        //                bval.Write(source.ReadByte(sourceAddr+vv.Key));
        //            }
        //            break;
        //        case (byte)Cdy.Tag.TagType.Short:
        //        case (byte)Cdy.Tag.TagType.UShort:
        //            bval = new MarshalMemoryBlock(mqulitys.Count*2);
        //            foreach (var vv in mqulitys)
        //            {
        //                bval.Write(source.ReadBytes(sourceAddr+vv.Key*2));
        //            }
        //            break;
        //        case (byte)Cdy.Tag.TagType.Int:
        //        case (byte)Cdy.Tag.TagType.UInt:
        //        case (byte)Cdy.Tag.TagType.Float:
        //            bval = new MarshalMemoryBlock(mqulitys.Count * 4);
        //            foreach(var vv in mqulitys)
        //            {
        //                bval.Write(source.ReadBytes(sourceAddr + vv.Key * 4));
        //            }
        //            break;
        //        case (byte)Cdy.Tag.TagType.Long:
        //        case (byte)Cdy.Tag.TagType.ULong:
        //        case (byte)Cdy.Tag.TagType.DateTime:
        //        case (byte)Cdy.Tag.TagType.Double:
        //            bval = new MarshalMemoryBlock(mqulitys.Count * 8);
        //            foreach (var vv in mqulitys)
        //            {
        //                bval.Write(source.ReadBytes(sourceAddr + vv.Key * 8));
        //            }
        //            break;
        //        case (byte)Cdy.Tag.TagType.String:
        //            bval = new MarshalMemoryBlock(mqulitys.Count * Const.StringSize);
        //            foreach (var vv in mqulitys)
        //            {
        //                bval.Write(source.ReadBytes(sourceAddr + vv.Key * 8));
        //            }
        //            break;
        //        default:
        //            bval = new MarshalMemoryBlock(0);
        //            break;
        //    }

        //    long count = bval.Position;
        //    //写入质量戳偏移地址
        //    target.Write((int)bval.Position);
        //    //写入数值
        //    bval.CopyTo(target, 0, (int)target.Position, (int)count);
        //    count += 4 + 8;

        //    //写入时间戳,质量戳
        //    foreach (var vv in mqulitys)
        //    {
        //        target.Write((ushort)vv.Key);
        //        target.WriteByte(vv.Value);
        //    }
        //    count += mqulitys.Count * 3;
        //    return (int)count;
        //}

        /// <summary>
        /// 读取时间戳
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="timeTick"></param>
        /// <param name="startTime"></param>
        /// <returns></returns>
        private Dictionary<DateTime, int> ReadTimeQulity(MarshalMemoryBlock source, int sourceAddr, int timeTick, out DateTime startTime)
        {
            source.Position = sourceAddr;

            startTime = source.ReadDateTime();

            //读取值的个数
            int qoffset = source.ReadUShort();

            Dictionary<DateTime, int> timeQulities = new Dictionary<DateTime, int>(qoffset);

            for(int i=0;i<qoffset;i++)
            {
                timeQulities.Add(startTime.AddMilliseconds(source.ReadUShort() * timeTick), i);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<bool> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值1个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 3, qs.Count);

            //值地址
            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach(var vv in qs)
            {
                //如果时间戳大于100说明，是其他类型的值，故排除掉
                if(qq[vv.Value] <100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadByte(valaddr + i)>0;
                    result.Add(bval,vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值1个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 3, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadByte(valaddr + i);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result)
        {

            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值2个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 4, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadShort(valaddr + i * 2);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值2个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 4, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadUShort(valaddr + i * 2);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值4个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadInt(valaddr + i * 4);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值4个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadUInt(valaddr + i * 4);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result)
        {

            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadLong(valaddr + i * 8);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadULong(valaddr + i * 8);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值4个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadFloat(valaddr + i * 4);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadDouble(valaddr + i * 8);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            //读取质量戳,时间戳2个字节，值8个字节，质量戳1个字节
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;
            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }
                    var bval = source.ReadDateTime(valaddr + i * 8);
                    result.Add(bval, vv.Key, qq[vv.Value]);
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
        public override int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result)
        {
            DateTime time;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time);

            var valaddr = qs.Count * 2;

            int i = 0;
            int rcount = 0;

            List<string> ls = new List<string>();

            source.Position = valaddr;
            for (int ic=0;ic<qs.Count;ic++)
            {
                ls.Add(source.ReadString());
            }

            var qq = source.ReadBytes(qs.Count);

            foreach (var vv in qs)
            {
                if (qq[vv.Value] < 100)
                {
                    if (vv.Key < startTime || vv.Key > endTime)
                    {
                        continue;
                    }

                    result.Add(ls[i],vv.Key,qq[vv.Value]);

                    //var bval = source.ReadUShort(valaddr + i * 8);
                    //result.Add(bval, vv.Key, qq[vv.Value]);
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
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public override bool? DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            DateTime time1;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadByte(valaddr + i)>0;
                    //return  ReadBoolValue(source, sourceAddr, i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadByte(valaddr + i) > 0;
                        // return ReadBoolValue(source, sourceAddr, i);
                        case QueryValueMatchType.After:
                            return source.ReadByte(valaddr + i+1) > 0;
                        //return ReadBoolValue(source, sourceAddr, i + 1);
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadByte(valaddr + i) > 0;
                                // return ReadBoolValue(source, sourceAddr, i);
                            }
                            else
                            {
                                return source.ReadByte(valaddr + i + 1) > 0;
                                //return ReadBoolValue(source, sourceAddr, i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadByte(valaddr + i+1) > 0;
                    //  return ReadBoolValue(source, sourceAddr, i+1);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 3, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadByte(valaddr + i) > 0;
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadByte(valaddr + i) > 0;
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                                
                            case QueryValueMatchType.After:
                                val = source.ReadByte(valaddr + i+1) > 0;
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;
                                
                                if (pval < fval)
                                {
                                    val = source.ReadByte(valaddr + i) > 0;
                                    result.Add(val, time1, qq[skey.Value]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadByte(valaddr + i + 1) > 0;
                                    result.Add(val, time1, qq[snext.Value]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadByte(valaddr + i + 1) > 0;
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadByte(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadByte(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadByte(valaddr + i + 1);
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
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
                else if (time == snext.Key)
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 3, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadByte(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadByte(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = source.ReadByte(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadByte(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadByte(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadByte(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadDateTime(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadDateTime(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadDateTime(valaddr + i + 1);
                        case QueryValueMatchType.Linear:
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadDateTime(valaddr + i);
                            }
                            else
                            {
                                return source.ReadDateTime(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadDateTime(valaddr + i + 1);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadDateTime(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadDateTime(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;

                            case QueryValueMatchType.After:
                                val = source.ReadDateTime(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:

                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = source.ReadDateTime(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                    break;
                                }
                                else
                                {
                                    val = source.ReadDateTime(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadDateTime(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadDouble(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadDouble(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadDouble(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadDouble(valaddr + i);
                                var sval2 = source.ReadDouble(valaddr + i + 1);
                                return (double)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadDouble(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadDouble(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadDouble(valaddr + i);
                            }
                            else
                            {
                                return source.ReadDouble(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadDouble(valaddr + i + 1);
                }

            }
            return null;

            
        }

        public override int DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result)
        {
            DateTime stime;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadDouble(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadDouble(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadDouble(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadDouble(valaddr + i);
                                    var sval2 = source.ReadDouble(valaddr + i + 1);
                                    var val1 = pval1 / tval1 * (sval2 - sval1) + sval1;
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadDouble(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadDouble(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadDouble(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadDouble(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadDouble(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadFloat(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadFloat(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadFloat(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadFloat(valaddr + i);
                                var sval2 = source.ReadFloat(valaddr + i + 1);
                                return (float)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadFloat(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadFloat(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadFloat(valaddr + i);
                            }
                            else
                            {
                                return source.ReadFloat(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadFloat(valaddr + i + 1);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadFloat(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadFloat(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadFloat(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadFloat(valaddr + i);
                                    var sval2 = source.ReadFloat(valaddr + i + 1);
                                    var val1 = (float)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadFloat(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadFloat(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadFloat(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadFloat(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadFloat(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadInt(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadInt(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadInt(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadInt(valaddr + i);
                                var sval2 = source.ReadInt(valaddr + i + 1);
                                return (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadInt(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadInt(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadInt(valaddr + i);
                            }
                            else
                            {
                                return source.ReadInt(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadInt(valaddr + i + 1);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadInt(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadInt(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadInt(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadInt(valaddr + i);
                                    var sval2 = source.ReadInt(valaddr + i + 1);
                                    var val1 = (int)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadInt(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadInt(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadInt(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadInt(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadInt(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadInt(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadLong(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadLong(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadLong(valaddr + i);
                                var sval2 = source.ReadLong(valaddr + i + 1);
                                return (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadLong(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadLong(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadLong(valaddr + i);
                            }
                            else
                            {
                                return source.ReadLong(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadLong(valaddr + i + 1);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 10,qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadLong(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadLong(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadLong(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadLong(valaddr + i);
                                    var sval2 = source.ReadLong(valaddr + i + 1);
                                    var val1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadLong(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadLong(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadLong(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadLong(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadLong(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 4, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadShort(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadShort(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadShort(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadShort(valaddr + i);
                                var sval2 = source.ReadShort(valaddr + i + 1);
                                return (short)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadShort(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadShort(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadShort(valaddr + i);
                            }
                            else
                            {
                                return source.ReadShort(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadShort(valaddr + i + 1);
                }

            }
            return null;
        }

        public override int DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result)
        {
            DateTime stime;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadShort(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadShort(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadShort(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadShort(valaddr + i);
                                    var sval2 = source.ReadShort(valaddr + i + 1);
                                    var val1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadShort(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadShort(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadShort(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadShort(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadShort(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            
            var valaddr = qs.Count * 2;
            Dictionary<int, string> dtmp = new Dictionary<int, string>();
            source.Position = valaddr;

            for(int i=0;i<qs.Count;i++)
            {
                dtmp.Add(i, source.ReadString());
            }

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
        public override int DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result)
        {
            DateTime stime;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);

            //var dtmp = source.ToStringList(sourceAddr + 12, Encoding.Unicode);

            var valaddr = qs.Count * 2;
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

                    if (time1 == skey.Key)
                    {
                        var val = dtmp[i];
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = dtmp[i];
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = dtmp[i+1];
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey.Key).TotalMilliseconds;
                                var fval = (snext.Key - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = dtmp[i];
                                    result.Add(val, time1, qq[skey.Value]);
                                    break;
                                }
                                else
                                {
                                    val = dtmp[i + 1];
                                    result.Add(val, time1, qq[snext.Value]);
                                    break;
                                }
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = dtmp[i];
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadUInt(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadUInt(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadUInt(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadUInt(valaddr + i);
                                var sval2 = source.ReadUInt(valaddr + i + 1);
                                return (uint)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadUInt(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadUInt(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadUInt(valaddr + i);
                            }
                            else
                            {
                                return source.ReadUInt(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadUInt(valaddr + i + 1);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 6, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadUInt(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadUInt(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadUInt(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadUInt(valaddr + i);
                                    var sval2 = source.ReadUInt(valaddr + i + 1);
                                    var val1 = (long)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadUInt(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadUInt(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadUInt(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadUInt(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadUInt(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadULong(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadULong(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadULong(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadULong(valaddr + i);
                                var sval2 = source.ReadULong(valaddr + i + 1);
                                return (ulong)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadULong(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadULong(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadULong(valaddr + i);
                            }
                            else
                            {
                                return source.ReadULong(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadULong(valaddr + i + 1);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 10, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadULong(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadULong(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadULong(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadULong(valaddr + i);
                                    var sval2 = source.ReadULong(valaddr + i + 1);
                                    var val1 = (ulong)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadULong(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadULong(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadULong(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadULong(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadULong(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
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
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out time1);
            var qq = source.ReadBytes(qs.Count * 4, qs.Count);

            var valaddr = qs.Count * 2;

            var vv = qs.ToArray();

            for (int i = 0; i < vv.Length - 1; i++)
            {
                var skey = vv[i];

                var snext = vv[i + 1];

                if (time == skey.Key)
                {
                    return source.ReadUShort(valaddr + i);
                }
                else if (time > skey.Key && time < snext.Key)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return source.ReadUShort(valaddr + i);
                        case QueryValueMatchType.After:
                            return source.ReadUShort(valaddr + i + 1);
                        case QueryValueMatchType.Linear:

                            if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                            {
                                var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                var sval1 = source.ReadUShort(valaddr + i);
                                var sval2 = source.ReadUShort(valaddr + i + 1);
                                return (ushort)(pval1 / tval1 * (sval2 - sval1) + sval1);
                            }
                            else if (qq[skey.Value] < 20)
                            {
                                return source.ReadUShort(valaddr + i);
                            }
                            else if (qq[snext.Value] < 20)
                            {
                                return source.ReadUShort(valaddr + i + 1);
                            }
                            break;

                        case QueryValueMatchType.Closed:
                            var pval = (time - skey.Key).TotalMilliseconds;
                            var fval = (snext.Key - time).TotalMilliseconds;
                            if (pval < fval)
                            {
                                return source.ReadUShort(valaddr + i);
                            }
                            else
                            {
                                return source.ReadUShort(valaddr + i + 1);
                            }
                    }
                }
                else if (time == snext.Key)
                {
                    return source.ReadUShort(valaddr + i + 1);
                }

            }
            return null;
        }

        public override int DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result)
        {
            DateTime stime;
            var qs = ReadTimeQulity(source, sourceAddr, timeTick, out stime);
            var qq = source.ReadBytes(qs.Count * 4, qs.Count);

            var vv = qs.ToArray();

            var valaddr = qs.Count * 2;

            int count = 0;
            foreach (var time1 in time)
            {
                for (int i = 0; i < vv.Length - 1; i++)
                {
                    var skey = vv[i];

                    var snext = vv[i + 1];

                    if (time1 == skey.Key)
                    {
                        var val = source.ReadUShort(valaddr + i);
                        result.Add(val, time1, qq[skey.Value]);
                        count++;
                        break;
                    }
                    else if (time1 > skey.Key && time1 < snext.Key)
                    {

                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = source.ReadUShort(valaddr + i);
                                result.Add(val, time1, qq[skey.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.After:
                                val = source.ReadUShort(valaddr + i + 1);
                                result.Add(val, time1, qq[snext.Value]);
                                count++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qq[skey.Value] < 20 && qq[snext.Value] < 20)
                                {
                                    var pval1 = (time1 - skey.Key).TotalMilliseconds;
                                    var tval1 = (snext.Key - skey.Key).TotalMilliseconds;
                                    var sval1 = source.ReadUShort(valaddr + i);
                                    var sval2 = source.ReadUShort(valaddr + i + 1);
                                    var val1 = (ushort)(pval1 / tval1 * (sval2 - sval1) + sval1);
                                    result.Add(val1, time1, 0);
                                }
                                else if (qq[skey.Value] < 20)
                                {
                                    val = source.ReadUShort(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else if (qq[snext.Value] < 20)
                                {
                                    val = source.ReadUShort(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
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
                                    val = source.ReadUShort(valaddr + i);
                                    result.Add(val, time1, qq[skey.Value]);
                                }
                                else
                                {
                                    val = source.ReadUShort(valaddr + i + 1);
                                    result.Add(val, time1, qq[snext.Value]);
                                }
                                break;
                        }
                        count++;
                        break;
                    }
                    else if (time1 == snext.Key)
                    {
                        var val = source.ReadUShort(valaddr + i + 1);
                        result.Add(val, time1, qq[snext.Value]);
                        count++;
                        break;
                    }

                }
            }
            return count;
        }
    }
}
