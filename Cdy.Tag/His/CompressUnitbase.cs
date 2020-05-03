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

namespace Cdy.Tag
{
    /// <summary>
    /// 压缩单元
    /// </summary>
    public abstract class CompressUnitbase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 描述
        /// </summary>
        public abstract string Desc { get; }

        /// <summary>
        /// 类型编码
        /// </summary>
        public abstract int TypeCode { get; }

        /// <summary>
        /// 质量戳位置偏移
        /// </summary>
        public int QulityOffset { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public RecordType RecordType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// 变量类型
        /// </summary>
        public TagType TagType { get; set; }

        /// <summary>
        /// 压缩类型
        /// </summary>
        public byte CompressType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string,double> Parameters { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public float CompressParameter1 { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public float CompressParameter2 { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public float CompressParameter3 { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public abstract long Compress(MarshalMemoryBlock source, long sourceAddr, MarshalMemoryBlock target, long targetAddr, long size);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        public abstract bool? DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr,DateTime time,int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressBoolValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<bool> result);

        /// <summary>
        /// 解压缩某个事件段内所有值
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime,DateTime endTime, int timeTick, HisQueryResult<bool> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract byte? DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressByteValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<byte> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<byte> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract short? DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<short> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<short> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract ushort? DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressUShortValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ushort> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ushort> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int? DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<int> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<int> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract uint? DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressUIntValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<uint> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<uint> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract long? DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressLongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<long> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<long> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract ulong? DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressULongValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<ulong> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<ulong> result);


        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        public abstract float? DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        public abstract int DeCompressFloatValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<float> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<float> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        public abstract double? DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <returns></returns>
        public abstract int DeCompressDoubleValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<double> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<double> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract DateTime? DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressDateTimeValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<DateTime> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<DateTime> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract string DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public abstract int DeCompressStringValue(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<string> result);

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
        public abstract int DeCompressAllValue(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<string> result);



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
        public abstract int DeCompressAllPointValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result);

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
        public abstract T DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type);

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
        public abstract int DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result);


        public abstract CompressUnitbase Clone();
        

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
