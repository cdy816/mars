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
    public abstract class CompressUnitbase2
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

        /// <summary>
        /// 
        /// </summary>
        public int Precision { get; set; }

        /// <summary>
        /// 时间间隔单位(ms)
        /// 影响记录的时间精度
        /// </summary>
        public int TimeTick { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 统计最大值、最小值、平均值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetaddr"></param>
        protected abstract int NumberStatistic(IMemoryFixedBlock source, long startAddr, IMemoryBlock target, long targetaddr, TagType type);
        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public abstract long Compress(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, IMemoryBlock statisticTarget,long statisticAddr);



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
        public abstract int DeCompressAllValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="tp"></param>
        /// <returns></returns>
        public abstract TagHisValue<T> DeCompressRawValue<T>(MarshalMemoryBlock source, int sourceAddr, byte tp);

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
        public abstract int DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result, Func<byte, object> ReadOtherDatablockAction);

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
        public abstract object DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type, Func<byte, object> ReadOtherDatablockAction);


        public abstract CompressUnitbase2 Clone();
        

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
