﻿//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe abstract class HisRunTag : HisTag
    {

        #region ... Variables  ...

        //private byte[] headBytes;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
            /// <summary>
            /// 
            /// </summary>
        public HisRunTag()
        {
            ValueSnape = new byte[SizeOfValue];
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 实时值地址
        /// </summary>
        public byte[] RealMemoryAddr { get; set; }

        /// <summary>
        /// 实时值地址指针
        /// </summary>
        public IntPtr RealMemoryPtr { get; set; }

        /// <summary>
        /// 实时值的内存地址
        /// </summary>
        public int RealValueAddr { get; set; }

        /// <summary>
        /// 历史缓存数据偏移地址
        /// </summary>
        [Obsolete]
        public static MarshalFixedMemoryBlock HisAddr { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static DateTime StartTime { get; set; }

        /// <summary>
        /// 历史值起始地址
        /// </summary>
        public HisDataMemoryBlock HisValueMemoryStartAddr { get; set; }

        /// <summary>
        /// 历史值缓存1
        /// </summary>
        public HisDataMemoryBlock HisValueMemory1 { get; set; }

        /// <summary>
        /// 历史值缓存2
        /// </summary>
        public HisDataMemoryBlock HisValueMemory2 { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public static int TimerOffset { get; set; } = 0;

        /// <summary>
        /// 头部信息起始地址
        /// </summary>
        public long BlockHeadStartAddr { get; set; }

        /// <summary>
        /// 时间戳存访地址
        /// </summary>
        public long TimerValueStartAddr { get; set; }

        /// <summary>
        /// 历史数据缓存内存起始地址
        /// </summary>
        public long HisValueStartAddr { get; set; }

        /// <summary>
        /// 质量戳起始地址
        /// </summary>
        public long HisQulityStartAddr { get; set; }

        /// <summary>
        /// 数据内存大小
        /// </summary>
        public long DataSize { get; set; }

        /// <summary>
        /// 已经记录的个数
        /// </summary>
        public int Count { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int MaxCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public virtual byte SizeOfValue { get; }

        /// <summary>
        /// 值快照
        /// </summary>
        public byte[] ValueSnape { get; set; }

        /// <summary>
        /// 质量戳快照
        /// </summary>
        public byte QulitySnape { get; set; }

        /// <summary>
        /// 数据精度
        /// </summary>
        public int Precision { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            Count = 0;
        }


        /// <summary>
        /// 清空数值区
        /// </summary>
        /// <param name="block"></param>
        public void ClearDataValue(MemoryBlock memory)
        {
            memory.Clear(TimerValueStartAddr, DataSize);
        }

        /// <summary>
        /// 执行快照
        /// </summary>
        public void Snape()
        {
            System.Runtime.InteropServices.Marshal.Copy(RealMemoryPtr + RealValueAddr, ValueSnape, 0, SizeOfValue);
            QulitySnape = RealMemoryAddr[RealValueAddr + SizeOfValue + 8];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tim"></param>
        [Obsolete]
        public void UpdateValue(int count,int tim)
        {
            //lock (mLockTest)
            //{
                var vcount = count > MaxCount ? MaxCount : count;
                Count = vcount;

                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
                HisAddr.WriteUShortDirect(TimerValueStartAddr + vcount * 2, (ushort)(tim));

            //写入数值
            //HisAddr.WriteBytesDirect(HisValueStartAddr + vcount * SizeOfValue, RealMemoryAddr, RealValueAddr, SizeOfValue);
            //LoggerService.Service.Erro("HisTag","read from realmemory:"+ MemoryHelper.ReadDouble((void*)RealMemoryPtr, RealValueAddr));
                HisAddr.WriteBytesDirect(HisValueStartAddr + vcount * SizeOfValue, RealMemoryPtr, RealValueAddr, SizeOfValue);

                //更新质量戳
                HisAddr.WriteByteDirect(HisQulityStartAddr + vcount, RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);
            //}
        }

        /// <summary>
        /// 更新历史数据到缓存中
        /// </summary>
        [Obsolete]
        public void UpdateValue(int tim)
        {
            UpdateValue(Count, tim);
            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <param name="tim"></param>
        public void UpdateValue2(int count, int tim)
        {
            var vcount = count > MaxCount ? MaxCount : count;
            Count = vcount;

            //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
            HisValueMemoryStartAddr.WriteUShortDirect((int)(HisValueMemoryStartAddr.TimerAddress + vcount * 2), (ushort)(tim));

            //写入数值
           // Array.Copy(RealMemoryAddr, RealValueAddr, HisValueMemoryStartAddr.Buffers, (int)(HisValueMemoryStartAddr.ValueAddress + vcount * SizeOfValue), SizeOfValue);

            HisValueMemoryStartAddr.WriteBytesDirect((int)(HisValueMemoryStartAddr.ValueAddress + vcount * SizeOfValue), RealMemoryPtr, RealValueAddr, SizeOfValue);

            //更新质量戳
            HisValueMemoryStartAddr.WriteByteDirect((int)(HisValueMemoryStartAddr.QualityAddress + vcount), RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tim"></param>
        public void UpdateValue2(int tim)
        {
            UpdateValue2(Count, tim);
            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
