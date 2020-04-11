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

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 实时值地址
        /// </summary>
        public byte[] RealMemoryAddr { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public IntPtr RealMemoryPtr { get; set; }

        /// <summary>
        /// 实时值的内存地址
        /// </summary>
        public int RealValueAddr { get; set; }

        /// <summary>
        /// 历史缓存数据偏移地址
        /// </summary>
        public static MarshalMemoryBlock HisAddr { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static DateTime StartTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static int TimerOffset { get; set; } = 0;

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

        ///// <summary>
        ///// 
        ///// </summary>
        //public static int HeadSize
        //{
        //    get
        //    {
        //        return 4;
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        //public byte[] HeadDatas
        //{
        //    get
        //    {
        //        return headBytes;
        //    }
        //}


        #endregion ...Properties...

        #region ... Methods    ...

        //public void Init()
        //{
        //    var hbyts = new List<byte>(4);
        //    hbyts.AddRange(BitConverter.GetBytes(this.HisQulityStartAddr - this.TimerValueStartAddr));
        //    headBytes = hbyts.ToArray();
        //}

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            Count = 0;
        }

        /// <summary>
        /// 内部计数更新
        /// 不做实际的数据更新
        /// </summary>
        public void UpdateNone()
        {
            Count = ++Count > MaxCount ? MaxCount : Count;
            HisAddr[HisQulityStartAddr + Count] = (byte)QulityConst.Tick;
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
        /// 
        /// </summary>
        /// <param name="tim"></param>
        public void UpdateValue(int count,int tim)
        {
            //lock (mLockTest)
            //{
                var vcount = count > MaxCount ? MaxCount : count;
                Count = vcount;

                //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
                //实时数据内存结构为:实时值+时间戳+质量戳
                HisAddr.WriteIntDirect(TimerValueStartAddr + vcount * 2, tim + TimerOffset);

                //写入数值
                //HisAddr.WriteBytesDirect(HisValueStartAddr + vcount * SizeOfValue, RealMemoryAddr, RealValueAddr, SizeOfValue);

                HisAddr.WriteBytesDirect(HisValueStartAddr + vcount * SizeOfValue, RealMemoryPtr, RealValueAddr, SizeOfValue);

                //更新质量戳
                HisAddr.WriteByteDirect(HisQulityStartAddr + vcount, RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);
            //}
        }

        /// <summary>
        /// 更新历史数据到缓存中
        /// </summary>
        public void UpdateValue(int tim)
        {
            UpdateValue(Count, tim);
            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
