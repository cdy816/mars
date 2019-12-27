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
    public  abstract class HisRunTag:HisTag
    {

        #region ... Variables  ...

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
        /// 实时值的内存地址
        /// </summary>
        public int RealValueAddr { get; set; }

        /// <summary>
        /// 历史缓存数据偏移地址
        /// </summary>
        public static byte[] HisAddr { get; set; }

        /// <summary>
        /// 头部信息起始地址
        /// </summary>
        public int BlockHeadStartAddr { get; set; }

        /// <summary>
        /// 历史数据缓存内存起始地址
        /// </summary>
        public int HisValueStartAddr { get; set; }

        /// <summary>
        /// 质量戳起始地址
        /// </summary>
        public int HisQulityStartAddr { get; set; }

        /// <summary>
        /// 数据内存大小
        /// </summary>
        public int DataSize { get; set; }

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
        /// 内部计数更新
        /// 不做实际的数据更新
        /// </summary>
        public void UpdateNone()
        {
            Count = ++Count > MaxCount ? MaxCount : Count;
            HisAddr[HisQulityStartAddr + Count] = (byte)QulityConst.Tick;
        }

        /// <summary>
        /// 写入头部信息
        /// </summary>
        public void UpdateHeader()
        {
            //数据块头部结构
            //数据区大小(int)+记录类型(byte)+变量类型(byte)+压缩类型(byte)+压缩参数1(float)+压缩参数2(float)+压缩参数3(float)
            var bids = BitConverter.GetBytes(this.HisQulityStartAddr - this.HisValueStartAddr);
            Buffer.BlockCopy(HisAddr, BlockHeadStartAddr, bids, 0, bids.Length);

            HisAddr[BlockHeadStartAddr + 4] = (byte)this.Type;
            HisAddr[BlockHeadStartAddr + 5] = (byte)this.TagType;
            HisAddr[BlockHeadStartAddr + 6] = (byte)this.CompressType;

            bids = BitConverter.GetBytes(CompressParameter1);
            Buffer.BlockCopy(HisAddr, BlockHeadStartAddr+7, bids, 0, bids.Length);

            bids = BitConverter.GetBytes(CompressParameter2);
            Buffer.BlockCopy(HisAddr, BlockHeadStartAddr + 11, bids, 0, bids.Length);
            bids = BitConverter.GetBytes(CompressParameter3);
            Buffer.BlockCopy(HisAddr, BlockHeadStartAddr + 15, bids, 0, bids.Length);
        }

        /// <summary>
        /// 在每个内存块的开始和结束部分插入值
        /// </summary>
        public void AppendValue()
        {
            //数据内容: 数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //更新数值
            Buffer.BlockCopy(RealMemoryAddr, RealValueAddr, HisAddr, HisValueStartAddr + Count * SizeOfValue, SizeOfValue);
            //更新质量戳
            //实时数据内存结构为:实时值+时间戳+质量戳

            //在现有质量戳基础上+100表示,该数值是强制添加到内存中的
            byte bval = (byte)(RealMemoryAddr[RealValueAddr + SizeOfValue + 8] + 100);

            HisAddr[HisQulityStartAddr + Count] = bval;

            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        /// <summary>
        /// 更新历史数据到缓存中
        /// </summary>
        public void UpdateValue(DateTime time)
        {
            //数据内容: 数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //更新数值
            Buffer.BlockCopy(RealMemoryAddr, RealValueAddr, HisAddr, HisValueStartAddr + Count * SizeOfValue, SizeOfValue);
            //更新质量戳
            //实时数据内存结构为:实时值+时间戳+质量戳
            Buffer.BlockCopy(RealMemoryAddr, RealValueAddr + SizeOfValue + 8, HisAddr, HisQulityStartAddr + Count, 1);

            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
