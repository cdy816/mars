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

        private byte[] headBytes;

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
        public static MemoryBlock HisAddr { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static DateTime StartTime { get; set; }

        /// <summary>
        /// 头部信息起始地址
        /// </summary>
        public int BlockHeadStartAddr { get; set; }

        /// <summary>
        /// 时间戳存访地址
        /// </summary>
        public int TimerValueStartAddr { get; set; }

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



        /// <summary>
        /// 
        /// </summary>
        public static int HeadSize
        {
            get
            {
                return 4 + 4;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        public void Init()
        {
            var hbyts = new List<byte>(8);
            hbyts.AddRange(BitConverter.GetBytes(this.HisQulityStartAddr - this.TimerValueStartAddr));
            //修改成参数从历史变量配置中获取
            hbyts.AddRange(BitConverter.GetBytes(this.Id));

            //hbyts.Add((byte)this.Type);
            //hbyts.Add((byte)this.TagType);
            //hbyts.Add((byte)this.CompressType);
            //hbyts.AddRange(BitConverter.GetBytes(CompressParameter1));
            //hbyts.AddRange(BitConverter.GetBytes(CompressParameter2));
            //hbyts.AddRange(BitConverter.GetBytes(CompressParameter3));
            headBytes = hbyts.ToArray();
        }

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
        /// <param name="block"></param>
        public void UpdateHeader(MemoryBlock block)
        {
            block.WriteBytes(BlockHeadStartAddr, headBytes);
        }

        /// <summary>
        /// 清空数值区
        /// </summary>
        /// <param name="block"></param>
        public void ClearDataValue(MemoryBlock memory)
        {
            long len = HisQulityStartAddr + MaxCount - TimerValueStartAddr;
            memory.Clear(TimerValueStartAddr, len);
        }

        ///// <summary>
        ///// 写入头部信息
        ///// </summary>
        //public void UpdateHeader()
        //{
        //    //数据块头部结构
        //    //数据区大小(int)+记录类型(byte)+变量类型(byte)+压缩类型(byte)+压缩参数1(float)+压缩参数2(float)+压缩参数3(float)
            
        //    //var bids = BitConverter.GetBytes(this.HisQulityStartAddr - this.HisValueStartAddr);
        //    //Buffer.BlockCopy(HisAddr, BlockHeadStartAddr, bids, 0, bids.Length);

        //    HisAddr.WriteBytes(BlockHeadStartAddr, headBytes);

        //    //HisAddr.WriteInt(BlockHeadStartAddr, this.HisQulityStartAddr - this.TimerValueStartAddr);

        //    ////写入记录类型
        //    //HisAddr.WriteByte(BlockHeadStartAddr+4, (byte)this.Type);
        //    ////写入变量类型
        //    //HisAddr.WriteByte(BlockHeadStartAddr+5, (byte)this.TagType);
        //    ////写入压缩类型
        //    //HisAddr.WriteByte(BlockHeadStartAddr+6, (byte)this.CompressType);
        //    ////写入压缩附属参数
        //    //HisAddr.WriteFloat(BlockHeadStartAddr + 7, CompressParameter1);
        //    //HisAddr.WriteFloat(BlockHeadStartAddr + 11, CompressParameter2);
        //    //HisAddr.WriteFloat(BlockHeadStartAddr + 15, CompressParameter3);
        //}

        /// <summary>
        /// 在每个内存块的开始和结束部分插入值
        /// </summary>
        public void AppendValue(int tim)
        {
            //数据内容:时间戳(time1+time2+...) + 数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳

            //写入时间戳

            HisAddr.WriteIntDirect(TimerValueStartAddr + Count * 2, tim);

            //写入数值
            HisAddr.WriteBytesDirect(HisValueStartAddr + Count * SizeOfValue, RealMemoryAddr, RealValueAddr, SizeOfValue);

            //更新质量戳
            //在现有质量戳基础上+100表示,该数值是强制添加到内存中的
            byte bval = (byte)(RealMemoryAddr[RealValueAddr + SizeOfValue + 8] + 100);

            HisAddr.WriteByteDirect(HisQulityStartAddr + Count, bval);


            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        /// <summary>
        /// 更新历史数据到缓存中
        /// </summary>
        public void UpdateValue(int tim)
        {
            //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳
            
            //Buffer.BlockCopy(RealMemoryAddr, RealValueAddr, HisAddr, HisValueStartAddr + Count * SizeOfValue, SizeOfValue);

            //写入时间戳
            //int tim = (int)((time - StartTime).TotalMilliseconds / HisEnginer.MemoryTimeTick);

            HisAddr.WriteIntDirect(TimerValueStartAddr + Count * 2, tim);

            //写入数值
            HisAddr.WriteBytesDirect(HisValueStartAddr + Count * SizeOfValue, RealMemoryAddr, RealValueAddr, SizeOfValue);

            //更新质量戳
            HisAddr.WriteByteDirect(HisQulityStartAddr + Count, RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);
           // HisAddr[HisQulityStartAddr + Count] = RealMemoryAddr[RealValueAddr + SizeOfValue + 8];

           // Buffer.BlockCopy(RealMemoryAddr, RealValueAddr + SizeOfValue + 8, HisAddr, HisQulityStartAddr + Count, 1);

            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
