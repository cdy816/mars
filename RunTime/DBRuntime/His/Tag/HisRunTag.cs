//==============================================================
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
using System.Runtime.CompilerServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe abstract class HisRunTag : HisTag,IDisposable
    {

        #region ... Variables  ...

        //private byte[] headBytes;
        protected int mLastTime = -10;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public HisRunTag()
        {
            ValueSnape = GC.AllocateArray<byte>(SizeOfValue, true);
            LastValue= GC.AllocateArray<byte>(SizeOfValue, true);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        ///// <summary>
        ///// 实时值地址
        ///// </summary>
        //public byte[] RealMemoryAddr { get; set; }

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
        [Obsolete]
        public HisDataMemoryBlock HisValueMemoryStartAddr { get; set; }

        /// <summary>
        /// 历史值缓存1
        /// </summary>
        [Obsolete]
        public HisDataMemoryBlock HisValueMemory1 { get; set; }

        /// <summary>
        /// 历史值缓存2
        /// </summary>
        [Obsolete]
        public HisDataMemoryBlock HisValueMemory2 { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public static HisDataMemoryBlockCollection3 CurrentDataMemory { get; set; }
        
        /// <summary>
        /// 
        /// </summary>
        public long DataMemoryPointer1 { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public long DataMemoryPointer2 { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public long CurrentMemoryPointer { get; set; }

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
        /// 
        /// </summary>
        public DateTime SnapeTime { get; set; }

        /// <summary>
        /// 最后记录的质量
        /// </summary>
        public byte LastQuality { get; set; } = (byte)QualityConst.Null;

        /// <summary>
        /// 最后记录的值
        /// </summary>
        public byte[] LastValue { get; set; }

        /// <summary>
        /// 最后记录的时间
        /// </summary>
        public DateTime LastTime { get; set; }

        /// <summary>
        /// 数据精度
        /// </summary>
        public int Precision { get; set; }

        /// <summary>
        /// 最后更新时间
        /// </summary>
        public DateTime LastUpdateTime { get; set; }

        /// <summary>
        /// 区域
        /// </summary>
        public string Area { get; set; }

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
        public void Snape(DateTime time)
        {
            SnapeTime = time;
            System.Runtime.InteropServices.Marshal.Copy(RealMemoryPtr + RealValueAddr, ValueSnape, 0, SizeOfValue);
            // QulitySnape = RealMemoryAddr[RealValueAddr + SizeOfValue + 8];
            QulitySnape = MemoryHelper.ReadByte((void*)(RealMemoryPtr), RealValueAddr + SizeOfValue + 8);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="tim"></param>
        //[Obsolete]
        //public void UpdateValue(int count,int tim)
        //{
        //    var vcount = count > MaxCount ? MaxCount : count;
        //    Count = vcount;

        //    //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
        //    //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
        //    HisAddr.WriteUShortDirect(TimerValueStartAddr + vcount * 2, (ushort)(tim));

        //    //写入数值
        //    HisAddr.WriteBytesDirect(HisValueStartAddr + vcount * SizeOfValue, RealMemoryPtr, RealValueAddr, SizeOfValue);

        //    //更新质量戳
        //    // HisAddr.WriteByteDirect(HisQulityStartAddr + vcount, RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);
        //    HisAddr.WriteByteDirect(HisQulityStartAddr + vcount, MemoryHelper.ReadByte((void*)(RealMemoryPtr), RealValueAddr + SizeOfValue + 8));
        //}

        ///// <summary>
        ///// 更新历史数据到缓存中
        ///// </summary>
        //[Obsolete]
        //public void UpdateValue(int tim)
        //{
        //    UpdateValue(Count, tim);
        //    Count = ++Count > MaxCount ? MaxCount : Count;
        //}

        /// <summary>
        /// 检查值是否发生改变
        /// </summary>
        /// <param name="startMemory"></param>
        /// <param name="offset"></param>
        /// <param name="time"></param>
        /// <returns></returns>
       
        public virtual bool CheckValueChangeToLastRecordValue(void* startMemory,long offset,int time)
        {
            return true;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="count"></param>
        ///// <param name="tim"></param>
        //[Obsolete]
        //public virtual void UpdateValue2(int count, int tim)
        //{
        //    var vcount = count > MaxCount ? MaxCount : count;
        //    Count = vcount;

        //    //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
        //    //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
        //    HisValueMemoryStartAddr.WriteUShortDirect((int)(HisValueMemoryStartAddr.TimerAddress + vcount * 2), (ushort)(tim));

        //    //写入数值
        //    HisValueMemoryStartAddr.WriteBytesDirect((int)(HisValueMemoryStartAddr.ValueAddress + vcount * SizeOfValue), RealMemoryPtr, RealValueAddr, SizeOfValue);
                       
        //    //更新质量戳
        //    //HisValueMemoryStartAddr.WriteByteDirect((int)(HisValueMemoryStartAddr.QualityAddress + vcount), RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);
        //    HisValueMemoryStartAddr.WriteByteDirect((int)(HisValueMemoryStartAddr.QualityAddress + vcount), MemoryHelper.ReadByte((void*)(RealMemoryPtr), RealValueAddr + SizeOfValue + 8));
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="tim"></param>
        //[Obsolete]
        //public void UpdateValue2(int tim)
        //{
        //    UpdateValue2(Count, tim);
        //    Count = ++Count > MaxCount ? MaxCount : Count;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="tim"></param>
        //[Obsolete]
        //public void UpdateChangedValue(int tim)
        //{
        //    if (CheckValueChangeToLastRecordValue((void*)RealMemoryPtr , RealValueAddr))
        //    {
        //        UpdateValue2(tim);
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <param name="tim"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void UpdateValue3(int count, int tim)
        {
            var vcount = count > MaxCount ? MaxCount : count;
            Count = vcount;

            //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
            CurrentDataMemory.WriteUShortDirect(CurrentMemoryPointer,(int)(TimerValueStartAddr + vcount * 2), (ushort)(tim));

            //写入数值
            CurrentDataMemory.WriteBytesDirect(CurrentMemoryPointer, (int)(HisValueStartAddr + vcount * SizeOfValue), RealMemoryPtr, RealValueAddr, SizeOfValue);

            //更新质量戳
            CurrentDataMemory.WriteByteDirect(CurrentMemoryPointer, (int)(HisQulityStartAddr + vcount), MemoryHelper.ReadByte((void*)(RealMemoryPtr), RealValueAddr + SizeOfValue + 8));
            // CurrentDataMemory.WriteByteDirect(CurrentMemoryPointer, (int)(HisQulityStartAddr + vcount), RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);

            //记录到最后的时间
            System.Runtime.InteropServices.Marshal.Copy(RealMemoryPtr + RealValueAddr, LastValue, 0, SizeOfValue);
            LastQuality = MemoryHelper.ReadByte((void*)(RealMemoryPtr), RealValueAddr + SizeOfValue + 8);
            LastTime = HisRunTag.StartTime.AddMilliseconds(tim * HisEnginer3.MemoryTimeTick);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        /// <param name="tim"></param>
        /// <param name="log"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public virtual void UpdateValue3(int count, int tim,LogRegion log)
        {
            var vcount = count > MaxCount ? MaxCount : count;
            Count = vcount;

            System.Runtime.InteropServices.Marshal.Copy(RealMemoryPtr + RealValueAddr, LastValue, 0, SizeOfValue);

            //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
            CurrentDataMemory.WriteUShortDirect(CurrentMemoryPointer, (int)(TimerValueStartAddr + vcount * 2), (ushort)(tim));

            //写入数值
            // CurrentDataMemory.WriteBytesDirect(CurrentMemoryPointer, (int)(HisValueStartAddr + vcount * SizeOfValue), RealMemoryPtr, RealValueAddr, SizeOfValue);
            CurrentDataMemory.WriteBytesDirect(CurrentMemoryPointer, (int)(HisValueStartAddr + vcount * SizeOfValue), LastValue, 0, SizeOfValue);

            //更新质量戳
            CurrentDataMemory.WriteByteDirect(CurrentMemoryPointer, (int)(HisQulityStartAddr + vcount), MemoryHelper.ReadByte((void*)(RealMemoryPtr), RealValueAddr + SizeOfValue + 8));
            // CurrentDataMemory.WriteByteDirect(CurrentMemoryPointer, (int)(HisQulityStartAddr + vcount), RealMemoryAddr[RealValueAddr + SizeOfValue + 8]);

            //记录到最后的时间
            //System.Runtime.InteropServices.Marshal.Copy(RealMemoryPtr + RealValueAddr, LastValue, 0, SizeOfValue);
            LastQuality = MemoryHelper.ReadByte((void*)(RealMemoryPtr), RealValueAddr + SizeOfValue + 8);
            LastTime = HisRunTag.StartTime.AddMilliseconds(tim * HisEnginer3.MemoryTimeTick);

            //if(Id==0)
            //{
            //    LoggerService.Service.Info("HisRunTag",$" **********************************tag0 value :{BitConverter.ToDouble(LastValue)} time:{tim} count: {count}**********************************");
            //}

            //记录到数据队列中
            log?.Append(TagType, Id, LastTime, LastQuality, LastValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="tim"></param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateValue(HisRunTag tag,int tim)
        {
            var vcount = tag.Count > tag.MaxCount ? tag.MaxCount : tag.Count;
            tag.Count = vcount;

            //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
            CurrentDataMemory.WriteUShortDirect(tag.CurrentMemoryPointer, (int)(tag.TimerValueStartAddr + vcount * 2), (ushort)(tim));

            //写入数值
            CurrentDataMemory.WriteBytesDirect(tag.CurrentMemoryPointer, (int)(tag.HisValueStartAddr + vcount * tag.SizeOfValue), tag.RealMemoryPtr, tag.RealValueAddr, tag.SizeOfValue);

            //更新质量戳
            CurrentDataMemory.WriteByteDirect(tag.CurrentMemoryPointer, (int)(tag.HisQulityStartAddr + vcount), MemoryHelper.ReadByte((void*)(tag.RealMemoryPtr), tag.RealValueAddr + tag.SizeOfValue + 8));

            tag.Count = ++tag.Count > tag.MaxCount ? tag.MaxCount : tag.Count;

            //记录到最后的时间
            System.Runtime.InteropServices.Marshal.Copy(tag.RealMemoryPtr + tag.RealValueAddr, tag.LastValue, 0, tag.SizeOfValue);
            tag.LastQuality = MemoryHelper.ReadByte((void*)(tag.RealMemoryPtr), tag.RealValueAddr + tag.SizeOfValue + 8);
            tag.LastTime = HisRunTag.StartTime.AddMilliseconds(tim * HisEnginer3.MemoryTimeTick);

        }


        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static void UpdateValue(HisRunTag tag, int tim,LogRegion log)
        {
            var vcount = tag.Count > tag.MaxCount ? tag.MaxCount : tag.Count;
            tag.Count = vcount;

            //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
            CurrentDataMemory.WriteUShortDirect(tag.CurrentMemoryPointer, (int)(tag.TimerValueStartAddr + vcount * 2), (ushort)(tim));

            //写入数值
            CurrentDataMemory.WriteBytesDirect(tag.CurrentMemoryPointer, (int)(tag.HisValueStartAddr + vcount * tag.SizeOfValue), tag.RealMemoryPtr, tag.RealValueAddr, tag.SizeOfValue);

            //更新质量戳
            CurrentDataMemory.WriteByteDirect(tag.CurrentMemoryPointer, (int)(tag.HisQulityStartAddr + vcount), MemoryHelper.ReadByte((void*)(tag.RealMemoryPtr), tag.RealValueAddr + tag.SizeOfValue + 8));

            tag.Count = ++tag.Count > tag.MaxCount ? tag.MaxCount : tag.Count;

            //记录到最后的时间
            System.Runtime.InteropServices.Marshal.Copy(tag.RealMemoryPtr + tag.RealValueAddr, tag.LastValue, 0, tag.SizeOfValue);
            tag.LastQuality = MemoryHelper.ReadByte((void*)(tag.RealMemoryPtr), tag.RealValueAddr + tag.SizeOfValue + 8);
            tag.LastTime = HisRunTag.StartTime.AddMilliseconds(tim * HisEnginer3.MemoryTimeTick);

            log?.Append(tag.TagType, tag.Id, tag.LastTime, tag.LastQuality, tag.LastValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tim"></param>
        public void UpdateValue3(int tim)
        {
            UpdateValue3(Count, tim);
            Count = ++Count > MaxCount ? MaxCount : Count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tim"></param>
        public bool UpdateChangedValue3(int tim)
        {
            if (CheckValueChangeToLastRecordValue((void*)RealMemoryPtr, RealValueAddr,tim))
            {
                UpdateValue3(Count, tim);
                Count = ++Count > MaxCount ? MaxCount : Count;
                return true;
                //UpdateValue3(tim);
            }
            return false;
        }


        /// <summary>
        /// 记录改变的值
        /// </summary>
        /// <param name="tim"></param>
        /// <param name="log"></param>
        /// <returns></returns>
        public bool UpdateChangedValue3(int tim,LogRegion log)
        {
            if (CheckValueChangeToLastRecordValue((void*)RealMemoryPtr, RealValueAddr, tim))
            {
                UpdateValue3(Count, tim, log);
                Count = ++Count > MaxCount ? MaxCount : Count;
                return true;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="tim"></param>
        public static void UpdateChangedValue(HisRunTag tag, int tim)
        {
            var vcount = tag.Count > tag.MaxCount ? tag.MaxCount : tag.Count;
            tag.Count = vcount;

            //数据内容: 时间戳(time1+time2+...) +数值区(value1+value2+...)+质量戳区(q1+q2+....)
            //实时数据内存结构为:实时值+时间戳+质量戳，时间戳2个字节，质量戳1个字节
            CurrentDataMemory.WriteUShortDirect(tag.CurrentMemoryPointer, (int)(tag.TimerValueStartAddr + vcount * 2), (ushort)(tim));

            //写入数值
            CurrentDataMemory.WriteBytesDirect(tag.CurrentMemoryPointer, (int)(tag.HisValueStartAddr + vcount * tag.SizeOfValue), tag.RealMemoryPtr, tag.RealValueAddr, tag.SizeOfValue);

            //更新质量戳
            CurrentDataMemory.WriteByteDirect(tag.CurrentMemoryPointer, (int)(tag.HisQulityStartAddr + vcount), MemoryHelper.ReadByte((void*)(tag.RealMemoryPtr), tag.RealValueAddr + tag.SizeOfValue + 8));

            tag.Count = ++tag.Count > tag.MaxCount ? tag.MaxCount : tag.Count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool EqualsTo(HisTag tag)
        {
            return tag.Id == this.Id && this.Type == tag.Type && this.TagType == tag.TagType && this.CompressType == tag.CompressType && this.Circle == tag.Circle && this.MaxValueCountPerSecond == tag.MaxValueCountPerSecond && Compare(this.Parameters, tag.Parameters);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source1"></param>
        /// <param name="source2"></param>
        /// <returns></returns>
        private bool Compare(Dictionary<string, double> source1, Dictionary<string, double> source2)
        {
            if (source1 == null && source2 == null) return true;
            if ((source1 == null && source2 != null) || (source1 != null && source2 == null)) return false;
            foreach (var vv in source1)
            {
                if (!source2.ContainsKey(vv.Key)) return false;
                if (source2[vv.Key] != source1[vv.Key]) return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            HisValueMemory1 = null;
            HisValueMemory2 = null;
            HisValueMemoryStartAddr = null;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
