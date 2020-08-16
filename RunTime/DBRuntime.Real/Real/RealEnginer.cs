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
using System.Threading.Tasks;
using Cdy.Tag.Driver;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe class RealEnginer: IRealDataNotify, IRealDataNotifyForProducter, IRealData, IRealTagProduct, IRealTagConsumer,IDisposable
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private byte[] mMemory;

        /// <summary>
        /// 
        /// </summary>
        private RealDatabase mConfigDatabase =null;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, long> mIdAndAddr = new Dictionary<int, long>();

        /// <summary>
        /// 已经使用的内存
        /// </summary>
        private long mUsedSize = 0;


        /// <summary>
        /// 
        /// </summary>
        private void* mMHandle;

        private ManualResetEvent mLockEvent;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public RealEnginer()
        {
            mLockEvent = new ManualResetEvent(true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public RealEnginer(RealDatabase database):this()
        {
            this.mConfigDatabase = database;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public byte[] Memory
        {
            get
            {
                return mMemory;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public IntPtr MemoryHandle
        {
            get
            {
                return (IntPtr)mMHandle;
            }
        }
    

        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            long msize = 0;
            byte unknowQuality = (byte)QualityConst.Init;
            mIdAndAddr.Clear();
            foreach (var vv in mConfigDatabase.Tags)
            {
                vv.Value.ValueAddress = msize;
                mIdAndAddr.Add(vv.Value.Id, vv.Value.ValueAddress);
                switch (vv.Value.Type)
                {
                    case TagType.Bool:
                    case TagType.Byte:
                        msize +=10;
                        break;
                    case TagType.Short:
                    case TagType.UShort:
                        msize += 11;
                        break;
                    case TagType.Int:
                    case TagType.UInt:
                    case TagType.Float:
                        msize += 13;
                        break;
                    case TagType.Long:
                    case TagType.ULong:
                    case TagType.Double:
                        msize += 17;
                        break;
                    case TagType.IntPoint:
                    case TagType.UIntPoint:
                        msize += 17;
                        break;
                    case TagType.IntPoint3:
                    case TagType.UIntPoint3:
                        msize += 21;
                        break;
                    case TagType.LongPoint:
                    case TagType.ULongPoint:
                        msize += 25;
                        break;
                    case TagType.LongPoint3:
                    case TagType.ULongPoint3:
                        msize += 33;
                        break;
                    case TagType.String:
                        msize += (Const.StringSize + 9);
                        break;
                }
            }
            //留20%的余量
            mUsedSize = msize;
            var fsize = ((long)(msize * 1.5 / 1024) + 1) * 1024;
            mMemory = new byte[fsize];
            mMHandle = mMemory.AsMemory().Pin().Pointer;
            mMemory.AsSpan().Clear();

            LoggerService.Service.Info("RealEnginer","Cal memory size:"+ fsize / 1024.0/1024+"M",ConsoleColor.Cyan);

            foreach (var vv in mConfigDatabase.Tags)
            {
                switch (vv.Value.Type)
                {
                    case TagType.Bool:
                    case TagType.Byte:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + 9, unknowQuality);
                        break;
                    case TagType.Short:
                    case TagType.UShort:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + 10, unknowQuality);
                        break;
                    case TagType.Int:
                    case TagType.UInt:
                    case TagType.Float:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + 12, unknowQuality);
                        break;
                    case TagType.Long:
                    case TagType.ULong:
                    case TagType.Double:
                    case TagType.IntPoint:
                    case TagType.UIntPoint:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + 16, unknowQuality);
                        break;
                    case TagType.IntPoint3:
                    case TagType.UIntPoint3:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + 20, unknowQuality);
                        break;
                    case TagType.LongPoint:
                    case TagType.ULongPoint:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + 24, unknowQuality);
                        break;
                    case TagType.LongPoint3:
                    case TagType.ULongPoint3:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + 32, unknowQuality);
                        break;
                    case TagType.String:
                        MemoryHelper.WriteByte(mMHandle, vv.Value.ValueAddress + Const.StringSize + 8, unknowQuality);
                        break;
                }
            }

        }

        /// <summary>
        /// 加载使能新的变量
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="mNewDb"></param>
        public void ReLoadTags(IEnumerable<Tag.Tagbase> tags,RealDatabase mNewDb)
        {
            long msize = 0;
            foreach (var vv in tags)
            {
                vv.ValueAddress = mUsedSize + msize;
                mIdAndAddr.Add(vv.Id, vv.ValueAddress);
                switch (vv.Type)
                {
                    case TagType.Bool:
                    case TagType.Byte:
                        msize += 10;
                        break;
                    case TagType.Short:
                    case TagType.UShort:
                        msize += 11;
                        break;
                    case TagType.Int:
                    case TagType.UInt:
                    case TagType.Float:
                        msize += 13;
                        break;
                    case TagType.Long:
                    case TagType.ULong:
                    case TagType.Double:
                        msize += 17;
                        break;
                    case TagType.IntPoint:
                    case TagType.UIntPoint:
                        msize += 17;
                        break;
                    case TagType.IntPoint3:
                    case TagType.UIntPoint3:
                        msize += 21;
                        break;
                    case TagType.LongPoint:
                    case TagType.ULongPoint:
                        msize += 25;
                        break;
                    case TagType.LongPoint3:
                    case TagType.ULongPoint3:
                        msize += 33;
                        break;
                    case TagType.String:
                        msize += (Const.StringSize + 9);
                        break;
                }
            }
            var fsize = mUsedSize + msize;
            if ((mUsedSize + msize) > mMemory.Length)
            {
                var men = new byte[fsize];
                var hmen = mMemory.AsMemory().Pin().Pointer;
                men.AsSpan().Clear();
                Array.Copy(mMemory, men,mMemory.Length);

                mMemory = men;
                mMHandle = hmen;
            }
            mUsedSize = fsize;

            mConfigDatabase = mNewDb;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Lock()
        {
            mLockEvent.Reset();
        }

        /// <summary>
        /// 
        /// </summary>
        public void UnLock()
        {
            mLockEvent.Set();
        }

        /// <summary>
        /// 
        /// </summary>
        private void Take()
        {
            mLockEvent.WaitOne();
        }

        /// <summary>
        /// 
        /// </summary>
        public void SubmiteNotifyChanged()
        {
            ComsumerValueChangedNotifyManager.Manager.NotifyChanged();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void NotifyValueChangedToProducter(int id,object value)
        {
            ProducterValueChangedNotifyManager.Manager.UpdateValue(id,value);
            ProducterValueChangedNotifyManager.Manager.NotifyChanged();
        }


        public void NotifyValueChangedToProducter(int id)
        {
            var value = GetTagValueForProductor(id);
            ProducterValueChangedNotifyManager.Manager.UpdateValue(id, value);
            ProducterValueChangedNotifyManager.Manager.NotifyChanged();
        }

        #region 通过地址写入数据
        /// <summary>
        /// 通过内存地址设置Byte值
        /// </summary>
        /// <param name="addr">地址</param>
        /// <param name="value">值</param>
        public void SetValueByAddr(long addr, byte value)
        {
            mMemory[addr] = value;
            MemoryHelper.WriteByte(mMHandle, addr + 9, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, byte value,byte quality,DateTime time)
        {
            mMemory[addr] = value;
            MemoryHelper.WriteDateTime(mMHandle, addr + 1,time);
            MemoryHelper.WriteByte(mMHandle, addr + 9, quality);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, short value)
        {
            MemoryHelper.WriteShort(mMHandle, addr, value);
            MemoryHelper.WriteByte(mMHandle, addr + 10, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, short value, byte quality, DateTime time)
        {
            MemoryHelper.WriteShort(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr +2, time);
            MemoryHelper.WriteByte(mMHandle, addr + 10, quality);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, int value)
        {
            MemoryHelper.WriteInt32(mMHandle, addr, value);
            MemoryHelper.WriteByte(mMHandle, addr + 12, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, int value, byte quality, DateTime time)
        {
            MemoryHelper.WriteInt32(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 4, time);
            MemoryHelper.WriteByte(mMHandle, addr + 12, quality); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, long value)
        {
            MemoryHelper.WriteInt64(mMHandle, addr, value);
            MemoryHelper.WriteByte(mMHandle, addr + 16, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, long value, byte quality, DateTime time)
        {
            MemoryHelper.WriteInt64(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality);
        }

 

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, float value)
        {
            MemoryHelper.WriteFloat(mMHandle, addr, value);
            MemoryHelper.WriteByte(mMHandle, addr + 12, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, float value, byte quality, DateTime time)
        {
            MemoryHelper.WriteFloat(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 4, time);
            MemoryHelper.WriteByte(mMHandle, addr + 12, quality); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, double value)
        {
            MemoryHelper.WriteDouble(mMHandle, addr, value);
            MemoryHelper.WriteByte(mMHandle, addr + 16, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, double value, byte quality, DateTime time)
        {
            //LoggerService.Service.Warn("RealEnginer", " write value:"+ value.ToString() + "");
            MemoryHelper.WriteDouble(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, DateTime value)
        {
            MemoryHelper.WriteDateTime(mMHandle, addr, value);
            MemoryHelper.WriteByte(mMHandle, addr + 16, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, DateTime value, byte quality, DateTime time)
        {
            MemoryHelper.WriteDateTime(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, string value)
        {
            //字符串存储内容：长度+内容
            MemoryHelper.WriteByte(mMHandle, addr, (byte)value.Length);
            
            System.Buffer.BlockCopy(value.ToCharArray(), 0, mMemory, (int)addr+1, value.Length);
            MemoryHelper.WriteByte(mMHandle, Const.StringSize + 8, 0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, string value, byte quality, DateTime time)
        {
            System.Buffer.BlockCopy(value.ToCharArray(), 0, mMemory, (int)addr, value.Length);
            MemoryHelper.WriteDateTime(mMHandle, Const.StringSize, time);
            MemoryHelper.WriteByte(mMHandle, Const.StringSize + 8, quality); 
        }

        #region

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, int value1,int value2, byte quality, DateTime time)
        {
            MemoryHelper.WriteInt32(mMHandle, addr, value1);
            MemoryHelper.WriteInt32(mMHandle, addr+4, value2);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, uint value1, uint value2, byte quality, DateTime time)
        {
            MemoryHelper.WriteUInt32(mMHandle, addr, value1);
            MemoryHelper.WriteUInt32(mMHandle, addr + 4, value2);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality); 
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="value3"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, int value1, int value2,int value3, byte quality, DateTime time)
        {
            MemoryHelper.WriteInt32(mMHandle, addr, value1);
            MemoryHelper.WriteInt32(mMHandle, addr + 4, value2);
            MemoryHelper.WriteInt32(mMHandle, addr + 8, value3);
            MemoryHelper.WriteDateTime(mMHandle, addr + 12, time);
            MemoryHelper.WriteByte(mMHandle, addr + 20, quality); 
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="value3"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, uint value1, uint value2, uint value3, byte quality, DateTime time)
        {
            MemoryHelper.WriteUInt32(mMHandle, addr, value1);
            MemoryHelper.WriteUInt32(mMHandle, addr + 4, value2);
            MemoryHelper.WriteUInt32(mMHandle, addr + 8, value3);
            MemoryHelper.WriteDateTime(mMHandle, addr + 12, time);
            MemoryHelper.WriteByte(mMHandle, addr + 20, quality); 
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, ulong value1, ulong value2, byte quality, DateTime time)
        {
            MemoryHelper.WriteUInt64(mMHandle, addr, value1);
            MemoryHelper.WriteUInt64(mMHandle, addr + 8, value2);
            MemoryHelper.WriteDateTime(mMHandle, addr + 16, time);
            MemoryHelper.WriteByte(mMHandle, addr + 24, quality); 
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, long value1, long value2,  byte quality, DateTime time)
        {
            MemoryHelper.WriteInt64(mMHandle, addr, value1);
            MemoryHelper.WriteInt64(mMHandle, addr + 8, value2);
            MemoryHelper.WriteDateTime(mMHandle, addr + 16, time);
            MemoryHelper.WriteByte(mMHandle, addr + 24, quality); 
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="value3"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, long value1, long value2, long value3, byte quality, DateTime time)
        {
            MemoryHelper.WriteInt64(mMHandle, addr, value1);
            MemoryHelper.WriteInt64(mMHandle, addr + 8, value2);
            MemoryHelper.WriteInt64(mMHandle, addr + 16, value3);
            MemoryHelper.WriteDateTime(mMHandle, addr + 24, time);
            MemoryHelper.WriteByte(mMHandle, addr + 32, quality); 
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <param name="value3"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetPointValueByAddr(long addr, ulong value1, ulong value2, ulong value3, byte quality, DateTime time)
        {
            MemoryHelper.WriteUInt64(mMHandle, addr, value1);
            MemoryHelper.WriteUInt64(mMHandle, addr + 8, value2);
            MemoryHelper.WriteUInt64(mMHandle, addr + 16, value3);
            MemoryHelper.WriteDateTime(mMHandle, addr + 24, time);
            MemoryHelper.WriteByte(mMHandle, addr + 32, quality); 
        }
        #endregion

        #region SetValue By Id
        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, bool value)
        {
            DateTime time = DateTime.Now;
            if (value)
            {
                SetValue(id, (byte)1, 0, time);
            }
            else
            {
                SetValue(id, (byte)0, 0, time);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, byte value)
        {
            DateTime time = DateTime.Now;
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int,byte> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach(var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<byte,byte,DateTime>> values)
        {
            //Parallel.ForEach(values,(vv) => {
                foreach (var vv in values)
                    if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, byte value,byte quality,DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,quality,time);
                NotifyValueChangedToConsumer(id);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, short value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,DateTime.Now);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, short> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, short value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<short, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, ushort value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,DateTime.Now);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, ushort> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, ushort value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<ushort, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, int value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,DateTime.Now);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, int> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, int value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<int, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, uint value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,DateTime.Now);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, uint> values)
        {
            //Parallel.ForEach(values, (vv) => {
            DateTime time = DateTime.Now;
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, uint value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<uint, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, long value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,DateTime.Now);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, long> values)
        {
            //Parallel.ForEach(values, (vv) => {
            DateTime time = DateTime.Now;
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, long value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<long, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, ulong value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,DateTime.Now);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, ulong> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, ulong value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<ulong, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, IntPointData value,DateTime time,byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X,value.Y,quality,time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, UIntPointData value, DateTime time, byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X, value.Y, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, IntPoint3Data value, DateTime time, byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X, value.Y, value.Z, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, UIntPoint3Data value, DateTime time, byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X, value.Y, value.Z, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, ULongPoint3Data value, DateTime time, byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X, value.Y, value.Z, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, LongPoint3Data value, DateTime time, byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X, value.Y, value.Z, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, LongPointData value, DateTime time, byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X, value.Y, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        public void SetValue(int id, ULongPointData value, DateTime time, byte quality)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetPointValueByAddr(mIdAndAddr[id], value.X, value.Y, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, float value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChangedToConsumer(id);
            }
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, float> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, float value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<float, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, double value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChangedToConsumer(id);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, double> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, double value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<double, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, DateTime value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,0,DateTime.Now);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, DateTime> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, DateTime value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<DateTime, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, string value)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(int id, byte quality, DateTime time, params object[] values)
        {
            if (mIdAndAddr.ContainsKey(id) && mConfigDatabase.Tags.ContainsKey(id))
            {
                SetPointValue(mConfigDatabase.Tags[id], quality, time, values);
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(Tagbase tag, byte quality, DateTime time, params object[] values)
        {
            if (tag.ReadWriteType == ReadWriteMode.Write) return true;
            switch (tag.Type)
            {
                case TagType.IntPoint:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToInt32(values[0]), Convert.ToInt32(values[1]), quality, time);
                    break;
                case TagType.UIntPoint:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToUInt32(values[0]), Convert.ToUInt32(values[1]), quality, time);
                    break;
                case TagType.IntPoint3:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToInt32(values[0]), Convert.ToInt32(values[1]), Convert.ToInt32(values[2]), quality, time);
                    break;
                case TagType.UIntPoint3:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToUInt32(values[0]), Convert.ToUInt32(values[1]), Convert.ToUInt32(values[2]), quality, time);
                    break;
                case TagType.LongPoint:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToInt64(values[0]), Convert.ToInt64(values[1]), quality, time);
                    break;
                case TagType.ULongPoint:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToUInt64(values[0]), Convert.ToUInt64(values[1]), quality, time);
                    break;
                case TagType.LongPoint3:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToInt64(values[0]), Convert.ToInt64(values[1]), Convert.ToInt64(values[2]), quality, time);
                    break;
                case TagType.ULongPoint3:
                    SetPointValueByAddr(mIdAndAddr[tag.Id], Convert.ToUInt64(values[0]), Convert.ToUInt64(values[1]), Convert.ToUInt64(values[2]), quality, time);
                    break;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(int id, byte quality,  params object[] values)
        {
            DateTime time = DateTime.Now;
            return SetPointValue(id, (byte)QualityConst.Good,time, values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="quality"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(Tagbase tag, byte quality, params object[] values)
        {
            DateTime time = DateTime.Now;
            return SetPointValue(tag, (byte)QualityConst.Good, time, values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(int id,  params object[] values)
        {
            return SetPointValue(id, (byte)QualityConst.Good, values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, string> values)
        {
            DateTime time = DateTime.Now;
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value,0,time);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        public void SetValue(int id, string value, byte quality, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, quality, time);
                NotifyValueChangedToConsumer(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<string, byte, DateTime>> values)
        {
            //Parallel.ForEach(values, (vv) => {
            foreach (var vv in values)
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            //});
            NotifyValueChangedToConsumer(values.Keys.ToList());
        }
        #endregion

        #endregion

        #region 数据读取

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public string ReadStringValueByAddr(long addr, Encoding encoding)
        {
            int len = MemoryHelper.ReadByte((sbyte*)mMHandle, addr);
            return new string((sbyte*)mMHandle, (int)addr+1, len, encoding);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="encoding"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public string ReadStringValueByAddr(long addr, Encoding encoding,out DateTime time, out byte quality)
        {
            int len = MemoryHelper.ReadByte((sbyte*)mMHandle, addr);
            var re = new string((sbyte*)mMHandle, (int)addr+1, len, encoding);
            time = MemoryHelper.ReadDateTime(mMHandle, addr+ Const.StringSize);
            quality = MemoryHelper.ReadByte(mMHandle, addr + Const.StringSize + 8);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public byte ReadByteValueByAddr(long addr)
        {
            return MemoryHelper.ReadByte(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public byte ReadByteValueByAddr(long addr, out DateTime time, out byte quality)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 1);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 9);
            return MemoryHelper.ReadByte(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public int ReadIntValueByAddr(long addr)
        {
            return MemoryHelper.ReadInt32(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public int ReadIntValueByAddr(long addr, out DateTime time, out byte quality)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 4);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 12);
            return MemoryHelper.ReadInt32(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public short ReadShortValueByAddr(long addr)
        {
            return MemoryHelper.ReadShort(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public short ReadShortValueByAddr(long addr, out DateTime time, out byte quality)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 2);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 10);
            return MemoryHelper.ReadShort(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public long ReadInt64ValueByAddr(long addr)
        {
            return MemoryHelper.ReadInt64(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public long ReadInt64ValueByAddr(long addr, out DateTime time, out byte quality)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 16);
            return MemoryHelper.ReadInt64(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public double ReadDoubleValueByAddr(long addr)
        {
            return MemoryHelper.ReadDouble(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public double ReadDoubleValueByAddr(long addr, out DateTime time, out byte quality)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 16);
            return MemoryHelper.ReadDouble(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public float ReadFloatValueByAddr(long addr)
        {
            return MemoryHelper.ReadFloat(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public float ReadFloatValueByAddr(long addr, out DateTime time, out byte quality)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 4);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 12);
            return MemoryHelper.ReadFloat(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public DateTime ReadDateTimeValueByAddr(long addr)
        {
            return MemoryHelper.ReadDateTime(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public DateTime ReadDateTimeValueByAddr(long addr, out DateTime time, out byte quality)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 16);
            return MemoryHelper.ReadDateTime(mMHandle, addr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public IntPointData ReadIntPointValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 16);
            return new IntPointData(MemoryHelper.ReadInt32(mMHandle, addr), MemoryHelper.ReadInt32(mMHandle, addr + 4));
        }

        public IntPointData ReadIntPointValueByAddr(long addr)
        {
            return new IntPointData(MemoryHelper.ReadInt32(mMHandle, addr), MemoryHelper.ReadInt32(mMHandle, addr + 4));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        public void ReadIntPointValueByAddr(long addr, out byte quality, out DateTime time, out int x, out int y)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 16);
            x = MemoryHelper.ReadInt32(mMHandle, addr);
            y = MemoryHelper.ReadInt32(mMHandle, addr + 4);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public UIntPointData ReadUIntPointValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 16);
           return  new UIntPointData(MemoryHelper.ReadUInt32(mMHandle, addr), MemoryHelper.ReadUInt32(mMHandle, addr + 4));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public UIntPointData ReadUIntPointValueByAddr(long addr)
        {
            return new UIntPointData(MemoryHelper.ReadUInt32(mMHandle, addr), MemoryHelper.ReadUInt32(mMHandle, addr + 4));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        public void ReadUIntPointValueByAddr(long addr, out byte quality, out DateTime time, out uint x, out uint y)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 16);
            x = MemoryHelper.ReadUInt32(mMHandle, addr);
            y = MemoryHelper.ReadUInt32(mMHandle, addr + 4);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public IntPoint3Data ReadIntPoint3ValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 12);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 20);
           return  new IntPoint3Data(MemoryHelper.ReadInt32(mMHandle, addr), MemoryHelper.ReadInt32(mMHandle, addr + 4), MemoryHelper.ReadInt32(mMHandle, addr + 8));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public IntPoint3Data ReadIntPoint3ValueByAddr(long addr)
        {
            return new IntPoint3Data(MemoryHelper.ReadInt32(mMHandle, addr), MemoryHelper.ReadInt32(mMHandle, addr + 4), MemoryHelper.ReadInt32(mMHandle, addr + 8));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        public void ReadIntPoint3ValueByAddr(long addr, out byte quality, out DateTime time, out int x, out int y, out int z)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 12);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 20);
            x = MemoryHelper.ReadInt32(mMHandle, addr);
            y = MemoryHelper.ReadInt32(mMHandle, addr + 4);
            z = MemoryHelper.ReadInt32(mMHandle, addr + 8);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public UIntPoint3Data ReadUIntPoint3ValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 12);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 20);
            return new UIntPoint3Data(MemoryHelper.ReadUInt32(mMHandle, addr), MemoryHelper.ReadUInt32(mMHandle, addr + 4), MemoryHelper.ReadUInt32(mMHandle, addr + 8));
        }

        public UIntPoint3Data ReadUIntPoint3ValueByAddr(long addr)
        {
            return new UIntPoint3Data(MemoryHelper.ReadUInt32(mMHandle, addr), MemoryHelper.ReadUInt32(mMHandle, addr + 4), MemoryHelper.ReadUInt32(mMHandle, addr + 8));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        public void ReadUIntPoint3ValueByAddr(long addr, out byte quality, out DateTime time, out uint x, out uint y, out uint z)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 12);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 20);
            x = MemoryHelper.ReadUInt32(mMHandle, addr);
            y = MemoryHelper.ReadUInt32(mMHandle, addr + 4);
            z = MemoryHelper.ReadUInt32(mMHandle, addr + 8);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public LongPointData ReadLongPointValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 16);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 24);
            return new LongPointData(MemoryHelper.ReadInt64(mMHandle, addr), MemoryHelper.ReadInt64(mMHandle, addr + 8));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public LongPointData ReadLongPointValueByAddr(long addr)
        {
            return new LongPointData(MemoryHelper.ReadInt64(mMHandle, addr), MemoryHelper.ReadInt64(mMHandle, addr + 8));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        public void ReadLongPointValueByAddr(long addr, out byte quality, out DateTime time, out long x, out long y)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 16);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 24);
            x = MemoryHelper.ReadInt64(mMHandle, addr);
            y = MemoryHelper.ReadInt64(mMHandle, addr + 8);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public ULongPointData ReadULongPointValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 16);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 24);
            return new ULongPointData(MemoryHelper.ReadUInt64(mMHandle, addr), MemoryHelper.ReadUInt64(mMHandle, addr + 8));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public ULongPointData ReadULongPointValueByAddr(long addr)
        {
            return new ULongPointData(MemoryHelper.ReadUInt64(mMHandle, addr), MemoryHelper.ReadUInt64(mMHandle, addr + 8));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        public void ReadULongPointValueByAddr(long addr, out byte quality, out DateTime time, out ulong x, out ulong y)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 16);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 24);
            x = MemoryHelper.ReadUInt64(mMHandle, addr);
            y = MemoryHelper.ReadUInt64(mMHandle, addr + 8);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public LongPoint3Data ReadLongPoint3ValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 24);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 32);
            return new LongPoint3Data(MemoryHelper.ReadInt64(mMHandle, addr), MemoryHelper.ReadInt64(mMHandle, addr + 8), MemoryHelper.ReadInt64(mMHandle, addr + 16));
        }


        public LongPoint3Data ReadLongPoint3ValueByAddr(long addr)
        {
            return new LongPoint3Data(MemoryHelper.ReadInt64(mMHandle, addr), MemoryHelper.ReadInt64(mMHandle, addr + 8), MemoryHelper.ReadInt64(mMHandle, addr + 16));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        public void ReadLongPoint3ValueByAddr(long addr, out byte quality, out DateTime time, out long x, out long y, out long z)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 24);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 32);
            x = MemoryHelper.ReadInt64(mMHandle, addr);
            y = MemoryHelper.ReadInt64(mMHandle, addr + 8);
            z = MemoryHelper.ReadInt64(mMHandle, addr + 16);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public ULongPoint3Data ReadULongPoint3ValueByAddr(long addr, out byte quality, out DateTime time)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 24);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 32);
            return new ULongPoint3Data(MemoryHelper.ReadUInt64(mMHandle, addr), MemoryHelper.ReadUInt64(mMHandle, addr + 8), MemoryHelper.ReadUInt64(mMHandle, addr + 16));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <returns></returns>
        public ULongPoint3Data ReadULongPoint3ValueByAddr(long addr)
        {
            return new ULongPoint3Data(MemoryHelper.ReadUInt64(mMHandle, addr), MemoryHelper.ReadUInt64(mMHandle, addr + 8), MemoryHelper.ReadUInt64(mMHandle, addr + 16));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        public void ReadULongPoint3ValueByAddr(long addr, out byte quality, out DateTime time,out ulong x,out ulong y,out ulong z)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 24);
            quality = MemoryHelper.ReadByte(mMHandle, addr + 32);
            x = MemoryHelper.ReadUInt64(mMHandle, addr);
            y = MemoryHelper.ReadUInt64(mMHandle, addr + 8);
            z= MemoryHelper.ReadUInt64(mMHandle, addr + 16);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public byte? ReadByteValue(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadByteValueByAddr(mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public byte? ReadByteValue(int id,out byte quality,out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadByteValueByAddr(mIdAndAddr[id],out time,out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public short? ReadShortValue(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadShortValueByAddr(mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public short? ReadShortValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadShortValueByAddr(mIdAndAddr[id], out time, out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public int? ReadIntValue(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadIntValueByAddr(mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public int? ReadIntValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadIntValueByAddr(mIdAndAddr[id], out time, out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public long? ReadInt64Value(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadInt64ValueByAddr(mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public long? ReadInt64Value(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadInt64ValueByAddr(mIdAndAddr[id], out time, out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public double? ReadDoubleValue(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadDoubleValueByAddr(mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public double? ReadDoubleValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadDoubleValueByAddr(mIdAndAddr[id], out time, out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public float? ReadFloatValue(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadFloatValueByAddr(mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public float? ReadFloatValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadFloatValueByAddr(mIdAndAddr[id], out time, out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public DateTime? ReadDatetimeValue(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadDateTimeValueByAddr(mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public DateTime? ReadDatetimeValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadDateTimeValueByAddr(mIdAndAddr[id], out time, out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public string ReadStringValue(int id,Encoding encoding)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadStringValueByAddr(mIdAndAddr[id], encoding);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public string ReadStringValue(int id, Encoding encoding, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadStringValueByAddr(mIdAndAddr[id],encoding, out time, out quality);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public IntPointData ReadIntPointValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadIntPointValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new IntPointData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool ReadIntPointValue(int id,out int x,out int y, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadIntPointValueByAddr(id, out quality, out time, out x, out y);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y =0;
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public UIntPointData ReadUIntPointValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadUIntPointValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new UIntPointData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool ReadUIntPointValue(int id, out uint x, out uint y, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadUIntPointValueByAddr(id, out quality, out time, out x, out y);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y = 0;
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public IntPoint3Data ReadIntPoint3Value(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadIntPoint3ValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new IntPoint3Data();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool ReadIntPoint3Value(int id, out int x, out int y,out int z, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadIntPoint3ValueByAddr(id, out quality, out time, out x, out y,out z);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y =z= 0;
            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public UIntPoint3Data ReadUIntPoint3Value(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadUIntPoint3ValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new UIntPoint3Data();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool ReadUIntPoint3Value(int id, out uint x, out uint y, out uint z, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadUIntPoint3ValueByAddr(id, out quality, out time, out x, out y, out z);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y = z = 0;
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>

        public LongPointData ReadLongPointValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadLongPointValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new LongPointData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool ReadLongPointValue(int id, out long x, out long y, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadLongPointValueByAddr(id, out quality, out time, out x, out y);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y =  0;
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public ULongPointData ReadULongPointValue(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadULongPointValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new ULongPointData();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool ReadULongPointValue(int id, out ulong x, out ulong y, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadULongPointValueByAddr(id, out quality, out time, out x, out y);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y =  0;
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public LongPoint3Data ReadLongPoint3Value(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadLongPoint3ValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new LongPoint3Data();
        }


        public bool ReadLongPoint3Value(int id, out long x, out long y, out long z, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadLongPoint3ValueByAddr(id, out quality, out time, out x, out y, out z);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y =z= 0;
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public ULongPoint3Data ReadULongPoint3Value(int id, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadULongPoint3ValueByAddr(id, out quality, out time);
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            return new ULongPoint3Data();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool ReadULongPoint3Value(int id, out ulong x, out ulong y, out ulong z, out byte quality, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                ReadULongPoint3ValueByAddr(id, out quality, out time, out x, out y, out z);
                return true;
            }
            quality = byte.MaxValue;
            time = DateTime.MinValue;
            x = y = z = 0;
            return false;
        }

        #endregion

        #region Addr




        /// <summary>
        /// 获取某个变量的内存地址
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public void* GetDataRawAddr(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                 return (void*)((byte*)mMHandle +  mIdAndAddr[id]);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public long GetDataAddr(int id)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return mIdAndAddr[id];
            }
            return -1;
        }



        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #region interface IRealTagProducter

        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public List<int?> GetTagIdByName(List<string> name)
        {
            return mConfigDatabase.GetTagIdByName(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public List<int> GetTagIdsByLinkAddress(string address)
        {
            return mConfigDatabase.GetTagIdsByLinkAddress(address);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public Dictionary<string, List<int>> GetTagsIdByLinkAddress(List<string> address)
        {
            return mConfigDatabase.GetTagsIdByLinkAddress(address);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        List<Tagbase> IRealTagProduct.GetTagByLinkAddress(string address)
        {
            return mConfigDatabase.GetTagByLinkAddress(address);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        Dictionary<string, List<Tagbase>> IRealTagProduct.GetTagsByLinkAddress(List<string> address)
        {
            return mConfigDatabase.GetTagsByLinkAddress(address);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public Dictionary<int, long> GetTagMemoryAddress(List<int> ids)
        {
            Dictionary<int, long> re = new Dictionary<int, long>();
            foreach(var vv in mConfigDatabase.GetTagsById(ids))
            {
                re.Add(vv.Id, vv.ValueAddress);
            }
            return re;
        }


       

        #region Set value By Tag Instance  from driver
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetBoolTagValue(Tagbase tag,object value,byte qulity,DateTime time)
        {
            Take();
            bool btmp = false;
            if(tag.Conveter!=null)
            {
                btmp = Convert.ToBoolean(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToBoolean(value);
            }
            SetValueByAddr(tag.ValueAddress, btmp ? (byte)1 : (byte)0, qulity, time);
            NotifyValueChangedToConsumer(tag.Id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetByteTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as NumberTagBase;
            
            byte btmp = 0;
            if (tag.Conveter != null)
            {
                
                btmp = Convert.ToByte(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToByte(value);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity :(byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetShortTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as NumberTagBase;

            short btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = Convert.ToInt16(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToInt16(value);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetUShortTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as NumberTagBase;

            ushort btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = Convert.ToUInt16(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToUInt16(value);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }


        public void SetIntTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as NumberTagBase;

            int btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = Convert.ToInt32(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToInt32(value);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }

        public void SetUIntTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as NumberTagBase;

            uint btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = Convert.ToUInt32(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToUInt32(value);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }


        public void SetLongTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as NumberTagBase;

            long btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = Convert.ToInt64(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToInt64(value);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }


        public void SetULongTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as NumberTagBase;

            ulong btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = Convert.ToUInt64(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToUInt64(value);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }


        public void SetDoubleTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as FloatingTagBase;

            double btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = Math.Round(Convert.ToDouble(tag.Conveter.ConvertTo(value)),vtag.Precision);
            }
            else
            {
                btmp = Math.Round(Convert.ToDouble(value),vtag.Precision);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }


        public void SetFloatTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            var vtag = tag as FloatingTagBase;

            float btmp = 0;
            if (tag.Conveter != null)
            {

                btmp = (float)Math.Round(Convert.ToSingle(tag.Conveter.ConvertTo(value)), vtag.Precision);
            }
            else
            {
                btmp = (float)Math.Round(Convert.ToSingle(value), vtag.Precision);
            }
            if (vtag != null)
            {
                SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);
                NotifyValueChangedToConsumer(tag.Id);
            }
        }


        public void SetSrtingTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            string btmp = "";
            if (tag.Conveter != null)
            {
                btmp = Convert.ToString(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToString(value);
            }
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            NotifyValueChangedToConsumer(tag.Id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetDateTimeTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            DateTime btmp;
            if (tag.Conveter != null)
            {
                btmp = Convert.ToDateTime(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = Convert.ToDateTime(value);
            }
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            NotifyValueChangedToConsumer(tag.Id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetIntPointTagValue(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            IntPointData btmp;
            if (tag.Conveter != null)
            {
                btmp = (IntPointData)(tag.Conveter.ConvertTo(value));
            }
            else
            {
                btmp = (IntPointData)(value);
            }
            SetPointValueByAddr(tag.ValueAddress, btmp.X,btmp.Y, qulity, time);
            NotifyValueChangedToConsumer(tag.Id);
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetTagByGroup(string group, params object[] values)
        {
            Take();
            var vatg = mConfigDatabase.GetTagsByGroup(group);
            DateTime time = DateTime.Now;
            for(int i=0;i<values.Length;i++)
            {
                var tag = vatg[i];
                switch (tag.Type)
                {
                    case TagType.Bool:
                        SetBoolTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.Byte:
                        SetByteTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.DateTime:
                        SetDateTimeTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.Double:
                        SetDoubleTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.Float:
                        SetFloatTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.Int:
                        SetIntTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.Long:
                        SetLongTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.Short:
                        SetShortTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.String:
                        SetSrtingTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.UInt:
                        SetUIntTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.ULong:
                        SetULongTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                    case TagType.UShort:
                        SetUShortTagValue(tag, values[i], (byte)QualityConst.Good, time);
                        break;
                }
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValue(int id, object value)
        {
            try
            {
                Take();
                var tag = mConfigDatabase.Tags[id];

                SetTagValue(tag,value);
            }
            catch
            {
                return false;
            }
            return true;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public bool SetTagValue(int id, object value, DateTime time, byte quality)
        {
            try
            {
                Take();
                var tag = mConfigDatabase.Tags[id];

                SetTagValue(tag, value,  time,  quality);
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public bool SetTagValue(int id, object value, byte quality)
        {
            return SetTagValue(id, value, DateTime.Now, quality);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <returns></returns>
        public bool SetTagValue(Tagbase tag, object value, byte quality)
        {
            return SetTagValue(tag, value, DateTime.Now, quality);
        }

        public bool SetTagValue(Tagbase tag, object value, DateTime time, byte quality)
        {
            try
            {
                Take();
                if (tag.ReadWriteType == ReadWriteMode.Write) return true;

                switch (tag.Type)
                {
                    case TagType.Bool:
                        SetBoolTagValue(tag, value, quality, time);
                        break;
                    case TagType.Byte:
                        SetByteTagValue(tag, value, quality, time);
                        break;
                    case TagType.DateTime:
                        SetDateTimeTagValue(tag, value, quality, time);
                        break;
                    case TagType.Double:
                        SetDoubleTagValue(tag, value, quality, time);
                        break;
                    case TagType.Float:
                        SetFloatTagValue(tag, value, quality, time);
                        break;
                    case TagType.Int:
                        SetIntTagValue(tag, value, quality, time);
                        break;
                    case TagType.Long:
                        SetLongTagValue(tag, value, quality, time);
                        break;
                    case TagType.Short:
                        SetShortTagValue(tag, value, quality, time);
                        break;
                    case TagType.String:
                        SetSrtingTagValue(tag, value, quality, time);
                        break;
                    case TagType.UInt:
                        SetUIntTagValue(tag, value, quality, time);
                        break;
                    case TagType.ULong:
                        SetULongTagValue(tag, value, quality, time);
                        break;
                    case TagType.UShort:
                        SetUShortTagValue(tag, value, quality, time);
                        break;
                }
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValueNoValueCheck(int id, object value)
        {
            try
            {
                Take();
                var tag = mConfigDatabase.Tags[id];
                DateTime time = DateTime.Now;
                switch (mConfigDatabase.Tags[id].Type)
                {
                    case TagType.Bool:
                        SetValueByAddr(id, Convert.ToBoolean(value)?(byte)1:(byte)0,(byte)QualityConst.Good,time);
                        break;
                    case TagType.Byte:
                        SetValueByAddr(id, Convert.ToByte(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.DateTime:
                        SetValueByAddr(id, (DateTime)(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.Double:
                        SetValueByAddr(id, Convert.ToDouble(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.Float:
                        SetValueByAddr(id, Convert.ToSingle(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.Int:
                        SetValueByAddr(id, Convert.ToInt32(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.Long:
                        SetValueByAddr(id, Convert.ToInt64(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.Short:
                        SetValueByAddr(id, Convert.ToInt16(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.String:
                        SetValueByAddr(id, Convert.ToString(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.UInt:
                        SetValueByAddr(id, Convert.ToUInt32(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.ULong:
                        SetValueByAddr(id, Convert.ToUInt64(value), (byte)QualityConst.Good, time);
                        break;
                    case TagType.UShort:
                        SetValueByAddr(id, Convert.ToUInt16(value), (byte)QualityConst.Good, time);
                        break;
                }
            }
            catch
            {
                return false;
            }
            return true;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValue(Tagbase tag, object value)
        {
            try
            {
                //if (tag.Id == 1) LoggerService.Service.Info("RealEnginer", "set tag" + tag.Id + " value: " + value,ConsoleColor.Cyan);

                Take();
                if (tag.ReadWriteType == ReadWriteMode.Write) return true;
                DateTime time = DateTime.Now;
                switch (tag.Type)
                {
                    case TagType.Bool:
                        SetBoolTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Byte:
                        SetByteTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.DateTime:
                        SetDateTimeTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Double:
                        SetDoubleTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Float:
                        SetFloatTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Int:
                        SetIntTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Long:
                        SetLongTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Short:
                        SetShortTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.String:
                        SetSrtingTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.UInt:
                        SetUIntTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.ULong:
                        SetULongTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.UShort:
                        SetUShortTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                }
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValue(List<Tagbase> ids, object value)
        {
            Take();
            bool re = true;
            foreach (var vv in ids)
            {
                re &= SetTagValue(vv, value);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValue(List<int> ids, object value)
        {
            Take();
            bool re = true;
            //Parallel.ForEach(ids, (vv) =>
            foreach (var vv in ids)
            {
                re &= SetTagValue(vv, value);
            }
            //});
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(List<int> ids, params object[] values)
        {
            bool re = true;
            Take();
            //Parallel.ForEach(ids, (vv) =>
            foreach (var vv in ids)
            {
                re &= SetPointValue(vv, values);
            }
            //});
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(Tagbase tag, params object[] values)
        {
            return SetPointValue(tag, QualityConst.Good, values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(List<Tagbase> tags, params object[] values)
        {
            bool re = true;
            Take();
            //Parallel.ForEach(ids, (vv) =>
            foreach (var vv in tags)
            {
                re &= SetPointValue(vv, values);
            }
            //});
            return re;
        }



        /// <summary>
        /// 订购值改变事件
        /// </summary>
        /// <param name="name"></param>
        public ProducterValueChangedNotifyProcesser SubscribeProducter(string name)
        {
            return ProducterValueChangedNotifyManager.Manager.GetNotifier(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        public void SubscribeValueChangedForProducter(string name, ProducterValueChangedNotifyProcesser.ValueChangedDelagete valueChanged, Func<List<int>> tagRegistor)
        {
            var re = ProducterValueChangedNotifyManager.Manager.GetNotifier(name);
            if (tagRegistor != null)
            {
                foreach (var vv in tagRegistor())
                {
                    re.Registor(vv);
                }
            }
            re.ValueChanged = valueChanged;
            re.Start();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void UnSubscribeValueChangedForProducter(string name)
        {
            ProducterValueChangedNotifyManager.Manager.DisposeNotifier(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public object GetTagValueForProductor(int id)
        {
            Take();
            var tag = mConfigDatabase.Tags[id];
            object re=null;
            switch (tag.Type)
            {
                case TagType.Bool:
                     re = Convert.ToBoolean(ReadByteValueByAddr(tag.ValueAddress));
                    break;
                case TagType.Byte:
                    re = ReadByteValueByAddr(tag.ValueAddress);
                    break;
                case TagType.DateTime:
                    re = ReadDateTimeValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Double:
                    re = ReadDoubleValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Float:
                    re = ReadFloatValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Int:
                    re = ReadIntValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Long:
                    re = ReadInt64ValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Short:
                    re = ReadShortValueByAddr(tag.ValueAddress);
                    break;
                case TagType.String:
                    re = ReadStringValueByAddr(tag.ValueAddress,Encoding.Unicode);
                    break;
                case TagType.UInt:
                    re = (uint)ReadIntValueByAddr(tag.ValueAddress);
                    break;
                case TagType.ULong:
                    re = (ulong)ReadInt64ValueByAddr(tag.ValueAddress);
                    break;
                case TagType.UShort:
                    re = (ushort)ReadShortValueByAddr(tag.ValueAddress);
                    break;
            }

            if (tag.Conveter != null) return tag.Conveter.ConvertBackTo(re); else return re;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public Tagbase GetTagById(int id)
        {
            return mConfigDatabase.Tags.ContainsKey(id) ? mConfigDatabase.Tags[id] : null;
        }


        

        /// <summary>
        /// 通知值改变了
        /// </summary>
        /// <param name="id"></param>
        private void NotifyValueChangedToConsumer(int id)
        {
            ComsumerValueChangedNotifyManager.Manager.UpdateValue(id);
            //ComsumerValueChangedNotifyManager.Manager.NotifyChanged();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        private void NotifyValueChangedToConsumer(List<int> ids)
        {
            ComsumerValueChangedNotifyManager.Manager.UpdateValue(ids);
            //ComsumerValueChangedNotifyManager.Manager.NotifyChanged();
        }

        #endregion

        #region Interface IConsumer

        /// <summary>
        /// 订购值改变事件
        /// </summary>
        /// <param name="name"></param>
        public ValueChangedNotifyProcesser SubscribeComsumer(string name)
        {
            return ComsumerValueChangedNotifyManager.Manager.GetNotifier(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        public void SubscribeValueChangedForConsumer(string name, ValueChangedNotifyProcesser.ValueChangedDelegate valueChanged, ValueChangedNotifyProcesser.BlockChangedDelegate blockchanged,Action BlockChangedNotify, Func<List<int>> tagRegistor)
        {
            var re = ComsumerValueChangedNotifyManager.Manager.GetNotifier(name);
            if (tagRegistor != null)
            {
                var val = tagRegistor();
                if (val == null)
                {
                    re.RegistorAll();
                    re.BuildBlock(mConfigDatabase.MaxTagId(), (id) => 
                    { 
                        var itmp = (int)GetDataAddr(id);
                        if (itmp < 0)
                            return (int)(mIdAndAddr.Last().Value);
                        else
                        {
                            return itmp;
                        }
                    });
                }
                else
                {
                    re.Registor(val);
                }
            }
            re.ValueChanged = valueChanged;
            re.BlockChanged = blockchanged;
            re.BlockChangedNotify = BlockChangedNotify;
            re.Start();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void UnSubscribeValueChangedForConsumer(string name)
        {
            ComsumerValueChangedNotifyManager.Manager.DisposeNotifier(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public object GetTagValue(Tagbase tag, out byte quality, out DateTime time)
        {
            try
            {
                Take();
                switch (tag.Type)
                {
                    case TagType.Bool:
                        return Convert.ToBoolean(ReadByteValueByAddr(tag.ValueAddress, out time, out quality));
                    case TagType.Byte:
                        return ReadByteValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.DateTime:
                        return ReadDateTimeValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.Double:
                        return ReadDoubleValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.Float:
                        return ReadFloatValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.Int:
                        return ReadIntValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.Long:
                        return ReadInt64ValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.Short:
                        return ReadShortValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.String:
                        return ReadStringValueByAddr(tag.ValueAddress, Encoding.Unicode, out time, out quality);
                    case TagType.UInt:
                        return (uint)ReadIntValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.ULong:
                        return (ulong)ReadInt64ValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.UShort:
                        return (ushort)ReadShortValueByAddr(tag.ValueAddress, out time, out quality);
                    case TagType.IntPoint:
                        return ReadIntPointValueByAddr(tag.ValueAddress, out quality, out time);
                    case TagType.UIntPoint:
                        return ReadUIntPointValueByAddr(tag.ValueAddress, out quality, out time);
                    case TagType.IntPoint3:
                        return ReadIntPoint3ValueByAddr(tag.ValueAddress, out quality, out time);
                    case TagType.UIntPoint3:
                        return ReadUIntPoint3ValueByAddr(tag.ValueAddress, out quality, out time);
                    case TagType.LongPoint:
                        return ReadLongPointValueByAddr(tag.ValueAddress, out quality, out time);
                    case TagType.ULongPoint:
                        return ReadULongPointValueByAddr(tag.ValueAddress, out quality, out time);
                    case TagType.LongPoint3:
                        return ReadLongPoint3ValueByAddr(tag.ValueAddress, out quality, out time);
                    case TagType.ULongPoint3:
                        return ReadULongPoint3ValueByAddr(tag.ValueAddress, out quality, out time);
                }
            }
            catch
            {

            }
            time = DateTime.Now;
            quality = (byte)QualityConst.Null;
            return null;
        }

        public object GetTagValue(Tagbase tag)
        {
            try
            {
                Take();
                switch (tag.Type)
                {
                    case TagType.Bool:
                        return Convert.ToBoolean(ReadByteValueByAddr(tag.ValueAddress));
                    case TagType.Byte:
                        return ReadByteValueByAddr(tag.ValueAddress);
                    case TagType.DateTime:
                        return ReadDateTimeValueByAddr(tag.ValueAddress);
                    case TagType.Double:
                        return ReadDoubleValueByAddr(tag.ValueAddress);
                    case TagType.Float:
                        return ReadFloatValueByAddr(tag.ValueAddress);
                    case TagType.Int:
                        return ReadIntValueByAddr(tag.ValueAddress);
                    case TagType.Long:
                        return ReadInt64ValueByAddr(tag.ValueAddress);
                    case TagType.Short:
                        return ReadShortValueByAddr(tag.ValueAddress);
                    case TagType.String:
                        return ReadStringValueByAddr(tag.ValueAddress, Encoding.Unicode);
                    case TagType.UInt:
                        return (uint)ReadIntValueByAddr(tag.ValueAddress);
                    case TagType.ULong:
                        return (ulong)ReadInt64ValueByAddr(tag.ValueAddress);
                    case TagType.UShort:
                        return (ushort)ReadShortValueByAddr(tag.ValueAddress);
                    case TagType.IntPoint:
                        return ReadIntPointValueByAddr(tag.ValueAddress);
                    case TagType.UIntPoint:
                        return ReadUIntPointValueByAddr(tag.ValueAddress);
                    case TagType.IntPoint3:
                        return ReadIntPoint3ValueByAddr(tag.ValueAddress);
                    case TagType.UIntPoint3:
                        return ReadUIntPoint3ValueByAddr(tag.ValueAddress);
                    case TagType.LongPoint:
                        return ReadLongPointValueByAddr(tag.ValueAddress);
                    case TagType.ULongPoint:
                        return ReadULongPointValueByAddr(tag.ValueAddress);
                    case TagType.LongPoint3:
                        return ReadLongPoint3ValueByAddr(tag.ValueAddress);
                    case TagType.ULongPoint3:
                        return ReadULongPoint3ValueByAddr(tag.ValueAddress);
                }
            }
            catch
            {

            }
            return null;
        }

        public object GetTagValue(int id)
        {
            Take();
            var tag = mConfigDatabase.GetTagById(id);
            object re = null;
            if (tag == null) return re;

            switch (tag.Type)
            {
                case TagType.Bool:
                    re = Convert.ToBoolean(ReadByteValueByAddr(tag.ValueAddress));
                    break;
                case TagType.Byte:
                    re = ReadByteValueByAddr(tag.ValueAddress);
                    break;
                case TagType.DateTime:
                    re = ReadDateTimeValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Double:
                    re = ReadDoubleValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Float:
                    re = ReadFloatValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Int:
                    re = ReadIntValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Long:
                    re = ReadInt64ValueByAddr(tag.ValueAddress);
                    break;
                case TagType.Short:
                    re = ReadShortValueByAddr(tag.ValueAddress);
                    break;
                case TagType.String:
                    re = ReadStringValueByAddr(tag.ValueAddress, Encoding.Unicode);
                    break;
                case TagType.UInt:
                    re = (uint)ReadIntValueByAddr(tag.ValueAddress);
                    break;
                case TagType.ULong:
                    re = (ulong)ReadInt64ValueByAddr(tag.ValueAddress);
                    break;
                case TagType.UShort:
                    re = (ushort)ReadShortValueByAddr(tag.ValueAddress);
                    break;
            }

            if (tag.Conveter != null) return tag.Conveter.ConvertBackTo(re); else return re;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public object GetTagValue(int id, out byte quality, out DateTime time, out byte valueType)
        {
            Take();
            var tag = mConfigDatabase.GetTagById(id);
            if (tag == null)
            {
                time = DateTime.Now;
                quality = (byte)QualityConst.Null;
                valueType = byte.MaxValue;
                return null;
            }
            valueType = (byte)tag.Type;
            return GetTagValue(tag, out quality, out time);
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="quality"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public object GetTagValue(string name, out byte quality, out DateTime time, out byte valueType)
        {
            Take();
            var tag = mConfigDatabase.GetTagByName(name);
            if (tag != null)
            {
                valueType = (byte)tag.Type;
                return GetTagValue(tag, out quality, out time);
            }
            quality = (byte)QualityConst.Null;
            time = DateTime.Now;
            valueType = byte.MaxValue;
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValueForConsumer(int id, object value)
        {
            try
            {
                Take();
                var tag = mConfigDatabase.Tags[id];
                DateTime time = DateTime.Now;
                switch (tag.Type)
                {
                    case TagType.Bool:
                        SetBoolTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Byte:
                        SetByteTagValue(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.DateTime:
                        SetDateTimeTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Double:
                        SetDoubleTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Float:
                        SetFloatTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Int:
                        SetIntTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Long:
                        SetLongTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.Short:
                        SetShortTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.String:
                        SetSrtingTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.UInt:
                        SetUIntTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.ULong:
                        SetULongTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.UShort:
                        SetUShortTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.IntPoint:
                        SetIntPointTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.UIntPoint:
                        SetUIntPointTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.IntPoint3:
                        SetIntPoint3TagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.UIntPoint3:
                        SetUIntPoint3TagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.LongPoint:
                        SetLongPointTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.ULongPoint:
                        SetULongPointTagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.LongPoint3:
                        SetLongPoint3TagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                    case TagType.ULongPoint3:
                        SetULongPoint3TagValueForConsumer(tag, value, (byte)QualityConst.Good, time);
                        break;
                }
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValueForConsumer(string name, object value)
        {
            Take();
            var tag = mConfigDatabase.GetTagIdByName(name);
            if (tag != null)
            {
                return SetTagValueForConsumer(tag.Value, value);
            }
            return true;
        }



        #region Set value By Tag Instance  from Comsumer
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetBoolTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            bool btmp = Convert.ToBoolean(value);
            SetValueByAddr(tag.ValueAddress, btmp ? (byte)1 : (byte)0, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = Convert.ToBoolean(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetByteTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as NumberTagBase;

            byte btmp = Convert.ToByte(value);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;

            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = Convert.ToByte(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetShortTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as NumberTagBase;

            short btmp = Convert.ToInt16(value);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = Convert.ToInt16(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetUShortTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as NumberTagBase;

            ushort btmp = Convert.ToUInt16(value);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = Convert.ToUInt16(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetIntTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as NumberTagBase;

            int btmp = Convert.ToInt32(value);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {

                btmp = Convert.ToInt32(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        public bool SetUIntTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as NumberTagBase;

            uint btmp = Convert.ToUInt32(value);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = Convert.ToUInt32(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }


        public bool SetLongTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as NumberTagBase;

            long btmp = Convert.ToInt64(value);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {

                btmp = Convert.ToInt64(tag.Conveter.ConvertBackTo(value));
            }

            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }


        public bool SetULongTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as NumberTagBase;
            if (vtag == null) return false;

            ulong btmp = Convert.ToUInt64(value);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);

            if (tag.Conveter != null)
            {

                btmp = Convert.ToUInt64(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool SetDoubleTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as FloatingTagBase;
            if (vtag == null) return false;

            double btmp = Math.Round(Convert.ToDouble(value), vtag.Precision);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;

            SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);

            if (tag.Conveter != null)
            {
                btmp = Math.Round(Convert.ToDouble(tag.Conveter.ConvertBackTo(value)), vtag.Precision);
            }

            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetFloatTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            var vtag = tag as FloatingTagBase;
            if (vtag == null) return false;

            float btmp = (float)Math.Round(Convert.ToSingle(value), vtag.Precision);
            if (btmp < vtag.MinValue || btmp > vtag.MaxValue) return false;
            SetValueByAddr(tag.ValueAddress, btmp, (btmp >= vtag.MinValue && btmp <= vtag.MaxValue ? qulity : (byte)QualityConst.OutOfRang), time);

            if (tag.Conveter != null)
            {
                btmp = (float)Math.Round(Convert.ToSingle(tag.Conveter.ConvertBackTo(value)), vtag.Precision);
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }


        public bool SetSrtingTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            string btmp = Convert.ToString(value);
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = Convert.ToString(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetDateTimeTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            DateTime btmp = Convert.ToDateTime(value);
            SetValueByAddr(tag.ValueAddress, btmp, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = Convert.ToDateTime(tag.Conveter.ConvertBackTo(value));
            }

            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public bool SetIntPointTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            IntPointData btmp = (IntPointData)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (IntPointData)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public bool SetUIntPointTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            UIntPointData btmp = (UIntPointData)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (UIntPointData)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        public bool SetUIntPoint3TagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            UIntPoint3Data btmp = (UIntPoint3Data)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (UIntPoint3Data)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }


        public bool SetIntPoint3TagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            IntPoint3Data btmp = (IntPoint3Data)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (IntPoint3Data)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }


        public bool SetLongPointTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            LongPointData btmp = (LongPointData)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (LongPointData)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        public bool SetULongPointTagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            ULongPointData btmp = (ULongPointData)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (ULongPointData)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }


        public bool SetLongPoint3TagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            LongPoint3Data btmp = (LongPoint3Data)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (LongPoint3Data)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        public bool SetULongPoint3TagValueForConsumer(Tagbase tag, object value, byte qulity, DateTime time)
        {
            Take();
            if (tag.ReadWriteType == ReadWriteMode.Read) return false;
            ULongPoint3Data btmp = (ULongPoint3Data)(value);
            SetPointValueByAddr(tag.ValueAddress, btmp.X, btmp.Y, qulity, time);
            if (tag.Conveter != null)
            {
                btmp = (ULongPoint3Data)(tag.Conveter.ConvertBackTo(value));
            }
            NotifyValueChangedToProducter(tag.Id, btmp);
            return true;
        }

        #endregion
        #endregion

        /// <summary>
        /// 
        /// </summary>

        public void Dispose()
        {
            mMemory = null;
            mMHandle = (void*)IntPtr.Zero;
            mIdAndAddr.Clear();
            mConfigDatabase.Tags.Clear();
            mConfigDatabase = null;
        }





        #endregion ...Interfaces...
    }
}
