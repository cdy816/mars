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

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe class RealEnginer: IRealDataNotify, IRealData, Driver.IRealTagDriver
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


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public RealEnginer()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public RealEnginer(RealDatabase database)
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
        /// <param name="database"></param>
        public void UpdateDatabase(RealDatabase database)
        {
            this.mConfigDatabase = database;
            Init();
        }

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
            mUsedSize = (long)(msize * 1.2);
            mMemory = new byte[mUsedSize];
            mMHandle = mMemory.AsMemory().Pin().Pointer;

            LoggerService.Service.Info("RealEnginer","Cal memory size:"+ mUsedSize/1024.0/1024+"M",ConsoleColor.Cyan);

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
        /// 订购值改变事件
        /// </summary>
        /// <param name="name"></param>
        public ValueChangedNotifyProcesser Subscribe(string name)
        {
             return ValueChangedNotifyManager.Manager.GetNotifier(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="valueChanged"></param>
        /// <param name="tagRegistor"></param>
        public void Subscribe(string name, ValueChangedNotifyProcesser.ValueChangedDelagete valueChanged,Func<List<int>> tagRegistor)
        {
            var re = ValueChangedNotifyManager.Manager.GetNotifier(name);
            if(tagRegistor!=null)
            {
                foreach(var vv in tagRegistor())
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
        public void UnSubscribe(string name)
        {
            ValueChangedNotifyManager.Manager.DisposeNotifier(name);
        }

        /// <summary>
        /// 通知值改变了
        /// </summary>
        /// <param name="id"></param>
        private void NotifyValueChanged(int id)
        {
            ValueChangedNotifyManager.Manager.UpdateValue(id);
            ValueChangedNotifyManager.Manager.NotifyChanged();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        private void NotifyValueChanged(List<int> ids)
        {
            ValueChangedNotifyManager.Manager.UpdateValue(ids);
            ValueChangedNotifyManager.Manager.NotifyChanged();
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
            MemoryHelper.WriteDouble(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality); ;
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
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality); ;
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
            MemoryHelper.WriteByte(mMHandle, addr + 16, quality); ;
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
            MemoryHelper.WriteByte(mMHandle, addr + 24, quality); ;
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
            MemoryHelper.WriteByte(mMHandle, addr + 32, quality); ;
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
            MemoryHelper.WriteByte(mMHandle, addr + 32, quality); ;
        }
        #endregion

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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
                switch (mConfigDatabase.Tags[id].Type)
                {
                    case TagType.IntPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], quality, time);
                        break;
                    case TagType.UIntPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (uint)values[0], (uint)values[1], quality, time);
                        break;
                    case TagType.IntPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], (int)values[2], quality, time);
                        break;
                    case TagType.UIntPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], (int)values[2], quality, time);
                        break;
                    case TagType.LongPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (long)values[0], (long)values[1], quality, time);
                        break;
                    case TagType.ULongPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (ulong)values[0], (ulong)values[1], quality, time);
                        break;
                    case TagType.LongPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (long)values[0], (long)values[1], (long)values[2], quality, time);
                        break;
                    case TagType.ULongPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (ulong)values[0], (ulong)values[1], (ulong)values[2], quality, time);
                        break;
                }
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
            if (mIdAndAddr.ContainsKey(id) && mConfigDatabase.Tags.ContainsKey(id))
            {
                switch (mConfigDatabase.Tags[id].Type)
                {
                    case TagType.IntPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], quality, time);
                        break;
                    case TagType.UIntPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (uint)values[0], (uint)values[1], quality, time);
                        break;
                    case TagType.IntPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], (int)values[2], quality, time);
                        break;
                    case TagType.UIntPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], (int)values[2], quality, time);
                        break;
                    case TagType.LongPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (long)values[0], (long)values[1], quality, time);
                        break;
                    case TagType.ULongPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (ulong)values[0], (ulong)values[1], quality, time);
                        break;
                    case TagType.LongPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (long)values[0], (long)values[1], (long)values[2], quality, time);
                        break;
                    case TagType.ULongPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (ulong)values[0], (ulong)values[1], (ulong)values[2], quality, time);
                        break;
                }
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool SetPointValue(int id,  params object[] values)
        {
            DateTime time = DateTime.Now;
            byte quality = 0;
            if (mIdAndAddr.ContainsKey(id) && mConfigDatabase.Tags.ContainsKey(id))
            {
                switch (mConfigDatabase.Tags[id].Type)
                {
                    case TagType.IntPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], quality, time);
                        break;
                    case TagType.UIntPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (uint)values[0], (uint)values[1], quality, time);
                        break;
                    case TagType.IntPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], (int)values[2], quality, time);
                        break;
                    case TagType.UIntPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (int)values[0], (int)values[1], (int)values[2], quality, time);
                        break;
                    case TagType.LongPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (long)values[0], (long)values[1], quality, time);
                        break;
                    case TagType.ULongPoint:
                        SetPointValueByAddr(mIdAndAddr[id], (ulong)values[0], (ulong)values[1], quality, time);
                        break;
                    case TagType.LongPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (long)values[0], (long)values[1], (long)values[2], quality, time);
                        break;
                    case TagType.ULongPoint3:
                        SetPointValueByAddr(mIdAndAddr[id], (ulong)values[0], (ulong)values[1], (ulong)values[2], quality, time);
                        break;
                }
            }
            return true;
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
            NotifyValueChanged(values.Keys.ToList());
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
                NotifyValueChanged(id);
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
            NotifyValueChanged(values.Keys.ToList());
        }
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public List<int> GetTagByLinkAddress(string address)
        {
            return mConfigDatabase.GetTagIdByLinkAddress(address);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public Dictionary<string, List<int>> GetTagsByLinkAddress(List<string> address)
        {
            return mConfigDatabase.GetTagsIdByLinkAddress(address);
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
                switch (mConfigDatabase.Tags[id].Type)
                {
                    case TagType.Bool:
                        SetValue(id, Convert.ToBoolean(value));
                        break;
                    case TagType.Byte:
                        SetValue(id, Convert.ToByte(value));
                        break;
                    case TagType.DateTime:
                        SetValue(id, (DateTime)(value));
                        break;
                    case TagType.Double:
                        SetValue(id, Convert.ToDouble(value));
                        break;
                    case TagType.Float:
                        SetValue(id, Convert.ToSingle(value));
                        break;
                    case TagType.Int:
                        SetValue(id, Convert.ToInt32(value));
                        break;
                    case TagType.Long:
                        SetValue(id, Convert.ToInt64(value));
                        break;
                    case TagType.Short:
                        SetValue(id, Convert.ToInt16(value));
                        break;
                    case TagType.String:
                        SetValue(id, Convert.ToString(value));
                        break;
                    case TagType.UInt:
                        SetValue(id, Convert.ToUInt32(value));
                        break;
                    case TagType.ULong:
                        SetValue(id, Convert.ToUInt64(value));
                        break;
                    case TagType.UShort:
                        SetValue(id, Convert.ToUInt16(value));
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
        public bool SetTagValue(List<int> ids, object value)
        {
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
            //Parallel.ForEach(ids, (vv) =>
            foreach (var vv in ids)
            {
                re &= SetPointValue(vv, values);
            }
            //});
            return re;
        }


        #endregion ...Interfaces...
    }
}
