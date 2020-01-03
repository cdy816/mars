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
    public unsafe class RealEnginer: IRealDataNotify, IRealData
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private byte[] mMemory;

        /// <summary>
        /// 
        /// </summary>
        private Database mConfigDatabase=null;



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
        public RealEnginer(Database database)
        {
            this.mConfigDatabase = database;
            Init();
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

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void UpdateDatabase(Database database)
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
            foreach(var vv in mConfigDatabase.Tags)
            {
                vv.Value.ValueAddress = msize;
                switch (vv.Value.Type)
                {
                    case TagType.Bool:
                    case TagType.Byte:
                        msize+=10;
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
                    case TagType.String:
                        msize += (Const.StringSize + 9);
                        break;
                }
            }
            //留20%的余量
            mUsedSize = (long)(msize * 1.2);
            mMemory = new byte[mUsedSize];
            mMHandle = mMemory.AsMemory().Pin().Pointer;
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
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, byte value,byte qulity,DateTime time)
        {
            mMemory[addr] = value;
            MemoryHelper.WriteDateTime(mMHandle, addr + 1,time);
            MemoryHelper.WriteByte(mMHandle, addr + 9, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, short value)
        {
            MemoryHelper.WriteShort(mMHandle, addr, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, short value, byte qulity, DateTime time)
        {
            MemoryHelper.WriteShort(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr +2, time);
            MemoryHelper.WriteByte(mMHandle, addr + 10, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, int value)
        {
            MemoryHelper.WriteInt32(mMHandle, addr, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, int value, byte qulity, DateTime time)
        {
            MemoryHelper.WriteInt32(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 4, time);
            MemoryHelper.WriteByte(mMHandle, addr + 12, qulity); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, long value)
        {
            MemoryHelper.WriteInt64(mMHandle, addr, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, long value, byte qulity, DateTime time)
        {
            MemoryHelper.WriteInt64(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, qulity); ;
        }

 

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, float value)
        {
            MemoryHelper.WriteFloat(mMHandle, addr, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, float value, byte qulity, DateTime time)
        {
            MemoryHelper.WriteFloat(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 4, time);
            MemoryHelper.WriteByte(mMHandle, addr + 12, qulity); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, double value)
        {
            MemoryHelper.WriteDouble(mMHandle, addr, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, double value, byte qulity, DateTime time)
        {
            MemoryHelper.WriteDouble(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, qulity); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        public void SetValueByAddr(long addr, DateTime value)
        {
            MemoryHelper.WriteDateTime(mMHandle, addr, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, DateTime value, byte qulity, DateTime time)
        {
            MemoryHelper.WriteDateTime(mMHandle, addr, value);
            MemoryHelper.WriteDateTime(mMHandle, addr + 8, time);
            MemoryHelper.WriteByte(mMHandle, addr + 16, qulity); ;
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
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValueByAddr(long addr, string value, byte qulity, DateTime time)
        {
            System.Buffer.BlockCopy(value.ToCharArray(), 0, mMemory, (int)addr, value.Length);
            MemoryHelper.WriteDateTime(mMHandle, Const.StringSize, time);
            MemoryHelper.WriteByte(mMHandle, Const.StringSize + 8, qulity); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        public void SetValue(int id, byte value)
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
        public void SetValue(Dictionary<int,byte> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<byte,byte,DateTime>> values)
        {
            Parallel.ForEach(values,(vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, byte value,byte qulity,DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value,qulity,time);
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
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, short> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, short value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<short, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, ushort> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, ushort value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<ushort, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, int> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, int value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<int, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, uint> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, uint value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<uint, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, long> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, long value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<long, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, ulong> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, ulong value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<ulong, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
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
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, float value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<float, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, double value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<double, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
                SetValueByAddr(mIdAndAddr[id], value);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, DateTime> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, DateTime value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<DateTime, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, string> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value);
                }
            });
            NotifyValueChanged(values.Keys.ToList());
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        public void SetValue(int id, string value, byte qulity, DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                SetValueByAddr(mIdAndAddr[id], value, qulity, time);
                NotifyValueChanged(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void SetValue(Dictionary<int, Tuple<string, byte, DateTime>> values)
        {
            Parallel.ForEach(values, (vv) => {
                if (mIdAndAddr.ContainsKey(vv.Key))
                {
                    SetValueByAddr(mIdAndAddr[vv.Key], vv.Value.Item1, vv.Value.Item2, vv.Value.Item3);
                }
            });
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public string ReadStringValueByAddr(long addr, Encoding encoding,out DateTime time, out byte qulity)
        {
            int len = MemoryHelper.ReadByte((sbyte*)mMHandle, addr);
            var re = new string((sbyte*)mMHandle, (int)addr+1, len, encoding);
            time = MemoryHelper.ReadDateTime(mMHandle, addr+ Const.StringSize);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + Const.StringSize + 8);
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public byte ReadByteValueByAddr(long addr, out DateTime time, out byte qulity)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 1);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + 9);
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public int ReadIntValueByAddr(long addr, out DateTime time, out byte qulity)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 4);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + 12);
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public short ReadShortValueByAddr(long addr, out DateTime time, out byte qulity)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 2);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + 10);
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public long ReadInt64ValueByAddr(long addr, out DateTime time, out byte qulity)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + 16);
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public double ReadDoubleValueByAddr(long addr, out DateTime time, out byte qulity)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + 16);
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public float ReadFloatValueByAddr(long addr, out DateTime time, out byte qulity)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 4);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + 12);
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
        /// <param name="qulity"></param>
        /// <returns></returns>
        public DateTime ReadDateTimeValueByAddr(long addr, out DateTime time, out byte qulity)
        {
            time = MemoryHelper.ReadDateTime(mMHandle, addr + 8);
            qulity = MemoryHelper.ReadByte(mMHandle, addr + 16);
            return MemoryHelper.ReadDateTime(mMHandle, addr);
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public byte? ReadByteValue(int id,out byte qulity,out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadByteValueByAddr(mIdAndAddr[id],out time,out qulity);
            }
            qulity = byte.MaxValue;
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public short? ReadShortValue(int id, out byte qulity, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadShortValueByAddr(mIdAndAddr[id], out time, out qulity);
            }
            qulity = byte.MaxValue;
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public int? ReadIntValue(int id, out byte qulity, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadIntValueByAddr(mIdAndAddr[id], out time, out qulity);
            }
            qulity = byte.MaxValue;
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public long? ReadInt64Value(int id, out byte qulity, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadInt64ValueByAddr(mIdAndAddr[id], out time, out qulity);
            }
            qulity = byte.MaxValue;
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public double? ReadDoubleValue(int id, out byte qulity, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadDoubleValueByAddr(mIdAndAddr[id], out time, out qulity);
            }
            qulity = byte.MaxValue;
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public float? ReadFloatValue(int id, out byte qulity, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadFloatValueByAddr(mIdAndAddr[id], out time, out qulity);
            }
            qulity = byte.MaxValue;
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public DateTime? ReadDatetimeValue(int id, out byte qulity, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadDateTimeValueByAddr(mIdAndAddr[id], out time, out qulity);
            }
            qulity = byte.MaxValue;
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
        /// <param name="qulity"></param>
        /// <param name="time"></param>
        /// <returns></returns>
        public string ReadStringValue(int id, Encoding encoding, out byte qulity, out DateTime time)
        {
            if (mIdAndAddr.ContainsKey(id))
            {
                return ReadStringValueByAddr(mIdAndAddr[id],encoding, out time, out qulity);
            }
            qulity = byte.MaxValue;
            time = DateTime.MinValue;
            return null;
        }

        #endregion

        //#region Helper fun

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <param name="val"></param>
        //public static unsafe void WriteByte(void* ptr, long ofs, byte val)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        *(addr) = val;
        //    }
        //    catch (NullReferenceException)
        //    {
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <param name="val"></param>
        //public static unsafe void WriteShort(void* ptr, long ofs, short val)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((short)addr) & 0x3) == 0)
        //        {
        //            // aligned write
        //            *((short*)addr) = val;
        //        }
        //        else
        //        {
        //            // unaligned write
        //            byte* valPtr = (byte*)&val;
        //            addr[0] = valPtr[0];
        //            addr[1] = valPtr[1];
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <param name="val"></param>
        //public static unsafe void WriteInt32(void* ptr, long ofs, int val)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((int)addr) & 0x3) == 0)
        //        {
        //            // aligned write
        //            *((int*)addr) = val;
        //        }
        //        else
        //        {
        //            // unaligned write
        //            byte* valPtr = (byte*)&val;
        //            addr[0] = valPtr[0];
        //            addr[1] = valPtr[1];
        //            addr[2] = valPtr[2];
        //            addr[3] = valPtr[3];
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <param name="val"></param>
        //public static unsafe void WriteFloat(void* ptr, long ofs, float val)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((int)addr) & 0x3) == 0)
        //        {
        //            // aligned write
        //            *((int*)addr) = *(int*)(&val);
        //        }
        //        else
        //        {
        //            // unaligned write
        //            byte* valPtr = (byte*)&val;
        //            addr[0] = valPtr[0];
        //            addr[1] = valPtr[1];
        //            addr[2] = valPtr[2];
        //            addr[3] = valPtr[3];
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <param name="val"></param>
        //public static unsafe void WriteInt64(void* ptr, long ofs, Int64 val)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((Int64)addr) & 0x3) == 0)
        //        {
        //            // aligned write
        //            *((Int64*)addr) = val;
        //        }
        //        else
        //        {
        //            // unaligned write
        //            byte* valPtr = (byte*)&val;
        //            addr[0] = valPtr[0];
        //            addr[1] = valPtr[1];
        //            addr[2] = valPtr[2];
        //            addr[3] = valPtr[3];
        //            addr[4] = valPtr[4];
        //            addr[5] = valPtr[5];
        //            addr[6] = valPtr[6];
        //            addr[7] = valPtr[7];
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <param name="val"></param>
        //public static unsafe void WriteDouble(void* ptr, long ofs, double val)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((Int64)addr) & 0x3) == 0)
        //        {
        //            // aligned write
        //            *((Int64*)addr) = *(Int64*)(&val);
        //        }
        //        else
        //        {
        //            // unaligned write
        //            byte* valPtr = (byte*)&val;
        //            addr[0] = valPtr[0];
        //            addr[1] = valPtr[1];
        //            addr[2] = valPtr[2];
        //            addr[3] = valPtr[3];
        //            addr[4] = valPtr[4];
        //            addr[5] = valPtr[5];
        //            addr[6] = valPtr[6];
        //            addr[7] = valPtr[7];
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <param name="val"></param>
        //public static unsafe void WriteDateTime(void* ptr, long ofs, DateTime val)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((Int64)addr) & 0x3) == 0)
        //        {
        //            // aligned write
        //            *((Int64*)addr) = *(Int64*)(&val);
        //        }
        //        else
        //        {
        //            // unaligned write
        //            byte* valPtr = (byte*)&val;
        //            addr[0] = valPtr[0];
        //            addr[1] = valPtr[1];
        //            addr[2] = valPtr[2];
        //            addr[3] = valPtr[3];
        //            addr[4] = valPtr[4];
        //            addr[5] = valPtr[5];
        //            addr[6] = valPtr[6];
        //            addr[7] = valPtr[7];
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <returns></returns>
        //public static unsafe int ReadInt32(void* ptr, long ofs)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((int)addr) & 0x3) == 0)
        //        {
        //            //aligned read
        //            return *((int*)addr);
        //        }
        //        else
        //        {
        //            // unaligned read
        //            int val;
        //            byte* valPtr = (byte*)&val;
        //            valPtr[0] = addr[0];
        //            valPtr[1] = addr[1];
        //            valPtr[2] = addr[2];
        //            valPtr[3] = addr[3];
        //            return val;
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <returns></returns>
        //public static unsafe float ReadFloat(void* ptr, long ofs)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((int)addr) & 0x3) == 0)
        //        {
        //            //aligned read
        //            return *((float*)addr);
        //        }
        //        else
        //        {
        //            // unaligned read
        //            float val;
        //            byte* valPtr = (byte*)&val;
        //            valPtr[0] = addr[0];
        //            valPtr[1] = addr[1];
        //            valPtr[2] = addr[2];
        //            valPtr[3] = addr[3];
        //            return val;
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <returns></returns>
        //public static unsafe short ReadShort(void* ptr, long ofs)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((short)addr) & 0x3) == 0)
        //        {
        //            //aligned read
        //            return *((short*)addr);
        //        }
        //        else
        //        {
        //            // unaligned read
        //            short val;
        //            byte* valPtr = (byte*)&val;
        //            valPtr[0] = addr[0];
        //            valPtr[1] = addr[1];
        //            return val;
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <returns></returns>
        //public static unsafe byte ReadByte(void* ptr, long ofs)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        return *(addr);
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <returns></returns>
        //public static unsafe long ReadInt64(void* ptr, long ofs)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((long)addr) & 0x3) == 0)
        //        {
        //            //aligned read
        //            return *((long*)addr);
        //        }
        //        else
        //        {
        //            // unaligned read
        //            long val;
        //            byte* valPtr = (byte*)&val;
        //            valPtr[0] = addr[0];
        //            valPtr[1] = addr[1];
        //            valPtr[2] = addr[2];
        //            valPtr[3] = addr[3];

        //            valPtr[4] = addr[4];
        //            valPtr[5] = addr[5];
        //            valPtr[6] = addr[6];
        //            valPtr[7] = addr[7];

        //            return val;
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <returns></returns>
        //public static unsafe double ReadDouble(void* ptr, long ofs)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((long)addr) & 0x3) == 0)
        //        {
        //            //aligned read
        //            return *((double*)addr);
        //        }
        //        else
        //        {
        //            // unaligned read
        //            double val;
        //            byte* valPtr = (byte*)&val;
        //            valPtr[0] = addr[0];
        //            valPtr[1] = addr[1];
        //            valPtr[2] = addr[2];
        //            valPtr[3] = addr[3];

        //            valPtr[4] = addr[4];
        //            valPtr[5] = addr[5];
        //            valPtr[6] = addr[6];
        //            valPtr[7] = addr[7];

        //            return val;
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="ptr"></param>
        ///// <param name="ofs"></param>
        ///// <returns></returns>
        //public static unsafe DateTime ReadDateTime(void* ptr, long ofs)
        //{
        //    try
        //    {
        //        byte* addr = (byte*)ptr + ofs;
        //        if ((unchecked((long)addr) & 0x3) == 0)
        //        {
        //            //aligned read
        //            return *((DateTime*)addr);
        //        }
        //        else
        //        {
        //            // unaligned read
        //            DateTime val;
        //            byte* valPtr = (byte*)&val;
        //            valPtr[0] = addr[0];
        //            valPtr[1] = addr[1];
        //            valPtr[2] = addr[2];
        //            valPtr[3] = addr[3];

        //            valPtr[4] = addr[4];
        //            valPtr[5] = addr[5];
        //            valPtr[6] = addr[6];
        //            valPtr[7] = addr[7];

        //            return val;
        //        }
        //    }
        //    catch (NullReferenceException)
        //    {
        //        // this method is documented to throw AccessViolationException on any AV
        //        throw new AccessViolationException();
        //    }
        //}
        //#endregion

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

        #endregion ...Interfaces...
    }
}
