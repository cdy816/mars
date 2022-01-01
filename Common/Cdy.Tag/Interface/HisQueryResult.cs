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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe class HisQueryResult<T>:IDisposable
    {

        #region ... Variables  ...

        //private byte[] mDataBuffer;

        private int mTimeAddr = 0;

        private int mQulityAddr = 0;

        //private int mLenght = 0;

        private int mDataSize = 0;

        private byte mDataType = 0;

        private int mPosition = 0;

        private int mCount = 0;

        private IntPtr handle;

        private int mLimite = 0;

        //public static byte[] zoreData = new byte[1024 * 10];

        private int mSize;

        private List<string> mStringCach;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public HisQueryResult(int count)
        {
            mDataSize = GetDataLen();
            Init(count);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public IntPtr Address
        {
            get
            {
                return handle;
            }
        }


        /// <summary>
        /// 当前添加数值个数
        /// </summary>
        public int Count
        {
            get
            {
                return mCount;
            }
            set
            {
                mCount = value;
            }
        }

        /// <summary>
        /// 总共数值个数
        /// </summary>
        public int Length
        {
            get
            {
                return mLimite;
            }
        }

        /// <summary>
        /// 总共内存分配大小
        /// </summary>
        public int Size
        {
            get
            {
                return mSize;
            }
        }




        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public HisQueryResult<T> Contracts()
        {
            HisQueryResult<T> re = new HisQueryResult<T>(this.Count);
            Buffer.MemoryCopy((void*)this.handle, (void*)re.handle, re.Size, mDataSize * Count);
            Buffer.MemoryCopy((void*)(this.handle+ mTimeAddr), (void*)(re.handle+ re.mTimeAddr), re.Size, 8 * Count);
            Buffer.MemoryCopy((void*)(this.handle + mQulityAddr), (void*)(re.handle + re.mQulityAddr), re.Size, Count);
            re.mCount = this.mCount;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        private void Init(int count)
        {
            //mDataBuffer = new byte[count * (9 + mDataSize)];

            int csize = count * (9 + mDataSize);

            int cc = csize / 1024;
            if(csize % 1024!=0)
            {
                cc++;
            }

            csize = cc * 1024;

            mTimeAddr = count * mDataSize;
            mQulityAddr = count * (mDataSize + 8);
            //mLenght = count;
            //handle = mDataBuffer.AsMemory().Pin().Pointer;
            handle = Marshal.AllocHGlobal(csize);
           // handle = (void*)System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mDataBuffer, 0);
            mLimite = count;
            mSize = csize;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add<TT>(TT value,DateTime time,byte qulity)
        {
            Add((object)value, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public object GetTargetValue(object value)
        {
            switch (mDataType)
            {
                case 0:
                    return BoolValueConvert(value);
                case 1:
                    return ByteValueConvert(value);
                case 2:
                    return ShortValueConvert(value);
                case 3:
                    return UShortValueConvert(value);
                case 4:
                    return IntValueConvert(value);
                case 5:
                    return UIntValueConvert(value);
                case 6:
                    return LongValueConvert(value);
                case 7:
                    return ULongValueConvert(value);
                case 8:
                    return FloatValueConvert(value);
                case 9:
                    return DoubleValueConvert(value);
                case 10:
                    return DatetimeValueConvert(value);
                case 11:
                    return StringValueConvert(value);
                case 12:
                    return IntPointData.ToIntPointData(value);
                case 13:
                    return UIntPointData.ToPointData(value);
                case 14:
                    return IntPoint3Data.ToPointData(value);
                case 15:
                    return UIntPoint3Data.ToPointData(value);
                case 16:
                    return LongPointData.ToPointData(value);
                case 17:
                    return ULongPointData.ToPointData(value);
                case 18:
                    return LongPoint3Data.ToPointData(value);
                case 19:
                    return ULongPoint3Data.ToPointData(value);
            }
            return value;
        }

        public bool BoolValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch(code)
            {
                
                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return false;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToBoolean(value);
                    }
                    catch
                    {
                    }
                    break;
            }
           return  false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public byte ByteValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToByte(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public short ShortValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToInt16(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public ushort UShortValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToUInt16(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }


        public int IntValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToInt32(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public uint UIntValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToUInt32(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public long LongValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                    return 0;
                case TypeCode.DateTime:
                    return ((DateTime)(value)).Ticks;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToInt64(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public ulong ULongValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                    return 0;
                case TypeCode.DateTime:
                    return (ulong)((DateTime)(value)).Ticks;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToUInt64(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public double DoubleValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToDouble(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public double FloatValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                case TypeCode.Byte:
                    try
                    {
                        return Convert.ToSingle(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public string StringValueConvert(object value)
        {
            if (value == null) return string.Empty;

            return Convert.ToString(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DateTime DatetimeValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                    return DateTime.MinValue;
                case TypeCode.DateTime:
                case TypeCode.String:
                    return Convert.ToDateTime(value);
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return DateTime.FromBinary(Convert.ToInt64(value));
            }
            return DateTime.MinValue;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(object value,DateTime time,byte qulity)
        {

            if(mCount>=mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }

            switch(mDataType)
            {
                case 0:
                    Marshal.WriteByte(handle + mPosition, (byte)(Convert.ToBoolean(value)?1:0));
                    mPosition++;
                    break;
                case 1:
                    Marshal.WriteByte(handle+mPosition,Convert.ToByte(value));
                    //mDataBuffer[mPosition] = (byte)value;
                    mPosition++;
                    break;
                case 2:
                    MemoryHelper.WriteShort((void*)handle, mPosition, Convert.ToInt16(value));
                    mPosition+=2;
                    break;
                case 3:
                    MemoryHelper.WriteUShort((void*)handle, mPosition, Convert.ToUInt16(value));
                    mPosition += 2;
                    break;
                case 4:
                    MemoryHelper.WriteInt32((void*)handle, mPosition, Convert.ToInt32(value));
                    mPosition += 4;
                    break;
                case 5:
                    MemoryHelper.WriteUInt32((void*)handle, mPosition, Convert.ToUInt32(value));
                    mPosition += 4;
                    break;
                case 6:
                    MemoryHelper.WriteInt64((void*)handle, mPosition, Convert.ToInt64(value));
                    mPosition += 8;
                    break;
                case 7:
                    MemoryHelper.WriteUInt64((void*)handle, mPosition, Convert.ToUInt64(value));
                    mPosition += 8;
                    break;
                case 8:
                    MemoryHelper.WriteFloat((void*)handle, mPosition, Convert.ToSingle(value));
                    mPosition += 4;
                    break;
                case 9:
                    MemoryHelper.WriteDouble((void*)handle, mPosition,Convert.ToDouble(value));
                    mPosition += 8;
                    break;
                case 10:
                    try
                    {
                        MemoryHelper.WriteDateTime((void*)handle, mPosition, Convert.ToDateTime(value));
                    }
                    catch
                    {

                    }
                    mPosition += 8;
                    break;
                case 11:

                    var sdata = Encoding.Unicode.GetBytes((string)value);
                    MemoryHelper.WriteByte((void*)handle, mPosition, (byte)sdata.Length);
                    mPosition++;

                    Marshal.Copy(sdata, 0, handle+ mPosition, sdata.Length);
                    mPosition += sdata.Length;
                    break;
                case 12:
                    Add((IntPointData)value, time, qulity);
                    return;
                case 13:
                    Add((UIntPointData)value, time, qulity);
                    return;
                case 14:
                    Add((IntPoint3Data)value, time, qulity);
                    return;
                case 15:
                    Add((UIntPoint3Data)value, time, qulity);
                    return;
                case 16:
                    Add((LongPointData)value, time, qulity);
                    return;
                case 17:
                    Add((ULongPointData)value, time, qulity);
                    return;
                case 18:
                    Add((LongPoint3Data)value, time, qulity);
                    return;
                case 19:
                    Add((ULongPoint3Data)value, time, qulity);
                    return;
            }
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            Marshal.WriteByte(handle + mCount + mQulityAddr, (byte)qulity);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            mCount++;
        }

        /// <summary>
        /// 将UTC时间转换成本地时间
        /// </summary>
        public HisQueryResult<T> ConvertUTCTimeToLocal()
        {
            for(int i=0;i<mCount;i++)
            {
                MemoryHelper.WriteDateTime((void*)handle, i * 8 + mTimeAddr, MemoryHelper.ReadDateTime((void*)handle,i*8+mTimeAddr).ToLocalTime());
            }
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(bool value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }

            Marshal.WriteByte(handle + mPosition, (byte)(value ? 1 : 0));

          //  mDataBuffer[mPosition] = (byte)(value ? 1 : 0);
            mPosition++;
            MemoryHelper.WriteDateTime((void*)(handle),mCount * 8 + mTimeAddr, time);
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
          //  mDataBuffer[mCount + mQulityAddr] = qulity;
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(byte value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            Marshal.WriteByte(handle + mPosition, value);
            // mDataBuffer[mPosition] = value;
            mPosition++;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }


        public void Add(short value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteShort((void*)handle, mPosition, value);
            mPosition += 2;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        public void Add(ushort value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteUShort((void*)handle, mPosition, value);
            mPosition += 2;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            //   mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        public void Add(int value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteInt32((void*)handle, mPosition, value);
            mPosition += 4;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }


        public void Add(uint value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteUInt32((void*)handle, mPosition, value);
            mPosition += 4;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }


        public void Add(long value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteInt64((void*)handle, mPosition, value);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            //mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }


        public void Add(ulong value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteUInt64((void*)handle, mPosition, value);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(float value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteFloat((void*)handle, mPosition, value);
            mPosition += 4;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            //mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(double value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteDouble((void*)handle, mPosition, value);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(DateTime value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteDateTime((void*)handle, mPosition, value);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(string value, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            var sdata = Encoding.Unicode.GetBytes((string)value);
            MemoryHelper.WriteByte((void*)handle, mPosition, (byte)sdata.Length);
            mPosition++;
            Marshal.Copy(sdata, 0, handle + mPosition, sdata.Length);
            //System.Buffer.BlockCopy(sdata, 0, mDataBuffer, mPosition, sdata.Length);
            mPosition +=sdata.Length;

            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            //mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void AddPoint(int x,int y,DateTime time,byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteInt32((void*)handle, mPosition, x);
            mPosition += 4;
            MemoryHelper.WriteInt32((void*)handle, mPosition, y);
            mPosition += 4;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(IntPointData value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void AddPoint(int x, int y,int z, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteInt32((void*)handle, mPosition, x);
            mPosition += 4;
            MemoryHelper.WriteInt32((void*)handle, mPosition, y);
            mPosition += 4;
            MemoryHelper.WriteInt32((void*)handle, mPosition, z);
            mPosition += 4;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(IntPoint3Data value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y, value.Z, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void AddPoint(uint x, uint y, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteUInt32((void*)handle, mPosition, x);
            mPosition += 4;
            MemoryHelper.WriteUInt32((void*)handle, mPosition, y);
            mPosition += 4;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(UIntPointData value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void AddPoint(uint x, uint y, uint z, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteUInt32((void*)handle, mPosition, x);
            mPosition += 4;
            MemoryHelper.WriteUInt32((void*)handle, mPosition, y);
            mPosition += 4;
            MemoryHelper.WriteUInt32((void*)handle, mPosition, z);
            mPosition += 4;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(UIntPoint3Data value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y, value.Z, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void AddPoint(long x, long y, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteInt64((void*)handle, mPosition, x);
            mPosition += 8;
            MemoryHelper.WriteInt64((void*)handle, mPosition, y);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(LongPointData value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y,  time, qulity);
        }

        public void AddPoint(long x, long y, long z, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteInt64((void*)handle, mPosition, x);
            mPosition += 8;
            MemoryHelper.WriteInt64((void*)handle, mPosition, y);
            mPosition += 8;
            MemoryHelper.WriteInt64((void*)handle, mPosition, z);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(LongPoint3Data value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y, value.Z, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void AddPoint(ulong x, ulong y, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteUInt64((void*)handle, mPosition, x);
            mPosition += 8;
            MemoryHelper.WriteUInt64((void*)handle, mPosition, y);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(ULongPointData value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        /// <param name="z"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void AddPoint(ulong x, ulong y, ulong z, DateTime time, byte qulity)
        {
            if (mCount >= mLimite)
            {
                int newCount = (int)(mCount * 1.2);
                Resize(newCount);
                mLimite = newCount;
            }
            MemoryHelper.WriteUInt64((void*)handle, mPosition, x);
            mPosition += 8;
            MemoryHelper.WriteUInt64((void*)handle, mPosition, y);
            mPosition += 8;
            MemoryHelper.WriteUInt64((void*)handle, mPosition, z);
            mPosition += 8;
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            Marshal.WriteByte(handle + mCount + mQulityAddr, qulity);
            mCount++;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(ULongPoint3Data value, DateTime time, byte qulity)
        {
            AddPoint(value.X, value.Y, value.Z, time, qulity);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public T GetValue(int index)
        {
            object re = null;
            switch (mDataType)
            {
                case 0:
                    re = Convert.ToBoolean(MemoryHelper.ReadByte((void*)handle, index));
                    break;
                case 1:
                    re = MemoryHelper.ReadByte((void*)handle, index);
                    break;
                case 2:
                    re = MemoryHelper.ReadShort((void*)handle, index * 2);
                    break;
                case 3:
                    re = MemoryHelper.ReadShort((void*)handle, index * 2);
                    break;
                case 4:
                    re = MemoryHelper.ReadInt32((void*)handle, index * 4);
                    break;
                case 5:
                    re = MemoryHelper.ReadUInt32((void*)handle, index * 4);
                    break;
                case 6:
                    re = MemoryHelper.ReadInt64((void*)handle, index * 8);
                    break;
                case 7:
                    re = MemoryHelper.ReadUInt64((void*)handle, index * 8);
                    break;
                case 8:
                    re = MemoryHelper.ReadFloat((void*)handle, index * 4);
                    break;
                case 9:
                    re = MemoryHelper.ReadDouble((void*)handle, index * 8);
                    break;
                case 10:
                    re = MemoryHelper.ReadDateTime((void*)handle, index * 8);
                    break;
                case 11:

                    if(mStringCach!=null)
                    {
                        re =mStringCach[index];
                    }
                    else
                    {
                        mStringCach = new List<string>(mCount);
                        int cc = 0;
                        int pos = 0;
                        int len = 0;
                        while (cc<this.mCount)
                        {
                            len = MemoryHelper.ReadByte((void*)handle, pos);
                            mStringCach.Add(new string((sbyte*)handle, pos + 1, len, Encoding.Unicode));
                            pos += len + 1;
                            cc++;
                        }
                        re = mStringCach[index];
                    }
                    break;
                case 12:
                    var x = MemoryHelper.ReadInt32((void*)handle, index * 8);
                    var y = MemoryHelper.ReadInt32((void*)handle, index * 8 + 4);
                    re = new IntPointData(x, y);
                    break;
                case 13:
                    re = new UIntPointData(MemoryHelper.ReadUInt32((void*)handle, index * 8), MemoryHelper.ReadUInt32((void*)handle, index * 8 + 4));
                    break;
                case 14:
                    re = new IntPoint3Data(MemoryHelper.ReadInt32((void*)handle, index * 12), MemoryHelper.ReadInt32((void*)handle, index * 12 + 4), MemoryHelper.ReadInt32((void*)handle, index * 12 + 8));
                    break;
                case 15:
                    re = new UIntPoint3Data(MemoryHelper.ReadUInt32((void*)handle, index * 12), MemoryHelper.ReadUInt32((void*)handle, index * 12 + 4), MemoryHelper.ReadUInt32((void*)handle, index * 12 + 8));
                    break;
                case 16:
                    re = new LongPointData(MemoryHelper.ReadInt64((void*)handle, index * 16), MemoryHelper.ReadInt64((void*)handle, index * 16 + 8));
                    break;
                case 17:
                    re = new ULongPointData(MemoryHelper.ReadUInt64((void*)handle, index * 16), MemoryHelper.ReadUInt64((void*)handle, index * 16 + 8));
                    break;
                case 18:
                    re = new LongPoint3Data(MemoryHelper.ReadInt64((void*)handle, index * 24), MemoryHelper.ReadInt64((void*)handle, index * 24 + 8), MemoryHelper.ReadInt64((void*)handle, index * 24 + 16));
                    break;
                case 19:
                    re = new ULongPoint3Data(MemoryHelper.ReadUInt64((void*)handle, index * 24), MemoryHelper.ReadUInt64((void*)handle, index * 24 + 8), MemoryHelper.ReadUInt64((void*)handle, index * 24 + 16));
                    break;
            }
            return (T)re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public T GetValue(int index,out DateTime time,out byte qulity)
        {
            object re = null;
            switch (mDataType)
            {
                case 0:
                    re = Convert.ToBoolean(MemoryHelper.ReadByte((void*)handle, index));
                    break;
                case 1:
                    re = MemoryHelper.ReadByte((void*)handle, index);
                    break;
                case 2:
                    re = MemoryHelper.ReadShort((void*)handle, index*2);
                    break;
                case 3:
                    re = MemoryHelper.ReadUShort((void*)handle, index * 2);
                    break;
                case 4:
                    re = MemoryHelper.ReadInt32((void*)handle, index * 4);
                    break;
                case 5:
                    re = MemoryHelper.ReadUInt32((void*)handle, index * 4);
                    break;
                case 6:
                    re = MemoryHelper.ReadInt64((void*)handle, index * 8);
                    break;
                case 7:
                    re = MemoryHelper.ReadUInt64((void*)handle, index * 8);
                    break;
                case 8:
                    re = MemoryHelper.ReadFloat((void*)handle, index * 4);
                    break;
                case 9:
                    re = MemoryHelper.ReadDouble((void*)handle, index * 8);
                    break;
                case 10:
                    re = MemoryHelper.ReadDateTime((void*)handle, index * 8);
                    break;
                case 11:

                    if (mStringCach != null)
                    {
                         re = mStringCach[index];
                    }
                    else
                    {
                        mStringCach = new List<string>(mCount);
                        int cc = 0;
                        int pos = 0;
                        int len = 0;
                        while (cc < this.mCount)
                        {
                            len = MemoryHelper.ReadByte((void*)handle, pos);
                            mStringCach.Add(new string((sbyte*)handle, pos + 1, len, Encoding.Unicode));
                            pos += len + 1;
                            cc++;
                        }
                        re = mStringCach[index];
                    }

                    break;
                case 12:
                    var x = MemoryHelper.ReadInt32((void*)handle, index * 8);
                    var y = MemoryHelper.ReadInt32((void*)handle, index * 8+4);
                    re = new IntPointData(x, y);
                    break;
                case 13:
                    re = new UIntPointData(MemoryHelper.ReadUInt32((void*)handle, index * 8), MemoryHelper.ReadUInt32((void*)handle, index * 8 + 4));
                    break;
                case 14:
                    re = new IntPoint3Data(MemoryHelper.ReadInt32((void*)handle, index * 12), MemoryHelper.ReadInt32((void*)handle, index * 12 + 4), MemoryHelper.ReadInt32((void*)handle, index * 12 + 8));
                    break;
                case 15:
                    re = new UIntPoint3Data(MemoryHelper.ReadUInt32((void*)handle, index * 12), MemoryHelper.ReadUInt32((void*)handle, index * 12 + 4), MemoryHelper.ReadUInt32((void*)handle, index * 12 + 8));
                    break;
                case 16:
                    re = new LongPointData(MemoryHelper.ReadInt64((void*)handle, index * 16), MemoryHelper.ReadInt64((void*)handle, index * 16 + 8));
                    break;
                case 17:
                    re = new ULongPointData(MemoryHelper.ReadUInt64((void*)handle, index * 16), MemoryHelper.ReadUInt64((void*)handle, index * 16 + 8));
                    break;
                case 18:
                    re = new LongPoint3Data(MemoryHelper.ReadInt64((void*)handle, index * 24), MemoryHelper.ReadInt64((void*)handle, index * 24 + 8), MemoryHelper.ReadInt64((void*)handle, index * 24 + 16));
                    break;
                case 19:
                    re = new ULongPoint3Data(MemoryHelper.ReadUInt64((void*)handle, index * 24), MemoryHelper.ReadUInt64((void*)handle, index * 24 + 8), MemoryHelper.ReadUInt64((void*)handle, index * 24 + 16));
                    break;
            }

            time = MemoryHelper.ReadDateTime((void*)handle, index * 8 + mTimeAddr);
            qulity = MemoryHelper.ReadByte((void*)handle, mQulityAddr + index);

           

            return (T)re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public byte GetQuality(int index)
        {
            return MemoryHelper.ReadByte((void*)handle, mQulityAddr + index);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Resize(int count)
        {

            var newsize = count * (9 + mDataSize);
            mLimite = count > mLimite ? count : mLimite;

            if (newsize <= mSize)
            {
                return;
            }

            IntPtr nhd = Marshal.AllocHGlobal(newsize);

            var mTimeAddrn = count * mDataSize;
            var mQulityAddrn = count * (mDataSize + 8);

            Buffer.MemoryCopy((void*)handle, (void*)nhd, newsize, mSize);

            mTimeAddr = mTimeAddrn;
            mQulityAddr = mQulityAddrn;

            Marshal.FreeHGlobal(handle);
            handle = nhd;

            mSize = newsize;

            //mLimite = count;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int GetDataLen()
        {
            var sname = typeof(T).Name.ToLower();
            
            switch(sname)
            {
                case "boolean":
                    mDataType = 0;
                    return 1;
                case "byte":
                    mDataType = 1;
                    return 1;
                case "int16":
                    mDataType = 2;
                    return 2;
                case "uint16":
                    mDataType = 3;
                    return 2;
                case "int32":
                    mDataType = 4;
                    return 4;
                case "uint32":
                    mDataType = 5;
                    return 4;
                case "int64":
                    mDataType = 6;
                    return 8;
                case "uint64":
                    mDataType = 7;
                    return 8;
                case "single":
                    mDataType = 8;
                    return 4;
                case "double":
                    mDataType = 9;
                    return 8;
                case "datetime":
                    mDataType = 10;
                    return 8;
                case "string":
                    mDataType = 11;
                    return Const.StringSize;
                case "intpointdata":
                    mDataType = 12;
                    return 8;
                case "uintpointdata":
                    mDataType = 13;
                    return 8;
                case "intpoint3data":
                    mDataType = 14;
                    return 12;
                case "uintpoint3data":
                    mDataType = 15;
                    return 12;
                case "longpointdata":
                    mDataType = 16;
                    return 16;
                case "ulongpointdata":
                    mDataType = 17;
                    return 16;
                case "longpoint3data":
                    mDataType = 18;
                    return 24;
                case "ulongpoint3data":
                    mDataType = 19;
                    return 24;
            }
          
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            mPosition = 0;
            mCount = 0;

            //for (int i = 0; i < mLenght / zoreData.Length; i++)
            //{
            //    Marshal.Copy(zoreData, 0, (IntPtr)(handle+ i * zoreData.Length), zoreData.Length);
            //}

            Unsafe.InitBlockUnaligned((void*)handle, 0, (uint)mSize);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        public void CloneTo(HisQueryResult<T> target)
        {
            if (target.mLimite < this.mLimite) target.Resize(this.mLimite);

            Buffer.MemoryCopy((void*)handle, (void*)target.handle, mLimite, this.mSize);
            target.mCount = this.mCount;
            target.mPosition = this.mPosition;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            Marshal.FreeHGlobal(handle);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
