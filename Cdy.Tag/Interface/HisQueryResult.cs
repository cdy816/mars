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

        private int mLenght = 0;

        private int mDataSize = 0;

        private byte mDataType = 0;

        private int mPosition = 0;

        private int mCount = 0;

        private IntPtr handle;

        private int mLimite = 0;

        public static byte[] zoreData = new byte[1024 * 10];

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
        public int Count
        {
            get
            {
                return mCount;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int BufferSize
        {
            get
            {
                return mLenght;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

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
            mLenght = count;
            //handle = mDataBuffer.AsMemory().Pin().Pointer;
            handle = Marshal.AllocHGlobal(csize);
           // handle = (void*)System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mDataBuffer, 0);
            mLimite = count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add<T>(T value,DateTime time,byte qulity)
        {
            Add((object)value, time, qulity);
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
                case 1:
                    Marshal.WriteByte(handle+mPosition, (byte)value);
                    //mDataBuffer[mPosition] = (byte)value;
                    mPosition++;
                    break;
                case 2:
                    MemoryHelper.WriteShort((void*)handle, mPosition, (short)value);
                    mPosition+=2;
                    break;
                case 3:
                    MemoryHelper.WriteUShort((void*)handle, mPosition, (ushort)value);
                    mPosition += 2;
                    break;
                case 4:
                    MemoryHelper.WriteInt32((void*)handle, mPosition, (int)value);
                    mPosition += 4;
                    break;
                case 5:
                    MemoryHelper.WriteUInt32((void*)handle, mPosition, (uint)value);
                    mPosition += 4;
                    break;
                case 6:
                    MemoryHelper.WriteInt64((void*)handle, mPosition, (long)value);
                    mPosition += 8;
                    break;
                case 7:
                    MemoryHelper.WriteUInt64((void*)handle, mPosition, (ulong)value);
                    mPosition += 8;
                    break;
                case 8:
                    MemoryHelper.WriteFloat((void*)handle, mPosition, (float)value);
                    mPosition += 4;
                    break;
                case 9:
                    MemoryHelper.WriteDouble((void*)handle, mPosition, (double)value);
                    mPosition += 8;
                    break;
                case 10:
                    MemoryHelper.WriteDateTime((void*)handle, mPosition, (DateTime)value);
                    mPosition += 8;
                    break;
                case 11:
                    var sdata = Encoding.Unicode.GetBytes((string)value);
                    MemoryHelper.WriteByte((void*)handle, mPosition, (byte)sdata.Length);
                    mPosition++;

                    Marshal.Copy(sdata, 0, handle+ mPosition, sdata.Length);
                    mPosition += sdata.Length;
                    break;
            }
            MemoryHelper.WriteDateTime((void*)handle, mCount * 8 + mTimeAddr, time);
            Marshal.WriteByte(handle + mCount + mQulityAddr, (byte)qulity);
            // mDataBuffer[mCount + mQulityAddr] = qulity;
            mCount++;
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
                    re = MemoryHelper.ReadShort((void*)handle, index * 2);
                    break;
                case 4:
                    re = MemoryHelper.ReadInt32((void*)handle, index * 4);
                    break;
                case 5:
                    re = MemoryHelper.ReadInt32((void*)handle, index * 4);
                    break;
                case 6:
                    re = MemoryHelper.ReadInt32((void*)handle, index * 8);
                    break;
                case 7:
                    re = MemoryHelper.ReadInt32((void*)handle, index * 8);
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

                    int cc = 0;
                    int pos = 0;
                    while(true)
                    {
                        if(cc>=index)
                        {
                            break;
                        }
                        //pos += (mDataBuffer[pos]+1);
                        pos += MemoryHelper.ReadByte((void*)handle, pos) + 1;
                        cc++;
                    }
                    re = new string((char*)handle, pos+1, MemoryHelper.ReadByte((void*)handle, pos));
                    break;
            }

            time = MemoryHelper.ReadDateTime((void*)handle, index * 8 + mTimeAddr);
            qulity = MemoryHelper.ReadByte((void*)handle, mQulityAddr + index);

           

            return (T)re;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Resize(int count)
        {

            var newsize = count * (9 + mDataSize);

            if (newsize == mLenght) return;

            IntPtr nhd = Marshal.AllocHGlobal(newsize);

            var mTimeAddrn = count * mDataSize;
            var mQulityAddrn = count * (mDataSize + 8);

            Buffer.MemoryCopy((void*)handle, (void*)nhd, newsize, mLenght);

            mTimeAddr = mTimeAddrn;
            mQulityAddr = mQulityAddrn;

            Marshal.FreeHGlobal(handle);
            handle = nhd;
            mLimite = count;

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

            for (int i = 0; i < mLenght / zoreData.Length; i++)
            {
                Marshal.Copy(zoreData, 0, (IntPtr)(handle+ i * zoreData.Length), zoreData.Length);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        public void CloneTo(HisQueryResult<T> target)
        {
            if (target.mLimite < this.mLimite) target.Resize(this.mLimite);

            Buffer.MemoryCopy((void*)handle, (void*)target.handle, mLimite, this.mLenght);
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
