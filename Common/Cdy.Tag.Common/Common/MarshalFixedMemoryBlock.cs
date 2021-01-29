//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 划分内存
    /// </summary>
    public unsafe class MarshalFixedMemoryBlock : IDisposable,IMemoryFixedBlock
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private IntPtr mHandles;

        private long mHandleValue;

        /// <summary>
        /// 
        /// </summary>
        private long mPosition = 0;

        //private long mUsedSize = 0;

        private long mAllocSize = 0;

        private long mSize = 0;

        //private object mUserSizeLock = new object();


        public static int BufferItemSize = 1024 * 4;

        //public static byte[] zoreData = new byte[BufferItemSize];

        private int mRefCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        /// <summary>
        /// 
        /// </summary>
        public MarshalFixedMemoryBlock()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="handle"></param>
        /// <param name="size"></param>
        public MarshalFixedMemoryBlock(IntPtr handle,long size)
        {
            mHandles = handle;
            mAllocSize = size;
            mSize = size;
            mHandleValue = mHandles.ToInt64();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public MarshalFixedMemoryBlock(long size)
        {
            Init(size);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public IntPtr Buffers
        {
            get
            {
                return mHandles;
            }
            internal set
            {
                mHandles = value;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public IntPtr StartMemory
        {
            get
            {
                return mHandles;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public long Length
        {
            get
            {
                return mSize;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public long Position
        {
            get
            {
                return mPosition;
            }
            set
            {
                mPosition = value;
                //lock (mUserSizeLock)
                //    mUsedSize = mUsedSize < value ? value : mUsedSize;
            }
        }


        ///// <summary>
        ///// 
        ///// </summary>
        //public long UsedSize
        //{
        //    get
        //    {
        //        return mUsedSize;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        public long AllocSize
        {
            get
            {
                return mAllocSize;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        internal IntPtr Handles
        {
            get
            {
                return mHandles;
            }
            set
            {
                mHandles = value;
            }

        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="handle"></param>
        /// <param name="size"></param>
        public void Reset(IntPtr handle,int size)
        {
            mPosition = 0;
            mHandles = handle;
            mAllocSize = size;
            mSize = size;
            mHandleValue = mHandles.ToInt64();
        }

        public void CheckAndResize(long size)
        {
            if (size > mAllocSize)
            {
                IntPtr moldptr = mHandles;
                long oldlen = mPosition;
                Init(size);

                Buffer.MemoryCopy((void*)moldptr, (void*)mHandles, size, oldlen);

                Marshal.FreeHGlobal(moldptr);
                LoggerService.Service.Info("CheckAndResize", "CheckAndResize " + this.Name + " " + size, ConsoleColor.Red);

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <param name="incpercent"></param>
        public void CheckAndResize(long size,double incpercent)
        {
            if (size > mAllocSize)
            {
                IntPtr moldptr = mHandles;
                long oldlen = mPosition;
                Init((int)(size*(1+ incpercent)));

                Buffer.MemoryCopy((void*)moldptr, (void*)mHandles, size, oldlen);

                Marshal.FreeHGlobal(moldptr);
                LoggerService.Service.Info("CheckAndResize", "CheckAndResize " + this.Name + " " + size, ConsoleColor.Red);

            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void IncRef()
        {
            lock (this)
                Interlocked.Increment(ref mRefCount);
        }

        /// <summary>
        /// 
        /// </summary>
        public void DecRef()
        {
            lock (this)
                mRefCount = mRefCount > 0 ? mRefCount - 1 : mRefCount;
        }

        /// <summary>
        /// 是否繁忙
        /// </summary>
        /// <returns></returns>
        public bool IsBusy()
        {
            return mRefCount > 0;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        private void Init(long size)
        {
            //long stmp = size % BufferItemSize > 0 ? ((size / BufferItemSize) + 1) * BufferItemSize : (size / BufferItemSize) * BufferItemSize;
            mSize = size;
            mHandles = Marshal.AllocHGlobal(new IntPtr(size));
            mHandleValue = mHandles.ToInt64();
            mAllocSize = size;
        }


        /// <summary>
        /// 重新分配对象
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc(long size)
        {
            Init(size);
            //GC.Collect();
        }





        /// <summary>
        /// 清空内存
        /// </summary>
        public IMemoryFixedBlock Clear()
        {
            //long i = 0;
            //for (i = 0; i < mSize / zoreData.Length; i++)
            //{
            //    Marshal.Copy(zoreData, 0, new IntPtr(mHandleValue + (i * zoreData.Length)), zoreData.Length);
            //}

            //int cc = (int)(mSize % zoreData.Length);
            //if (cc > 0)
            //    Marshal.Copy(zoreData, 0, new IntPtr(mHandleValue + (i * zoreData.Length)), cc);

            Unsafe.InitBlockUnaligned((void*)mHandleValue, 0, (uint)mSize);

            //mUsedSize = 0;
            mPosition = 0;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        protected void InitValue()
        {
            for(int i=0;i<mSize/8;i++)
            {
                Write(long.MaxValue);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        private void Clear(IntPtr target, long start, long len)
        {
            //int i = 0;
            //for (i = 0; i < len / zoreData.Length; i++)
            //{
            //    Marshal.Copy(zoreData, 0, new IntPtr(target.ToInt64()+ start + i * zoreData.Length), zoreData.Length);
            //}
            //long zz = len % zoreData.Length;
            //if (zz > 0)
            //{
            //    Marshal.Copy(zoreData, 0, new IntPtr(target.ToInt64() + start + i * zoreData.Length), (int)zz);
            //}
            Unsafe.InitBlockUnaligned((void*)(target + (int)start), 0, (uint)len);
        }

        #region ReadAndWrite



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(DateTime value)
        {
            WriteDatetime(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(long value)
        {
            WriteLong(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>

        public void Write(ulong value)
        {
            WriteULong(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(int value)
        {
            WriteInt(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(uint value)
        {
            WriteUInt(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(short value)
        {
            WriteShort(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(ushort value)
        {
            WriteUShort(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(float value)
        {
            WriteFloat(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(double value)
        {
            WriteDouble(mPosition, value);
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="encoding"></param>
        public void Write(string value,Encoding encoding)
        {
            WriteString(mPosition, value, encoding);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(string value)
        {
            WriteString(mPosition, value, Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(byte value)
        {
            WriteByte(mPosition, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(byte[] values)
        {
            WriteBytes(mPosition, values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public void Write(byte[] values,int offset,int len)
        {
            WriteBytes(mPosition, values,offset,len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(Memory<byte> values)
        {
            WriteMemory(mPosition, values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLong(long offset, long value)
        {
            MemoryHelper.WriteInt64((void*)mHandles, offset, value);
            Position = offset + 8;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLongDirect(long offset, long value)
        {
            MemoryHelper.WriteInt64((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteULongDirect(long offset, ulong value)
        {
            MemoryHelper.WriteUInt64((void*)mHandles, offset, value);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteULong(long offset, ulong value)
        {
            MemoryHelper.WriteUInt64((void*)mHandles, offset, value);
            Position = offset + 8;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteFloat(long offset, float value)
        {
            MemoryHelper.WriteFloat((void*)mHandles, offset, value);
            Position = offset + 4;

        }

        public void WriteFloatDirect(long offset, float value)
        {
            MemoryHelper.WriteFloat((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDouble(long offset, double value)
        {
            MemoryHelper.WriteDouble((void*)mHandles, offset, value);
            Position = offset + 8;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDoubleDirect(long offset, double value)
        {
            MemoryHelper.WriteDouble((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteMemory(long offset,Memory<byte> values)
        {
            WriteMemory(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytes(long offset,byte[] values)
        {
            WriteBytes(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytesDirect(long offset, byte[] values)
        {
            WriteBytesDirect(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 清空值
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public void Clear(long offset,long len)
        {

            Clear(mHandles, offset, (int)len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteMemory(long offset, Memory<byte> values, int valueoffset, int len)
        {
            Buffer.MemoryCopy((void*)((IntPtr)values.Pin().Pointer + valueoffset), (void*)(new IntPtr(mHandleValue+ offset)), mSize-offset, len);
            Position = offset + len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytes(long offset, byte[] values, int valueoffset, int len)
        {
            Marshal.Copy(values, valueoffset, (new IntPtr(mHandleValue + offset)), len);
            Position = offset + len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueOffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long offset,IntPtr values,int valueOffset,int len)
        {
           Buffer.MemoryCopy((void*)(values + valueOffset), (void*)((mHandleValue + offset)), mSize - offset, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long offset, byte[] values, int valueoffset, int len)
        {
            Marshal.Copy(values, valueoffset, (new IntPtr(mHandleValue + offset)), len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByte(long offset, byte value)
        {
            MemoryHelper.WriteByte((void*)mHandles, offset, value);
            Position = offset + 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByteDirect(long offset, byte value)
        {
            MemoryHelper.WriteByte((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteByte(byte value)
        {
            WriteByte(mPosition, value);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDatetime(long offset, DateTime value)
        {
            MemoryHelper.WriteDateTime((void*)mHandles, offset, value);
            Position = offset + 8;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteInt(long offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandles, offset, value);
            Position = offset + 4;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteIntDirect(long offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUInt(long offset, uint value)
        {
            MemoryHelper.WriteUInt32((void*)mHandles, offset, value);
            Position = offset + 4;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUIntDirect(long offset, uint value)
        {
            MemoryHelper.WriteUInt32((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShort(long offset, short value)
        {
            MemoryHelper.WriteShort((void*)mHandles, offset, value);
            Position = offset + 2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShortDirect(long offset, short value)
        {
            MemoryHelper.WriteShort((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShort(long offset, ushort value)
        {
            MemoryHelper.WriteUShort((void*)mHandles, offset, value);
            Position = offset + 2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShortDirect(long offset, ushort value)
        {
            MemoryHelper.WriteUShort((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public void WriteString(long offset, string value, Encoding encode)
        {
            var sdata = encode.GetBytes(value);
            WriteByte(offset, (byte)sdata.Length);
            WriteBytes(offset+1, sdata);
            Position = offset + sdata.Length + 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public void WriteStringDirect(long offset, string value, Encoding encode)
        {
            var sdata = encode.GetBytes(value);
            WriteByte(offset, (byte)sdata.Length);
            WriteBytes(offset + 1, sdata);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public DateTime ReadDateTime(long offset)
        {
            mPosition = offset + sizeof(DateTime);
            return MemoryHelper.ReadDateTime((void*)mHandles, offset);
        }

        public List<DateTime> ReadDateTimes(long offset, int count)
        {
            List<DateTime> re = new List<DateTime>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadDateTime(offset + 8 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public int ReadInt(long offset)
        {
            mPosition = offset + sizeof(int);
            return MemoryHelper.ReadInt32((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<int> ReadInts(long offset,int count)
        {
            List<int> re = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadInt(offset + 4 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public uint ReadUInt(long offset)
        {
            mPosition = offset + 4;
            return MemoryHelper.ReadUInt32((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<uint> ReadUInts(long offset, int count)
        {
            List<uint> re = new List<uint>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadUInt(offset + 4 * i));
            }
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public short ReadShort(long offset)
        {
            mPosition = offset + 2;
            return MemoryHelper.ReadShort((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ushort ReadUShort(long offset)
        {
            mPosition = offset + 2;
            return MemoryHelper.ReadUShort((void*)mHandles, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public float ReadFloat(long offset)
        {
            mPosition = offset + 4;
            return MemoryHelper.ReadFloat((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<float> ReadFloats(long offset, int count)
        {
            List<float> re = new List<float>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadFloat(offset + 4 * i));
            }
            return re;
        }

        //public T Read<T>(long offset)
        //{
        //    if (typeof(T) == typeof(double))
        //    {
                
        //        return ReadDouble(offset);
        //    }

        //    return  default(T);
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public double ReadDouble(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadDouble((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<double> ReadDoubles(long offset,int count)
        {
            List<double> re = new List<double>(count);
            for(int i=0;i<count;i++)
            {
                re.Add(ReadDouble(offset + 8 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public long ReadLong(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadInt64((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ulong ReadULong(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadUInt64((void*)mHandles, offset);
            //mPosition = offset + sizeof(long);
            //return MemoryHelper.ReadUInt64(handle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte ReadByte(long offset)
        {
            mPosition = offset + 1;
            return MemoryHelper.ReadByte((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset, Encoding encoding)
        {
            var len = ReadByte(offset);
            mPosition = offset + len + 1;
            return encoding.GetString(ReadBytesInner(offset + 1, len));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset)
        {
            return ReadString(offset, Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        private byte[] ReadBytesInner(long offset,int len)
        {
            byte[] re = new byte[len];
            Marshal.Copy(new IntPtr(mHandleValue + offset), re, 0, len);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="target"></param>
        /// <param name="len"></param>
        public void ReadBytes(long offset, byte[] target, int len)
        {
            Marshal.Copy(new IntPtr(mHandleValue + offset), target, 0, len);
            mPosition += len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte[] ReadBytes(long offset,int len)
        {
            byte[] re = ReadBytesInner(offset, len);
            mPosition += len;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public byte ReadByte()
        {
            return ReadByte(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long ReadLong()
        {
            return ReadLong(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<long> ReadLongs(long offset, int count)
        {
            List<long> re = new List<long>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadLong(offset + 8 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ulong ReadULong()
        {
            return ReadULong(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<ulong> ReadULongs(long offset, int count)
        {
            List<ulong> re = new List<ulong>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadULong(offset + 8 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int ReadInt()
        {
            return ReadInt(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public uint ReadUInt()
        {
            return ReadUInt(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public float ReadFloat()
        {
            return ReadFloat(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public double ReadDouble()
        {
            return ReadDouble(mPosition);
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public short ReadShort()
        {
            return ReadShort(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<short> ReadShorts(long offset, int count)
        {
            List<short> re = new List<short>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadShort(offset + 2 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ushort ReadUShort()
        {
            return ReadUShort(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<ushort> ReadUShorts(long offset, int count)
        {
            List<ushort> re = new List<ushort>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadUShort(offset + 2 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DateTime ReadDateTime()
        {
            return ReadDateTime(mPosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public string ReadString(Encoding encoding)
        {
            return ReadString(mPosition,encoding);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string ReadString()
        {
            return ReadString(mPosition, Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<string> ReadStrings(long offset, int count)
        {
            mPosition = offset;
            List<string> re = new List<string>(count);
            for(int i=0;i<count;i++)
            {
                re.Add(ReadString());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="len"></param>
        /// <returns></returns>
        public byte[] ReadBytes(int len)
        {
            return ReadBytes(mPosition, len);
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose()
        {
            Marshal.FreeHGlobal(mHandles);
            //LoggerService.Service.Info("MarshalFixedMemoryBlock", Name +" Disposed ",ConsoleColor.Red );
            //GC.Collect();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public byte this[long index]
        {
            get
            {
                return Marshal.ReadByte(new IntPtr(mHandles.ToInt64() + index));
            }
            set
            {
                Marshal.WriteByte(new IntPtr(mHandles.ToInt64() + index), value);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(IMemoryFixedBlock target,long sourceStart, long targetStart, long len)
        {
            if (target == null || !(target is MarshalFixedMemoryBlock)) return;

            Buffer.MemoryCopy((void*)(new IntPtr(this.mHandleValue + sourceStart)), (void*)(((target as MarshalFixedMemoryBlock).mHandleValue + targetStart)), target.Length-targetStart, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(IntPtr target, long sourceStart, long targetStart, long len)
        {
            if (target == null) return;

            Buffer.MemoryCopy((void*)(new IntPtr(this.mHandleValue + sourceStart)), (void*)(new IntPtr(target.ToInt64() + targetStart)), len, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(IMemoryBlock target, long sourceStart, long targetStart, long len)
        {
            if (target == null) return;

            long ostt;

            //计算从源数据需要读取数据块索引
            long targetAddr = targetStart;

            if (target is MarshalMemoryBlock)
            {
                var vtarget = target as MarshalMemoryBlock;
                //拷贝数据到目标数据块中
                var hdt = vtarget.RelocationAddressToArrayIndex(targetAddr, out ostt);
                if (ostt + len < vtarget.BufferItemSize)
                {
                    Buffer.MemoryCopy((void*)(sourceStart), (void*)(vtarget.Handles[hdt] + (int)ostt), vtarget.BufferItemSize - ostt, len);
                }
                else
                {
                    Buffer.MemoryCopy((void*)(sourceStart), (void*)(vtarget.Handles[hdt] + (int)ostt), vtarget.BufferItemSize - ostt, (vtarget.BufferItemSize - ostt));
                    var vcount = vtarget.BufferItemSize - ostt;
                    var count = len - vcount;
                    while (count > vtarget.BufferItemSize)
                    {
                        hdt++;
                        Buffer.MemoryCopy((void*)(sourceStart + vcount), (void*)(vtarget.Handles[hdt]), vtarget.BufferItemSize, vtarget.BufferItemSize);
                        count = len - vcount;
                        vcount += vtarget.BufferItemSize;
                    }
                    if (count > 0)
                    {
                        hdt++;
                        Buffer.MemoryCopy((void*)(sourceStart + vcount), (void*)(vtarget.Handles[hdt]), vtarget.BufferItemSize, (count));
                    }
                }
            }
            else
            {
                var vtarget = target as MemoryBlock;
                //拷贝数据到目标数据块中
                var hdt = vtarget.RelocationAddressToArrayIndex(targetAddr, out ostt);
                if (ostt + len < vtarget.BufferItemSize)
                {
                    Buffer.MemoryCopy((void*)(sourceStart), (void*)(vtarget.Handles[hdt] + (int)ostt), vtarget.BufferItemSize - ostt, len);
                }
                else
                {
                    Buffer.MemoryCopy((void*)(sourceStart), (void*)(vtarget.Handles[hdt] + (int)ostt), vtarget.BufferItemSize - ostt, (vtarget.BufferItemSize - ostt));
                    var vcount = vtarget.BufferItemSize - ostt;
                    var count = len - vcount;
                    while (count > vtarget.BufferItemSize)
                    {
                        hdt++;
                        Buffer.MemoryCopy((void*)(sourceStart + vcount), (void*)(vtarget.Handles[hdt]), vtarget.BufferItemSize, vtarget.BufferItemSize);
                        count = len - vcount;
                        vcount += vtarget.BufferItemSize;
                    }
                    if (count > 0)
                    {
                        hdt++;
                        Buffer.MemoryCopy((void*)(sourceStart + vcount), (void*)(vtarget.Handles[hdt]), vtarget.BufferItemSize, (count));
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public IMemoryFixedBlock WriteToStream(Stream stream,long offset,long len)
        {
            using (System.IO.UnmanagedMemoryStream mstream = new UnmanagedMemoryStream((byte*)this.Handles+offset, len))
            {
                mstream.CopyTo(stream);
            }
            return this;
        }


        public unsafe IMemoryFixedBlock WriteToStream(Stream stream)
        {
            using(System.IO.UnmanagedMemoryStream mstream = new UnmanagedMemoryStream((byte*)this.Handles,Length))
            {
                mstream.CopyTo(stream);
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="start"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public byte[] ToBytes(int start,int len)
        {
            using(var mm = new System.IO.MemoryStream(len))
            {
                WriteToStream(mm, 0, len);
                return mm.ToArray();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public string ReadStringByFixSize(long offset, Encoding encoding)
        {
            var len = ReadByte(offset);
            mPosition = offset + Const.StringSize;
            return encoding.GetString(ReadBytesInner(offset + 1, len));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string ReadStringByFixSize()
        {
            return ReadStringByFixSize(mPosition, Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<string> ReadStringsByFixSize(long offset, int count)
        {
            mPosition = offset;
            List<string> re = new List<string>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadStringByFixSize());
            }
            return re;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class MarshalFixedMemoryBlockExtends
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<long> ToLongList(this MarshalFixedMemoryBlock memory)
        {
            List<long> re = new List<long>((int)(memory.Length / 8));
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadLong());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<int> ToIntList(this MarshalFixedMemoryBlock memory)
        {
            List<int> re = new List<int>((int)(memory.Length / 4));
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadInt());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<double> ToDoubleList(this MarshalFixedMemoryBlock memory)
        {
            List<double> re = new List<double>((int)(memory.Length / 8));
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadDouble());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<double> ToFloatList(this MarshalFixedMemoryBlock memory)
        {
            List<double> re = new List<double>((int)(memory.Length / 4));
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadFloat());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<short> ToShortList(this MarshalFixedMemoryBlock memory)
        {
            List<short> re = new List<short>((int)(memory.Length / 2));
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadShort());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<DateTime> ToDatetimeList(this MarshalFixedMemoryBlock memory)
        {
            List<DateTime> re = new List<DateTime>((int)(memory.Length / 8));
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadDateTime());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<string> ToStringList(this MarshalFixedMemoryBlock memory, Encoding encoding)
        {
            List<string> re = new List<string>();
            memory.Position = 0;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadString(encoding));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="offset"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public static List<string> ToStringList(this MarshalFixedMemoryBlock memory,int offset, Encoding encoding)
        {
            List<string> re = new List<string>();
            memory.Position = offset;
            while (memory.Position < memory.Length)
            {
                re.Add(memory.ReadString(encoding));
            }
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryBusy(this MarshalFixedMemoryBlock memory)
        {
            memory.IncRef();
            //LoggerService.Service.Info("MemoryBlock","make "+ memory.Name + " is busy.....");
            //memory.IsBusy = true;
            
            //memory.StartMemory[0] = 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this MarshalFixedMemoryBlock memory)
        {
            memory.DecRef();
            //LoggerService.Service.Info("MemoryBlock", "make " + memory.Name+ " is ready !");
            
            //memory.StartMemory[0] = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public unsafe static MemoryMappedViewStream MapFileToMemoryForWrite(string file,long datasize)
        {
            var cfile = MemoryMappedFile.CreateFromFile(file, FileMode.OpenOrCreate,file,datasize);
            return cfile.CreateViewStream(0, datasize, MemoryMappedFileAccess.Write);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="fileName"></param>
        public static void Dump(this MarshalFixedMemoryBlock memory,string fileName)
        {
            using (var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                byte[] bvals = ArrayPool<byte>.Shared.Rent(1024);
                Array.Clear(bvals, 0, bvals.Length);
                for (long i = 0; i < memory.Length / bvals.Length; i++)
                {
                    Marshal.Copy(new IntPtr(memory.Handles.ToInt64() + i * bvals.Length), bvals, 0, bvals.Length);
                    stream.Write(bvals, 0, bvals.Length);
                }
                ArrayPool<byte>.Shared.Return(bvals);
                stream.Flush();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static void Dump(this MarshalFixedMemoryBlock memory, Stream stream)
        {
            byte[] bvals = ArrayPool<byte>.Shared.Rent(1024);
            Array.Clear(bvals, 0, bvals.Length);
            long i = 0;
            for (i = 0; i < memory.Length / bvals.Length; i++)
            {
                Marshal.Copy(new IntPtr(memory.Handles.ToInt64() + i * bvals.Length), bvals, 0, bvals.Length);
                stream.Write(bvals, 0, bvals.Length);
            }

            long cc = memory.Length % bvals.Length;

            Marshal.Copy(new IntPtr(memory.Handles.ToInt64() + i * bvals.Length), bvals, 0, (int)cc);
            stream.Write(bvals, 0, bvals.Length);
            ArrayPool<byte>.Shared.Return(bvals);
            stream.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static void RecordToLog(this MarshalFixedMemoryBlock memory, Stream stream)
        {
            int ls = 1024 * 1024 * 128;
            int stmp = 0;
            int ltmp = (int)memory.AllocSize;
            var source = memory.Handles;

             var bvals = ArrayPool<byte>.Shared.Rent(ls);
            Array.Clear(bvals, 0, bvals.Length);
            while (ltmp > 0)
            {
                int ctmp = Math.Min(bvals.Length, ltmp);
                Marshal.Copy(source + stmp, bvals, 0, ctmp);
                stream.Write(bvals, 0, ctmp);
                stmp += ctmp;
                ltmp -= ctmp;
            }
            ArrayPool<byte>.Shared.Return(bvals);
           // stream.Flush();
        }

        public static void RecordToLog2(this MarshalFixedMemoryBlock memory, Stream stream)
        {
            int ltmp = (int)memory.AllocSize;
            var source = memory.Handles;

            var bvals = ArrayPool<byte>.Shared.Rent(ltmp);
            Array.Clear(bvals, 0, bvals.Length);

            int ctmp = Math.Min(bvals.Length, ltmp);
            Marshal.Copy(source, bvals, 0, ctmp);
            stream.Write(bvals, 0, ctmp);
            ArrayPool<byte>.Shared.Return(bvals);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static void RecordToLog(this MarshalFixedMemoryBlock memory, IntPtr stream)
        {
            //byte[] bvals = new byte[1024];
            long totalsize = memory.AllocSize;
            //long csize = 0;
            memory.CopyTo(stream, 0, 0, totalsize);
            //for (long i = 0; i < memory.Length / 1024; i++)
            //{
            //Marshal.Copy(new IntPtr(memory.Handles.ToInt64() + i * 1024), bvals, 0, 1024);
            //    int isize = (int)Math.Min(totalsize - csize, 1024);
            //    csize += isize;
            //    stream.Write(bvals, 0, isize);
            //    if (csize >= totalsize)
            //        break;
            //}
            //stream.Flush();
        }

        public static void Dump(this MarshalFixedMemoryBlock memory,DateTime time)
        {
            string fileName = memory.Name + "_" + time.ToString("yyyy_MM_dd_HH_mm_ss") + ".dmp";
            fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalFixedMemoryBlock).Assembly.Location), fileName);
            Dump(memory, fileName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void Dump(this MarshalFixedMemoryBlock memory)
        {
            string fileName = memory.Name + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm_ss")+".dmp";
            fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalFixedMemoryBlock).Assembly.Location), fileName);
            Dump(memory, fileName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public static MarshalFixedMemoryBlock LoadDumpFromFile(this string file)
        {
            if(System.IO.File.Exists(file))
            {
                using (var stream = System.IO.File.OpenRead(file))
                {
                    MarshalFixedMemoryBlock block = new MarshalFixedMemoryBlock(stream.Length);
                    byte[] bvals = new byte[1024 * 64];
                    int len = 0;
                    do
                    {
                        len = stream.Read(bvals, 0, bvals.Length);
                        block.WriteBytes(block.Position, bvals, 0, len);
                    }
                    while (len > 0);
                }
            }
            return null;
        }
    }
}
