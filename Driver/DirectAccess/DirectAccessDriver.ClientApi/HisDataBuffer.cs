﻿//==============================================================
//  Copyright (C) 2022  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/2 11:00:57.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace DirectAccessDriver.ClientApi
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe class HisDataBuffer : IDisposable
    {

        #region ... Variables  ...

        private IntPtr mHandle;

        public const int BufferItemSize = 1024 * 4;

        public static byte[] zoreData = new byte[BufferItemSize];

        private long mAllocSize = 0;

        private long mSize = 0;

        private long mPosition = 0;

        private long mHandleValue;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public HisDataBuffer() : this(BufferItemSize)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public HisDataBuffer(int size)
        {
            Init(size);
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public IntPtr Buffers
        {
            get
            {
                return mHandle;
            }
            internal set
            {
                mHandle = value;
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
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int ValueCount { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void CheckAndResize(long size)
        {
            if (size > mAllocSize)
            {
                IntPtr moldptr = mHandle;
                long oldlen = mAllocSize;
                long newsize = mAllocSize + (size - mAllocSize) * 10;
                Init(newsize);
                Buffer.MemoryCopy((void*)moldptr, (void*)mHandle, newsize, oldlen);
                Marshal.FreeHGlobal(moldptr);
                mAllocSize = newsize;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        private void Init(long size)
        {
            mSize = size;
            mHandle = Marshal.AllocHGlobal(new IntPtr(size));
            mHandleValue = mHandle.ToInt64();
            mAllocSize = size;
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
        public void Write(string value, Encoding encoding)
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
        public void Write(byte[] values, int offset, int len)
        {
            WriteBytes(mPosition, values, offset, len);
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
            MemoryHelper.WriteInt64((void*)mHandle, offset, value);
            Position = offset + 8;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLongDirect(long offset, long value)
        {
            MemoryHelper.WriteInt64((void*)mHandle, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteULong(long offset, ulong value)
        {
            MemoryHelper.WriteUInt64((void*)mHandle, offset, value);
            Position = offset + 8;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteFloat(long offset, float value)
        {
            MemoryHelper.WriteFloat((void*)mHandle, offset, value);
            Position = offset + 4;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDouble(long offset, double value)
        {
            MemoryHelper.WriteDouble((void*)mHandle, offset, value);
            Position = offset + 8;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteMemory(long offset, Memory<byte> values)
        {
            WriteMemory(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytes(long offset, byte[] values)
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
        public void Clear(long offset, long len)
        {
            Clear(mHandle, offset, (int)len);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            Clear(mHandle, 0, (int)this.mAllocSize);
            ValueCount = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        private void Clear(IntPtr target, long start, long len)
        {
            int i = 0;
            for (i = 0; i < len / zoreData.Length; i++)
            {
                Marshal.Copy(zoreData, 0, new IntPtr(target.ToInt64() + start + i * zoreData.Length), zoreData.Length);
            }
            long zz = len % zoreData.Length;
            if (zz > 0)
            {
                Marshal.Copy(zoreData, 0, new IntPtr(target.ToInt64() + start + i * zoreData.Length), (int)zz);
            }
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
            Buffer.MemoryCopy((void*)((IntPtr)values.Pin().Pointer + valueoffset), (void*)(new IntPtr(mHandleValue + offset)), mSize - offset, len);
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
        public void WriteBytesDirect(long offset, IntPtr values, int valueOffset, int len)
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
            MemoryHelper.WriteByte((void*)mHandle, offset, value);
            Position = offset + 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByteDirect(long offset, byte value)
        {
            MemoryHelper.WriteByte((void*)mHandle, offset, value);
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
            MemoryHelper.WriteDateTime((void*)mHandle, offset, value);
            Position = offset + 8;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteInt(long offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandle, offset, value);
            Position = offset + 4;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteIntDirect(long offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandle, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUInt(long offset, uint value)
        {
            MemoryHelper.WriteUInt32((void*)mHandle, offset, value);
            Position = offset + 4;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUIntDirect(long offset, uint value)
        {
            MemoryHelper.WriteUInt32((void*)mHandle, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShort(long offset, short value)
        {
            MemoryHelper.WriteShort((void*)mHandle, offset, value);
            Position = offset + 2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShortDirect(long offset, short value)
        {
            MemoryHelper.WriteShort((void*)mHandle, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShort(long offset, ushort value)
        {
            MemoryHelper.WriteUShort((void*)mHandle, offset, value);
            Position = offset + 2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShortDirect(long offset, ushort value)
        {
            MemoryHelper.WriteUShort((void*)mHandle, offset, value);
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
            WriteInt(offset, sdata.Length);
            WriteBytes(offset + 1, sdata);
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
            WriteInt(offset, sdata.Length);
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
            return MemoryHelper.ReadDateTime((void*)mHandle, offset);
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
            return MemoryHelper.ReadInt32((void*)mHandle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<int> ReadInts(long offset, int count)
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
            return MemoryHelper.ReadUInt32((void*)mHandle, offset);
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
            return MemoryHelper.ReadShort((void*)mHandle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ushort ReadUShort(long offset)
        {
            mPosition = offset + 2;
            return MemoryHelper.ReadUShort((void*)mHandle, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public float ReadFloat(long offset)
        {
            mPosition = offset + 4;
            return MemoryHelper.ReadFloat((void*)mHandle, offset);
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


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public double ReadDouble(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadDouble((void*)mHandle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<double> ReadDoubles(long offset, int count)
        {
            List<double> re = new List<double>(count);
            for (int i = 0; i < count; i++)
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
            return MemoryHelper.ReadInt64((void*)mHandle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ulong ReadULong(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadUInt64((void*)mHandle, offset);
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
            return MemoryHelper.ReadByte((void*)mHandle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset, Encoding encoding)
        {
            var len = ReadInt(offset);
            mPosition = offset + len + 4;
            return encoding.GetString(ReadBytesInner(offset + 4, len));
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
        private byte[] ReadBytesInner(long offset, int len)
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
        public byte[] ReadBytes(long offset, int len)
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
            return ReadString(mPosition, encoding);
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
            for (int i = 0; i < count; i++)
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
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, double value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 17);
            this.WriteLong(Position, time.ToBinary());
            this.WriteDouble(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, double value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 22);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.Double);
            this.WriteDouble(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, float value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 13);
            this.WriteLong(Position, time.ToBinary());
            this.WriteFloat(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, float value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 18);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.Float);
            this.WriteFloat(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, int value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 13);
            this.WriteLong(Position, time.ToBinary());
            this.WriteInt(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, int value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 18);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.Int);
            this.WriteInt(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, uint value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 13);
            this.WriteLong(Position, time.ToBinary());
            this.WriteUInt(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, uint value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 18);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.UInt);
            this.WriteUInt(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, short value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 11);
            this.WriteLong(Position, time.ToBinary());
            this.WriteShort(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, short value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 16);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.Short);
            this.WriteShort(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, ushort value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 11);
            this.WriteLong(Position, time.ToBinary());
            this.WriteUShort(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, ushort value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 16);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.UShort);
            this.WriteUShort(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, byte value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 10);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, byte value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 15);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.Byte);
            this.WriteByte(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, long value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 17);
            this.WriteLong(Position, time.ToBinary());
            this.WriteLong(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, long value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 22);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.Long);
            this.WriteLong(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, ulong value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 17);
            this.WriteLong(Position, time.ToBinary());
            this.WriteULong(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, ulong value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 22);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.ULong);
            this.WriteULong(Position, value);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, DateTime value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 17);
            this.WriteLong(Position, time.ToBinary());
            this.WriteLong(Position, value.ToBinary());
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, DateTime value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 22);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.DateTime);
            this.WriteLong(Position, value.ToBinary());
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, string value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + value.Length + 9);
            this.WriteLong(Position, time.ToBinary());
            this.WriteString(Position, value, Encoding.Unicode);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, string value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + value.Length + 14);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.String);
            this.WriteString(Position, value, Encoding.Unicode);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, IntPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 17);
            this.WriteLong(Position, time.ToBinary());
            this.WriteInt(Position, value.X);
            this.WriteInt(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, IntPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 22);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.IntPoint);
            this.WriteInt(Position, value.X);
            this.WriteInt(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, UIntPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 17);
            this.WriteLong(Position, time.ToBinary());
            this.WriteUInt(Position, value.X);
            this.WriteUInt(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, UIntPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 22);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.UIntPoint);
            this.WriteUInt(Position, value.X);
            this.WriteUInt(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, IntPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 21);
            this.WriteLong(Position, time.ToBinary());
            this.WriteInt(Position, value.X);
            this.WriteInt(Position, value.Y);
            this.WriteInt(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, IntPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 26);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.IntPoint3);
            this.WriteInt(Position, value.X);
            this.WriteInt(Position, value.Y);
            this.WriteInt(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, UIntPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 21);
            this.WriteLong(Position, time.ToBinary());
            this.WriteUInt(Position, value.X);
            this.WriteUInt(Position, value.Y);
            this.WriteUInt(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, UIntPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 26);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.UIntPoint3);
            this.WriteUInt(Position, value.X);
            this.WriteUInt(Position, value.Y);
            this.WriteUInt(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, LongPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 25);
            this.WriteLong(Position, time.ToBinary());
            this.WriteLong(Position, value.X);
            this.WriteLong(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, LongPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 30);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.LongPoint);
            this.WriteLong(Position, value.X);
            this.WriteLong(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, ULongPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 25);
            this.WriteLong(Position, time.ToBinary());
            this.WriteULong(Position, value.X);
            this.WriteULong(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, ULongPointData value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 29);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.ULongPoint);
            this.WriteULong(Position, value.X);
            this.WriteULong(Position, value.Y);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, LongPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 33);
            this.WriteLong(Position, time.ToBinary());
            this.WriteLong(Position, value.X);
            this.WriteLong(Position, value.Y);
            this.WriteLong(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, LongPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 40);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.LongPoint3);
            this.WriteLong(Position, value.X);
            this.WriteLong(Position, value.Y);
            this.WriteLong(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public HisDataBuffer AppendValue(DateTime time, ULongPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 33);
            this.WriteLong(Position, time.ToBinary());
            this.WriteULong(Position, value.X);
            this.WriteULong(Position, value.Y);
            this.WriteULong(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        public HisDataBuffer AppendValue(int id, DateTime time, ULongPoint3Data value, byte quality)
        {
            ValueCount++;
            CheckAndResize(Position + 38);
            this.WriteInt(Position, id);
            this.WriteLong(Position, time.ToBinary());
            this.WriteByte(Position, (byte)TagType.ULongPoint3);
            this.WriteULong(Position, value.X);
            this.WriteULong(Position, value.Y);
            this.WriteULong(Position, value.Z);
            this.WriteByte(Position, quality);
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            try
            {
                if (mHandle != IntPtr.Zero)
                    Marshal.FreeHGlobal(mHandle);
            }
            catch
            {

            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
