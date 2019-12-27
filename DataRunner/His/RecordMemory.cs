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
using System.IO;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 划分内存
    /// </summary>
    public unsafe class RecordMemory:IDisposable
    {

        #region ... Variables  ...
        
        /// <summary>
        /// 
        /// </summary>
        private byte[] mDataBuffer;

        /// <summary>
        /// 
        /// </summary>
        private void* handle;
        
        /// <summary>
        /// 
        /// </summary>
        private long mPosition = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        /// <summary>
        /// 
        /// </summary>
        public RecordMemory()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public RecordMemory(long size)
        {
            mDataBuffer = new byte[size];
            handle = mDataBuffer.AsMemory().Pin().Pointer;
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
                return mDataBuffer;
            }
        }

        /// <summary>
        /// 是否繁忙
        /// </summary>
        public bool IsBusy { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public int Length
        {
            get
            {
                return mDataBuffer.Length;
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

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public MemoryStream GetStream()
        {
            return new MemoryStream(mDataBuffer);
        }

        /// <summary>
        /// 重新分配对象
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc(long size)
        {
            mDataBuffer = new byte[size];
            GC.Collect();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void Resize(long size)
        {
            var newMemory = new byte[size];
            Buffer.BlockCopy(mDataBuffer, 0, newMemory, 0, Math.Min(mDataBuffer.Length, newMemory.Length));
            mDataBuffer = newMemory;
            GC.Collect();
        }

        /// <summary>
        /// 清空内存
        /// </summary>
        public void Clear()
        {
            Array.Clear(mDataBuffer, 0, mDataBuffer.Length);
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
        public void Write(int value)
        {
            WriteInt(mPosition, value);
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
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLong(long offset, long value)
        {
            MemoryHelper.WriteInt64(handle, offset, value);
            mPosition = offset + sizeof(long);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteFloat(long offset, float value)
        {
            MemoryHelper.WriteFloat(handle, offset, value);
            mPosition = offset + sizeof(float);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDouble(long offset, double value)
        {
            MemoryHelper.WriteDouble(handle, offset, value);
            mPosition = offset + sizeof(double);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytes(long offset,byte[] values)
        {
            mPosition = offset + values.Length;
            Buffer.BlockCopy(values, 0, this.mDataBuffer, (int)offset, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByte(long offset, byte value)
        {
            mPosition = offset + 1;
            this.mDataBuffer[offset] = value;
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
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytes(long offset, byte[] values,int valueoffset,int len)
        {
            mPosition = offset + len;
            Buffer.BlockCopy(values, valueoffset, this.mDataBuffer, (int)offset, len);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDatetime(long offset, DateTime value)
        {
            MemoryHelper.WriteDateTime(handle, offset, value);
            mPosition = offset + sizeof(DateTime);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteInt(long offset, int value)
        {
            MemoryHelper.WriteInt32(handle, offset, value);
            mPosition = offset + sizeof(int);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShort(long offset, short value)
        {
            MemoryHelper.WriteShort(handle, offset, value);
            mPosition = offset + sizeof(short);
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
            MemoryHelper.WriteByte(handle, offset, (byte)sdata.Length);
            System.Buffer.BlockCopy(sdata, 0, mDataBuffer, (int)offset + 1, sdata.Length);
            mPosition = offset + sdata.Length + 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public DateTime ReadDateTime(long offset)
        {
            mPosition = offset + sizeof(DateTime);
            return MemoryHelper.ReadDateTime(handle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>

        public int ReadInt(long offset)
        {
            mPosition = offset + sizeof(int);
            return MemoryHelper.ReadInt32(handle, offset);
        }


        public uint ReadUInt(long offset)
        {
            mPosition = offset + sizeof(int);
            return MemoryHelper.ReadUInt32(handle, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public short ReadShort(long offset)
        {
            mPosition = offset + sizeof(short);
            return MemoryHelper.ReadShort(handle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ushort ReadUShort(long offset)
        {
            mPosition = offset + sizeof(short);
            return MemoryHelper.ReadUShort(handle, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public float ReadFloat(long offset)
        {
            mPosition = offset + sizeof(float);
            return MemoryHelper.ReadFloat(handle, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public double ReadDouble(long offset)
        {
            mPosition = offset + sizeof(double);
            return MemoryHelper.ReadDouble(handle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public long ReadLong(long offset)
        {
            mPosition = offset + sizeof(long);
            return MemoryHelper.ReadInt64(handle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ulong ReadULong(long offset)
        {
            mPosition = offset + sizeof(long);
            return MemoryHelper.ReadUInt64(handle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte ReadByte(long offset)
        {
            mPosition = offset + 1;
            return mDataBuffer[offset];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset, Encoding encoding)
        {
            var len = MemoryHelper.ReadByte(handle, offset);
            mPosition = offset + len + 1;
            return new string((sbyte*)handle, (int)offset + 1, len, encoding);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte[] ReadBytes(long offset,int len)
        {
            byte[] re = new byte[len];
            mPosition = offset + len;
            Buffer.BlockCopy(mDataBuffer,(int)offset, re, 0, len);
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
        /// <returns></returns>
        public int ReadInt()
        {
            return ReadInt(mPosition);
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
        /// <returns></returns>
        public ushort ReadUShort()
        {
            return ReadUShort(mPosition);
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
        public void Dispose()
        {
            mDataBuffer = null;
            GC.Collect();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public byte this[int index]
        {
            get
            {
                return this.mDataBuffer[index];
            }
            set
            {
                this.mDataBuffer[index] = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        public void CopyTo(RecordMemory target,int sourceStart,int targgetStart,int len)
        {
            if (target == null) return;
            Buffer.BlockCopy(this.mDataBuffer, sourceStart, target.mDataBuffer, targgetStart, len);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class RecordMemoryExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<long> ToLongList(this RecordMemory memory)
        {
            List<long> re = new List<long>(memory.Length / 8);
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
        public static List<int> ToIntList(this RecordMemory memory)
        {
            List<int> re = new List<int>(memory.Length / 4);
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
        public static List<double> ToDoubleList(this RecordMemory memory)
        {
            List<double> re = new List<double>(memory.Length / 8);
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
        public static List<double> ToFloatList(this RecordMemory memory)
        {
            List<double> re = new List<double>(memory.Length / 4);
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
        public static List<short> ToShortList(this RecordMemory memory)
        {
            List<short> re = new List<short>(memory.Length / 2);
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
        public static List<DateTime> ToDatetimeList(this RecordMemory memory)
        {
            List<DateTime> re = new List<DateTime>(memory.Length / 8);
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
        public static List<string> ToStringList(this RecordMemory memory, Encoding encoding)
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
        public static List<string> ToStringList(this RecordMemory memory,int offset, Encoding encoding)
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
        public static void MakeMemoryBusy(this RecordMemory memory)
        {
            memory.IsBusy = true;
            memory.Memory[0] = 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this RecordMemory memory)
        {
            memory.IsBusy = false;
            memory.Memory[0] = 0;
        }
    }

}
