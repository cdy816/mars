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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 划分内存
    /// </summary>
    public unsafe class FixedMemoryBlock : IDisposable
    {

        #region ... Variables  ...

        ///// <summary>
        ///// 
        ///// </summary>
        //private byte[] mDataBuffer;

        private byte[] mBuffers;

        /// <summary>
        /// 
        /// </summary>
        private IntPtr mHandles;

        /// <summary>
        /// 
        /// </summary>
        private int mPosition = 0;

        //private long mUsedSize = 0;

        private long mAllocSize = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        /// <summary>
        /// 
        /// </summary>
        public FixedMemoryBlock()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public FixedMemoryBlock(long size)
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
        public byte[] Buffers
        {
            get
            {
                return mBuffers;
            }
            internal set
            {
                mBuffers = value;
            }
        }


        /// <summary>
        /// 是否繁忙
        /// </summary>
        public bool IsBusy { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public long Length
        {
            get
            {
                return mBuffers.Length;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int Position
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
        /// <param name="size"></param>
        private void Init(long size)
        {
            mBuffers = new byte[size];
            mHandles = System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers, 0);
            mAllocSize = size;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <returns></returns>
        //public  IEnumerable<MemoryStream> GetStream()
        //{
        //    foreach(var vv in mBuffers)
        //    {
        //        yield return new MemoryStream(vv);
        //    }
        //}

        /// <summary>
        /// 重新分配对象
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc(long size)
        {
            Init(size);
            GC.Collect();
        }

        /// <summary>
        /// 清空内存
        /// </summary>
        public void Clear()
        {
            mBuffers.AsSpan().Fill(0);
            
            LoggerService.Service.Info("FixedMemoryBlock", Name + " is clear !");
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
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteLong(int offset, long value)
        {
            MemoryHelper.WriteInt64((void*)mHandles, offset, value);
            Position = offset + 8;
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLongDirect(int offset, long value)
        {
            MemoryHelper.WriteInt64((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteULong(int offset, ulong value)
        {
            MemoryHelper.WriteUInt64((void*)mHandles, offset, value);
            Position = offset + 8;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteFloat(int offset, float value)
        {
            MemoryHelper.WriteFloat((void*)mHandles, offset, value);
            Position = offset + 4;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteDouble(int offset, double value)
        {
            MemoryHelper.WriteDouble((void*)mHandles, offset, value);
            Position = offset + 8;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public virtual void WriteBytes(int offset,byte[] values)
        {
            WriteBytes(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public virtual void WriteBytesDirect(int offset, byte[] values)
        {
            WriteBytesDirect(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 清空值
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public virtual void Clear(int offset,int len)
        {
            mBuffers.AsSpan<byte>(offset, len).Fill(0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public virtual void WriteBytes(int offset, byte[] values, int valueoffset, int len)
        {
            Array.Copy(values, valueoffset, mBuffers,offset , len);
            Position = offset + len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(int offset, byte[] values, int valueoffset, int len)
        {
            Array.Copy(values, valueoffset, mBuffers, offset, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteByte(int offset, byte value)
        {
            mBuffers[offset] = value;
            Position = offset + 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public  void WriteByteDirect(int offset, byte value)
        {
            mBuffers[offset] = value;
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
        public virtual void WriteDatetime(int offset, DateTime value)
        {
            MemoryHelper.WriteDateTime((void*)mHandles, offset, value);
            Position = offset + 8;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteInt(int offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandles, offset, value);
            Position = offset + 4;
        }


        public void WriteIntDirect(int offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteUInt(int offset, uint value)
        {
            MemoryHelper.WriteUInt32((void*)mHandles, offset, value);
            Position = offset + 4;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUIntDirect(int offset, uint value)
        {
            MemoryHelper.WriteUInt32((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteShort(int offset, short value)
        {
            MemoryHelper.WriteShort((void*)mHandles, offset, value);
            Position = offset + 2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShortDirect(int offset, short value)
        {
            MemoryHelper.WriteShort((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteUShort(int offset, ushort value)
        {
            MemoryHelper.WriteUShort((void*)mHandles, offset, value);
            Position = offset + 2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShortDirect(int offset, ushort value)
        {
            MemoryHelper.WriteUShort((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public virtual void WriteString(int offset, string value, Encoding encode)
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
        public void WriteStringDirect(int offset, string value, Encoding encode)
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
        public virtual DateTime ReadDateTime(int offset)
        {
            mPosition = offset + sizeof(DateTime);

            return MemoryHelper.ReadDateTime((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>

        public virtual int ReadInt(int offset)
        {
            mPosition = offset + sizeof(int);
            return MemoryHelper.ReadInt32((void*)mHandles, offset);
        }


        public virtual uint ReadUInt(int offset)
        {
            mPosition = offset + 4;
            return MemoryHelper.ReadUInt32((void*)mHandles, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual short ReadShort(int offset)
        {
            mPosition = offset + 2;
            return MemoryHelper.ReadShort((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual ushort ReadUShort(int offset)
        {
            mPosition = offset + 2;
            return MemoryHelper.ReadUShort((void*)mHandles, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual float ReadFloat(int offset)
        {
            mPosition = offset + 4;
            return MemoryHelper.ReadFloat((void*)mHandles, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual double ReadDouble(int offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadDouble((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual long ReadLong(int offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadInt64((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual ulong ReadULong(int offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadUInt64((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual byte ReadByte(int offset)
        {
            mPosition = offset + 1;
            return MemoryHelper.ReadByte((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual string ReadString(int offset, Encoding encoding)
        {
            var len = ReadByte(offset);
            mPosition = offset + len + 1;
            return new string((sbyte*)mHandles, (int)offset + 1, len, encoding);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual string ReadString(int offset)
        {
            return ReadString(offset, Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        private byte[] ReadBytesInner(int offset,int len)
        {
            byte[] re = new byte[len];
            Array.Copy(mBuffers, offset, re, 0, len);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual byte[] ReadBytes(int offset,int len)
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
        /// <returns></returns>
        public ulong ReadULong()
        {
            return ReadULong(mPosition);
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
        /// <returns></returns>
        public string ReadString()
        {
            return ReadString(mPosition, Encoding.Unicode);
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
            //mDataBuffer = null;
            mBuffers = null;
            LoggerService.Service.Erro("FixedMemoryBlock", Name + " Disposed ");
            GC.Collect();
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
                return this.mBuffers[index];
            }
            set
            {
                this.mBuffers[index] = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(FixedMemoryBlock target,long sourceStart, long targetStart, long len)
        {
            if (target == null) return;
            Array.Copy(mBuffers, sourceStart, target.mBuffers, targetStart, len);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class FixedMemoryBlockExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<long> ToLongList(this FixedMemoryBlock memory)
        {
            List<long> re = new List<long>((int)(memory.AllocSize / 8));
            memory.Position = 0;
            while (memory.Position < memory.AllocSize)
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
        public static List<int> ToIntList(this FixedMemoryBlock memory)
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
        public static List<double> ToDoubleList(this FixedMemoryBlock memory)
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
        public static List<double> ToFloatList(this FixedMemoryBlock memory)
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
        public static List<short> ToShortList(this FixedMemoryBlock memory)
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
        public static List<DateTime> ToDatetimeList(this FixedMemoryBlock memory)
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
        public static List<string> ToStringList(this FixedMemoryBlock memory, Encoding encoding)
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
        public static List<string> ToStringList(this FixedMemoryBlock memory,int offset, Encoding encoding)
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
        public static void MakeMemoryBusy(this FixedMemoryBlock memory)
        {
            LoggerService.Service.Info("FixedMemoryBlock", memory.Name + " is busy.....");
            memory.IsBusy = true;
            //memory.StartMemory[0] = 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this FixedMemoryBlock memory)
        {
            LoggerService.Service.Info("FixedMemoryBlock", memory.Name+ " is ready !");
            memory.IsBusy = false;
            //memory.StartMemory[0] = 0;
        }
    }
}
