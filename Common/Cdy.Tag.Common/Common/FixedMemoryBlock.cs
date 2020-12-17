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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 划分内存
    /// </summary>
    public unsafe class FixedMemoryBlock : IDisposable, IMemoryFixedBlock
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
        private long mPosition = 0;

        //private long mUsedSize = 0;

        private long mAllocSize = 0;

        //private object mUserSizeLock = new object();

        private int mRefCount = 0;

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


        ///// <summary>
        ///// 是否繁忙
        ///// </summary>
        //public bool IsBusy { get; set; }


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
        public void IncRef()
        {
            lock (this)
            {
                mRefCount++;
            }
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
        public void CheckAndResize(long size)
        {
            if (size > mPosition)
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
        private void Init(long size)
        {
            mBuffers = new byte[size];
            mHandles = (IntPtr)mBuffers.AsMemory().Pin().Pointer;
            //mHandles = System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers, 0);
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
        }

        /// <summary>
        /// 清空内存
        /// </summary>
        public IMemoryFixedBlock Clear()
        {
            Array.Clear(this.Buffers, 0, Buffers.Length);
            return this;
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
        public virtual void WriteLong(long offset, long value)
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
        public virtual void WriteULong(long offset, ulong value)
        {
            MemoryHelper.WriteUInt64((void*)mHandles, offset, value);
            Position = offset + 8;

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
        public virtual void WriteFloat(long offset, float value)
        {
            MemoryHelper.WriteFloat((void*)mHandles, offset, value);
            Position = offset + 4;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteFloatDirect(long offset, float value)
        {
            MemoryHelper.WriteFloat((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteDouble(long offset, double value)
        {
            MemoryHelper.WriteDouble((void*)mHandles, offset, value);
            Position = offset + 8;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteDoubleDirect(long offset, double value)
        {
            MemoryHelper.WriteDouble((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public virtual void WriteBytes(long offset,byte[] values)
        {
            WriteBytes(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public virtual void WriteBytesDirect(long offset, byte[] values)
        {
            WriteBytesDirect(offset, values, 0, values.Length);
        }

        /// <summary>
        /// 清空值
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public virtual void Clear(long offset,int len)
        {
            mBuffers.AsSpan<byte>((int)offset, len).Fill(0);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public virtual void WriteBytes(long offset, byte[] values, int valueoffset, int len)
        {
            Array.Copy(values, valueoffset, mBuffers,offset , len);
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
            Buffer.MemoryCopy((void*)(values + valueOffset), (void*)((this.Handles + (int)offset)), this.Length - offset, len);
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
            Array.Copy(values, valueoffset, mBuffers, offset, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteByte(long offset, byte value)
        {
            mBuffers[offset] = value;
            Position = offset + 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public  void WriteByteDirect(long offset, byte value)
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
        public virtual void WriteDatetime(long offset, DateTime value)
        {
            MemoryHelper.WriteDateTime((void*)mHandles, offset, value);
            Position = offset + 8;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteInt(long offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandles, offset, value);
            Position = offset + 4;
        }


        public void WriteIntDirect(long offset, int value)
        {
            MemoryHelper.WriteInt32((void*)mHandles, offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public virtual void WriteUInt(long offset, uint value)
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
        public virtual void WriteShort(long offset, short value)
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
        public virtual void WriteUShort(long offset, ushort value)
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
        public virtual void WriteString(long offset, string value, Encoding encode)
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
        public virtual DateTime ReadDateTime(long offset)
        {
            mPosition = offset + sizeof(DateTime);

            return MemoryHelper.ReadDateTime((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>

        public virtual int ReadInt(long offset)
        {
            mPosition = offset + sizeof(int);
            return MemoryHelper.ReadInt32((void*)mHandles, offset);
        }


        public virtual uint ReadUInt(long offset)
        {
            mPosition = offset + 4;
            return MemoryHelper.ReadUInt32((void*)mHandles, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual short ReadShort(long offset)
        {
            mPosition = offset + 2;
            return MemoryHelper.ReadShort((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual ushort ReadUShort(long offset)
        {
            mPosition = offset + 2;
            return MemoryHelper.ReadUShort((void*)mHandles, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual float ReadFloat(long offset)
        {
            mPosition = offset + 4;
            return MemoryHelper.ReadFloat((void*)mHandles, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual double ReadDouble(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadDouble((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual long ReadLong(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadInt64((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual ulong ReadULong(long offset)
        {
            mPosition = offset + 8;
            return MemoryHelper.ReadUInt64((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual byte ReadByte(long offset)
        {
            mPosition = offset + 1;
            return MemoryHelper.ReadByte((void*)mHandles, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual string ReadString(long offset, Encoding encoding)
        {
            var len = ReadByte(offset);
            mPosition = offset + len + 1;
            return new string((sbyte*)mHandles, (int)offset + 1, len, encoding);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public virtual string ReadStringByFixSize(long offset,Encoding encoding)
        {
            var len = ReadByte(offset);
            mPosition = offset + Const.StringSize;
            return new string((sbyte*)mHandles, (int)offset + 1, len, encoding);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual string ReadString(long offset)
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
            Array.Copy(mBuffers, offset, re, 0, len);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public virtual byte[] ReadBytes(long offset,int len)
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
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<short> ReadShorts(long offset, int count)
        {
            List<short> re = new List<short>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadShort((int)offset + 2 * i));
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
                re.Add(ReadUShort((int)offset + 2 * i));
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
            mPosition = (int)offset;
            List<string> re = new List<string>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadString());
            }
            return re;
        }

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
            mPosition = (int)offset;
            List<string> re = new List<string>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadStringByFixSize());
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
            //mDataBuffer = null;
            mBuffers = null;
            //LoggerService.Service.Erro("FixedMemoryBlock", Name + " Disposed ");
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(IMemoryFixedBlock target, long sourceStart, long targetStart, long len)
        {
            CopyTo(target as FixedMemoryBlock, sourceStart, targetStart, len);
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
                    Marshal.Copy(this.Buffers, (int)sourceStart, (vtarget.Handles[hdt] + (int)ostt), (int)len);
                    // Buffer.MemoryCopy((void*)(sourceStart), (void*)(target.Handles[hdt] + (int)ostt), this.Length - ostt, len);
                }
                else
                {
                    Marshal.Copy(this.Buffers, (int)sourceStart, (vtarget.Handles[hdt] + (int)ostt), (int)(vtarget.BufferItemSize - ostt));
                    // Buffer.MemoryCopy((void*)(sourceStart), (void*)(target.Handles[hdt] + (int)ostt), this.Length - ostt, (this.Length - ostt));
                    var vcount = vtarget.BufferItemSize - ostt;
                    var count = len - vcount;
                    while (count > vtarget.BufferItemSize)
                    {
                        hdt++;
                        Marshal.Copy(this.Buffers, (int)(sourceStart + vcount), (vtarget.Handles[hdt]), (int)(vtarget.BufferItemSize));
                        //Buffer.MemoryCopy((void*)(sourceStart + vcount), (void*)(target.Handles[hdt]), target.BufferItemSize, target.BufferItemSize);
                        count = len - vcount;
                        vcount += vtarget.BufferItemSize;
                    }
                    if (count > 0)
                    {
                        hdt++;
                        Marshal.Copy(this.Buffers, (int)(sourceStart + vcount), (vtarget.Handles[hdt]), (int)count);
                        // Buffer.MemoryCopy((void*)(sourceStart + vcount), (void*)(target.Handles[hdt]), target.BufferItemSize, (count));
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
                    Marshal.Copy(this.Buffers, (int)sourceStart, (vtarget.Handles[hdt] + (int)ostt), (int)len);
                }
                else
                {
                    Marshal.Copy(this.Buffers, (int)sourceStart, (vtarget.Handles[hdt] + (int)ostt), (int)(vtarget.BufferItemSize - ostt));
                    var vcount = vtarget.BufferItemSize - ostt;
                    var count = len - vcount;
                    while (count > vtarget.BufferItemSize)
                    {
                        hdt++;
                        Marshal.Copy(this.Buffers, (int)(sourceStart + vcount), (vtarget.Handles[hdt]), (int)(vtarget.BufferItemSize));
                        count = len - vcount;
                        vcount += vtarget.BufferItemSize;
                    }
                    if (count > 0)
                    {
                        hdt++;
                        Marshal.Copy(this.Buffers, (int)(sourceStart + vcount), (vtarget.Handles[hdt]), (int)count);
                    }
                }
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
            using (var vpp = values.Pin())
            {
                Marshal.Copy((IntPtr)vpp.Pointer + valueoffset, this.Buffers, (int)offset, len);
            }
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
        /// <param name="values"></param>
        public void Write(Memory<byte> values)
        {
            WriteMemory(mPosition, values);
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

        public List<int> ReadInts(long offset, int count)
        {
            List<int> re = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadInt(offset + 4 * i));
            }
            return re;
        }

        public List<uint> ReadUInts(long offset, int count)
        {
            List<uint> re = new List<uint>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadUInt(offset + 4 * i));
            }
            return re;
        }

        public List<float> ReadFloats(long offset, int count)
        {
            List<float> re = new List<float>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadFloat(offset + 4 * i));
            }
            return re;
        }

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
        /// <param name="target"></param>
        /// <param name="len"></param>
        public void ReadBytes(long offset, byte[] target, int len)
        {
            Marshal.Copy((mHandles + (int)offset), target, 0, len);
            mPosition += len;
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

        public List<ulong> ReadULongs(long offset, int count)
        {
            List<ulong> re = new List<ulong>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadULong(offset + 8 * i));
            }
            return re;
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
        public static List<string> ToStringList(this FixedMemoryBlock memory,long offset, Encoding encoding)
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
        /// <param name="stream"></param>
        public static void RecordToLog(this FixedMemoryBlock memory, Stream stream)
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

                Array.Copy(memory.Buffers,stmp, bvals,0, ctmp);
               // Marshal.Copy(source + stmp, bvals, 0, ctmp);
                stream.Write(bvals, 0, ctmp);
                stmp += ctmp;
                ltmp -= ctmp;
            }
            ArrayPool<byte>.Shared.Return(bvals);
            // stream.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static void RecordToLog2(this FixedMemoryBlock memory, Stream stream)
        {
            stream.Write(memory.Buffers, 0, memory.Buffers.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="fileName"></param>
        public static void Dump(this FixedMemoryBlock memory, string fileName)
        {
            using (var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                stream.Write(memory.Buffers, 0, memory.Buffers.Length);
                stream.Flush();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static void Dump(this FixedMemoryBlock memory, Stream stream)
        {
            stream.Write(memory.Buffers,0, memory.Buffers.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="time"></param>
        public static void Dump(this FixedMemoryBlock memory, DateTime time)
        {
            string fileName = memory.Name + "_" + time.ToString("yyyy_MM_dd_HH_mm_ss") + ".dmp";
            fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalFixedMemoryBlock).Assembly.Location), fileName);
            Dump(memory, fileName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void Dump(this FixedMemoryBlock memory)
        {
            string fileName = memory.Name + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm_ss") + ".dmp";
            fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalFixedMemoryBlock).Assembly.Location), fileName);
            Dump(memory, fileName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryBusy(this FixedMemoryBlock memory)
        {
            //LoggerService.Service.Info("FixedMemoryBlock", memory.Name + " is busy.....");
            memory.IncRef();
            //memory.StartMemory[0] = 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this FixedMemoryBlock memory)
        {
            //LoggerService.Service.Info("FixedMemoryBlock", memory.Name+ " is ready !");
            memory.DecRef();
            //memory.StartMemory[0] = 0;
        }
    }
}
