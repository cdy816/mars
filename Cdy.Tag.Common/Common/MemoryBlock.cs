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
    public unsafe class MemoryBlock:IDisposable
    {

        #region ... Variables  ...
        
        /// <summary>
        /// 
        /// </summary>
        private byte[] mDataBuffer;

        private List<byte[]> mBuffers;

        private List<IntPtr> mHandles;
        
        /// <summary>
        /// 
        /// </summary>
        private long mPosition = 0;

        public const int BufferItemSize = 1024 * 1024 * 500;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        /// <summary>
        /// 
        /// </summary>
        public MemoryBlock()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public MemoryBlock(long size)
        {
            int count = (int)(size / BufferItemSize) + 1;

            mBuffers = new List<byte[]>(count);
            for(int i=0;i<count;i++)
            {
                mBuffers.Add(new byte[BufferItemSize]);
            }

            mHandles = new List<IntPtr>();
            for (int i = 0; i < count; i++)
            {
                mHandles.Add(System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers[i], 0));
            }

            mDataBuffer = mBuffers[0];
           // handle = (void*)System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mDataBuffer, 0);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public byte[] StartMemory
        {
            get
            {
                return mBuffers[0];
            }
        }

        public byte[] EndMemory
        {
            get
            {
                return mBuffers[0];
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
                return mBuffers.Count* BufferItemSize;
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
        public  IEnumerable<MemoryStream> GetStream()
        {
            foreach(var vv in mBuffers)
            {
                yield return new MemoryStream(vv);
            }
        }

        /// <summary>
        /// 重新分配对象
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc(long size)
        {
            int count = (int)(size / BufferItemSize) + 1;
            mBuffers = new List<byte[]>(count);
            for (int i = 0; i < count; i++)
            {
                mBuffers.Add(new byte[BufferItemSize]);
            }
            for (int i = 0; i < count; i++)
            {
                mHandles.Add(System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers[i], 0));
            }
            GC.Collect();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void Resize(long size)
        {
            int count = (int)(size / BufferItemSize) + 1;
            List<byte[]> nBuffers = new List<byte[]>(count);

            if (count > mBuffers.Count)
            {
                int i = 0;
                for (i = 0; i < mBuffers.Count; i++)
                {
                    nBuffers[i] = mBuffers[i];
                }
                for(int j=i;j<count;j++)
                {
                    mBuffers[j] = new byte[BufferItemSize];
                }
            }
            else if (count < mBuffers.Count)
            {
                for(int i=0;i<count;i++)
                {
                    nBuffers[i] = mBuffers[i];
                }
            }

            mBuffers = nBuffers;
            mHandles = new List<IntPtr>();
            for (int i = 0; i < count; i++)
            {
                mHandles.Add(System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers[i], 0));
            }

            //var newMemory = new byte[size];
            //Buffer.BlockCopy(mDataBuffer, 0, newMemory, 0, Math.Min(mDataBuffer.Length, newMemory.Length));
            //mDataBuffer = newMemory;
            //handle = (void*)System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mDataBuffer, 0);
            GC.Collect();
        }

        /// <summary>
        /// 清空内存
        /// </summary>
        public void Clear()
        {
            foreach (var vv in mBuffers)
                Array.Clear(vv, 0, vv.Length);
        }

        #region ReadAndWrite

        /// <summary>
        /// 
        /// </summary>
        /// <param name="position"></param>
        /// <param name="size"></param>
        /// <param name="innerAddr"></param>
        /// <param name="offset"></param>
        private IntPtr RelocationAddress(long position, out long offset)
        {
            offset = position % BufferItemSize;
            int id = (int)(position / BufferItemSize);
            return mHandles[id];
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="position"></param>
        /// <param name="offset"></param>
        /// <returns></returns>
        private int RelocationAddressToArrayIndex(long position,out long offset)
        {
            offset = position % BufferItemSize;
            return (int)(position / BufferItemSize);
        }

        /// <summary>
        /// 数据是否跨数据块
        /// </summary>
        /// <param name="position"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private bool IsCrossDataBuffer(long position, int size)
        {
            return (position % BufferItemSize) + size >= BufferItemSize;
        }

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
        public void WriteLong(long offset, long value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset,8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
                mPosition = offset + 8;
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteInt64((void*)hd, ost, value);
            }
           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteULong(long offset, ulong value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
               
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUInt64((void*)hd, ost, value);
                mPosition = offset + 8;
            }
           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteFloat(long offset, float value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 4))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteFloat((void*)hd, ost, value);
                mPosition = offset + 4;
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDouble(long offset, double value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteDouble((void*)hd, ost, value);
                mPosition = offset + 8;
            }
            
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
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        private void WriteBytes(long offset, byte[] values, int valueoffset, int len)
        {
            int id = (int)(offset / BufferItemSize);

            long ost = offset % BufferItemSize;

            if (len + ost < BufferItemSize)
            {
                Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, len);
            }
            else
            {
                int ll = BufferItemSize - (int)ost;

                Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, ll);

                if (len - ll < BufferItemSize)
                {
                    id++;
                    Buffer.BlockCopy(values, ll+ valueoffset, mBuffers[id], 0, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / BufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Buffer.BlockCopy(values, valueoffset+ll + i * BufferItemSize, mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % BufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Buffer.BlockCopy(values, valueoffset+ll + i * BufferItemSize, mBuffers[id], 0, otmp);
                    }
                }
            }

            mPosition = offset + len;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByte(long offset, byte value)
        {
            IntPtr hd;
            long ost;
            hd = RelocationAddress(offset, out ost);
            MemoryHelper.WriteByte((void*)hd, ost, value);
            mPosition = offset + 1;
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
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytes(offset, MemoryHelper.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteDateTime((void*)hd, ost, value);
                mPosition = offset + 8;
            }
           

           
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteInt(long offset, int value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteInt32((void*)hd, ost, value);
                mPosition = offset + sizeof(int);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUInt(long offset, uint value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUInt32((void*)hd, ost, value);
                mPosition = offset + sizeof(uint);
            }            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShort(long offset, short value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteShort((void*)hd, ost, value);
                mPosition = offset + sizeof(short);
            }           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShort(long offset, ushort value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUShort((void*)hd, ost, value);
                mPosition = offset + sizeof(ushort);
            }          
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
            WriteBytes(offset, sdata);
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

            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                return MemoryHelper.ReadDateTime(ReadBytesInner(offset, 8));
               // WriteBytes(offset, MemoryHelper.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadDateTime((void*)hd, ost);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>

        public int ReadInt(long offset)
        {
            mPosition = offset + sizeof(int);
            if (IsCrossDataBuffer(offset, 4))
            {
                return  BitConverter.ToInt32(ReadBytesInner(offset, 4),0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadInt32((void*)hd, ost);
            }
        }


        public uint ReadUInt(long offset)
        {
            mPosition = offset + 4;
            if (IsCrossDataBuffer(offset, 4))
            {
                return BitConverter.ToUInt32(ReadBytesInner(offset, 4), 0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadUInt32((void*)hd, ost);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public short ReadShort(long offset)
        {
            mPosition = offset + 2;
            if (IsCrossDataBuffer(offset, 2))
            {
                return BitConverter.ToInt16(ReadBytesInner(offset, 2), 0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadShort((void*)hd, ost);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ushort ReadUShort(long offset)
        {
            mPosition = offset + 2;
            if (IsCrossDataBuffer(offset, 2))
            {
                return BitConverter.ToUInt16(ReadBytesInner(offset, 2), 0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadUShort((void*)hd, ost);
            }
            //return MemoryHelper.ReadUShort(handle, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public float ReadFloat(long offset)
        {
            mPosition = offset + 4;
            if (IsCrossDataBuffer(offset, 4))
            {
                return BitConverter.ToSingle(ReadBytesInner(offset, 4), 0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadFloat((void*)hd, ost);
            }
            //return MemoryHelper.ReadFloat(handle, offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public double ReadDouble(long offset)
        {
            mPosition = offset + 8;
            if (IsCrossDataBuffer(offset, 8))
            {
                return BitConverter.ToDouble(ReadBytesInner(offset, 8), 0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadDouble((void*)hd, ost);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public long ReadLong(long offset)
        {
            mPosition = offset + 8;
            if (IsCrossDataBuffer(offset, 8))
            {
                return BitConverter.ToInt64(ReadBytesInner(offset, 8), 0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadInt64((void*)hd, ost);
            }
            //mPosition = offset + sizeof(long);
            //return MemoryHelper.ReadInt64(handle, offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ulong ReadULong(long offset)
        {
            mPosition = offset + 8;
            if (IsCrossDataBuffer(offset, 8))
            {
                return BitConverter.ToUInt64(ReadBytesInner(offset, 8), 0);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return MemoryHelper.ReadUInt64((void*)hd, ost);
            }
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
            long ost;
            var hd = RelocationAddress(offset, out ost);
            return MemoryHelper.ReadByte((void*)hd, ost);
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
            if (IsCrossDataBuffer(offset + 1, len))
            {
                return encoding.GetString(ReadBytesInner(offset + 1, len));
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return new string((sbyte*)hd, (int)offset + 1, len, encoding);
            }
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
            int id = (int)(offset / BufferItemSize);

            long ost = offset % BufferItemSize;

            if (len + ost < BufferItemSize)
            {
                Buffer.BlockCopy(mBuffers[id], (int)ost,re, 0, len);
            }
            else
            {
                int ll = BufferItemSize - (int)ost;

                Buffer.BlockCopy(mBuffers[id], (int)ost,re,0, ll);

                if (len - ll < BufferItemSize)
                {
                    id++;
                    Buffer.BlockCopy(mBuffers[id], 0, re, ll, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / BufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Buffer.BlockCopy(mBuffers[id], 0, re, ll + i * BufferItemSize, BufferItemSize);
                    }
                    int otmp = ll % BufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Buffer.BlockCopy(mBuffers[id], 0,re, ll + i * BufferItemSize, otmp);
                    }
                }
            }

            return re;
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
        public void Dispose()
        {
            mDataBuffer = null;
            mBuffers.Clear();
            mHandles.Clear();
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
                long ost;
                var hd = RelocationAddressToArrayIndex(index, out ost);
                return this.mBuffers[hd][ost];
            }
            set
            {
                long ost;
                var hd = RelocationAddressToArrayIndex(index, out ost);
                this.mBuffers[hd][ost] = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targgetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(MemoryBlock target,int sourceStart,int targgetStart,int len)
        {
            if (target == null) return;

            long osts,ostt;
            
            var hds = RelocationAddressToArrayIndex(sourceStart, out osts);

            //计算从源数据需要读取数据块索引
            //Array Index,Array Address,Start Index,Data Len
            List<Tuple<int, int, int, int>> mSourceIndex = new List<Tuple<int, int, int, int>>();
            if(osts+len<BufferItemSize)
            {
                mSourceIndex.Add(new Tuple<int, int, int, int>(hds, (int)osts, 0, len));
            }
            else
            {
                int ll = BufferItemSize - (int)osts;

                mSourceIndex.Add(new Tuple<int, int, int, int>(hds, (int)osts, 0, ll));

                if (len - ll < BufferItemSize)
                {
                    hds++;
                    mSourceIndex.Add(new Tuple<int, int, int, int>(hds, 0, ll, len-len));
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / BufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, int, int>(hds, 0, ll + i * BufferItemSize, ll + BufferItemSize));
                    }
                    int otmp = ll % BufferItemSize;
                    if (otmp > 0)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, int, int>(hds, 0, ll + i * BufferItemSize, ll +  otmp));
                    }
                }
            }

            //拷贝数据到目标数据块中
            foreach(var vv in mSourceIndex)
            {
                var hdt = RelocationAddressToArrayIndex(vv.Item3, out ostt);
                if (ostt + vv.Item4 < BufferItemSize)
                {
                    Buffer.BlockCopy(this.mBuffers[vv.Item1], vv.Item2, target.mBuffers[hdt], (int)ostt, vv.Item4);
                }
                else
                {
                    var count = vv.Item4 + ostt - BufferItemSize;
                    Buffer.BlockCopy(this.mBuffers[vv.Item1], vv.Item2, target.mBuffers[hdt], (int)ostt, (int)(BufferItemSize - ostt));
                    hdt++;
                    Buffer.BlockCopy(this.mBuffers[vv.Item1], vv.Item2, target.mBuffers[hdt], (int)0, (int)count);
                }
            }
            //Buffer.BlockCopy(this.mDataBuffer, sourceStart, target.mDataBuffer, targgetStart, len);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class MemoryBlockExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<long> ToLongList(this MemoryBlock memory)
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
        public static List<int> ToIntList(this MemoryBlock memory)
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
        public static List<double> ToDoubleList(this MemoryBlock memory)
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
        public static List<double> ToFloatList(this MemoryBlock memory)
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
        public static List<short> ToShortList(this MemoryBlock memory)
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
        public static List<DateTime> ToDatetimeList(this MemoryBlock memory)
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
        public static List<string> ToStringList(this MemoryBlock memory, Encoding encoding)
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
        public static List<string> ToStringList(this MemoryBlock memory,int offset, Encoding encoding)
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
        public static void MakeMemoryBusy(this MemoryBlock memory)
        {
            memory.IsBusy = true;
            memory.StartMemory[0] = 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this MemoryBlock memory)
        {
            memory.IsBusy = false;
            memory.StartMemory[0] = 0;
        }
    }

}
