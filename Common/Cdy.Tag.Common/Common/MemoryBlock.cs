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
    public unsafe class MemoryBlock : IDisposable, IMemoryBlock
    {

        #region ... Variables  ...

        ///// <summary>
        ///// 
        ///// </summary>
        //private byte[] mDataBuffer;

        private List<byte[]> mBuffers;

        /// <summary>
        /// 
        /// </summary>
        private List<IntPtr> mHandles;

        /// <summary>
        /// 
        /// </summary>
        private long mPosition = 0;

        //private long mUsedSize = 0;

        private long mAllocSize = 0;

        private object mUserSizeLock = new object();


        private int mBufferItemSize = 1024 * 1024 * 4;

        public static byte[] zoreData = new byte[1024 * 10];

        private bool mIsDisposed = false;

        private int mRefCount = 0;

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
        /// <param name="blockSize"></param>
        public MemoryBlock(long size, int blockSize)
        {
            mBufferItemSize = blockSize;
            Init(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public MemoryBlock(long size)
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
        public int BufferItemSize
        {
            get
            {
                return mBufferItemSize;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public List<byte[]> Buffers
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
        /// 
        /// </summary>
        public byte[] StartMemory
        {
            get
            {
                return mBuffers[0];
            }
        }

        /// <summary>
        /// 
        /// </summary>
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
        public long Length
        {
            get
            {
                return mBuffers.Count * (long)BufferItemSize;
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
        public List<IntPtr> Handles
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


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void IncRef()
        {
            lock (mUserSizeLock)
                Interlocked.Increment(ref mRefCount);
        }

        /// <summary>
        /// 
        /// </summary>
        public void DecRef()
        {
            lock (mUserSizeLock)
                mRefCount = mRefCount > 0 ? mRefCount - 1 : mRefCount;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        private void Init(long size)
        {
            int count = (int)(size / BufferItemSize);

            count = size % BufferItemSize > 0 ? count + 1 : count;


            mBuffers = new List<byte[]>(count);
            for (int i = 0; i < count; i++)
            {
                mBuffers.Add(new byte[BufferItemSize]);
            }

            mHandles = new List<IntPtr>();
            for (int i = 0; i < count; i++)
            {
                mHandles.Add(System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers[i], 0));
            }
            mAllocSize = size;
            //mDataBuffer = mBuffers[0];
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
            //int count = (int)(size / BufferItemSize) + 1;
            //mBuffers = new List<byte[]>(count);
            //for (int i = 0; i < count; i++)
            //{
            //    mBuffers.Add(new byte[BufferItemSize]);
            //}
            //for (int i = 0; i < count; i++)
            //{
            //    mHandles.Add(System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers[i], 0));
            //}
            Init(size);
            //GC.Collect();
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
                    nBuffers.Add(mBuffers[i]);
                }
                for(int j=i;j<count;j++)
                {
                    mBuffers.Add(new byte[BufferItemSize]);
                }
            }
            else if (count < mBuffers.Count)
            {
                for(int i=0;i<count;i++)
                {
                    nBuffers.Add(mBuffers[i]);
                }
            }
            
            mBuffers = nBuffers;
            mHandles = new List<IntPtr>();
            for (int i = 0; i < count; i++)
            {
                mHandles.Add(System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers[i], 0));
            }
            //GC.Collect();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void CheckAndResize(long size)
        {
            if(size>mAllocSize)
            {
                if (size > this.Length)
                {
                    int count = (int)(size / BufferItemSize);
                    count = size % BufferItemSize > 0 ? count + 1 : count;

                    if (mBuffers.Count < count)
                    {
                        List<byte[]> nBuffers = new List<byte[]>(count);
                        int i = 0;
                        for (i = 0; i < mBuffers.Count; i++)
                        {
                            nBuffers.Add(mBuffers[i]);
                            //nBuffers[i] = mBuffers[i];
                        }

                        for (int j = i; j < count; j++)
                        {
                            nBuffers.Add(new byte[BufferItemSize]);
                           // nBuffers[j] = new byte[BufferItemSize];
                        }

                        mBuffers = nBuffers;
                        mHandles = new List<IntPtr>();
                        for (i = 0; i < count; i++)
                        {
                            mHandles.Add(System.Runtime.InteropServices.Marshal.UnsafeAddrOfPinnedArrayElement(mBuffers[i], 0));
                        }
                        //GC.Collect();
                    }
                    mAllocSize = size;
                    //GC.Collect();

                    LoggerService.Service.Info("MemoryBlock", "CheckAndResize " + this.Name + " " + size, ConsoleColor.Red);
                }
                else
                {
                    mAllocSize = size;
                }

            }
        }

        /// <summary>
        /// 清空内存
        /// </summary>
        public IMemoryBlock Clear()
        {
            foreach (var vv in mBuffers)
            {
                if (mIsDisposed) break;
                Array.Clear(vv, 0, vv.Length);
            }
            mPosition = 0;
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        private void Clear(byte[] target,int start,int len)
        {
            int i = 0;
            for (i = 0; i < len / zoreData.Length; i++)
            {
                Buffer.BlockCopy(zoreData, 0, target, start + i * zoreData.Length, zoreData.Length);
            }
            int zz = len % zoreData.Length;
            if (zz > 0)
            {
                Buffer.BlockCopy(zoreData, 0, target, start+ i * zoreData.Length, zz);
            }
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
        public int RelocationAddressToArrayIndex(long position,out long offset)
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
            }
            else
            {
                CheckAndResize(offset + 8);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteInt64((void*)hd, ost, value);
                Position = offset + 8;
            }
           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLongDirect(long offset, long value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 8))
            {
                WriteBytesDirect(offset, BitConverter.GetBytes(value));
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
                CheckAndResize(offset + 8);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUInt64((void*)hd, ost, value);
                Position = offset + 8;
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
                CheckAndResize(offset + 4);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteFloat((void*)hd, ost, value);
                Position = offset + 4;
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
                CheckAndResize(offset + 8);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteDouble((void*)hd, ost, value);
                Position = offset + 8;
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
            int id = (int)(offset / BufferItemSize);

            long ost = offset % BufferItemSize;

            if (len + ost < BufferItemSize)
            {
                Clear(mBuffers[id], (int)ost, (int)len);
               // System.Array.Clear(mBuffers[id], (int)ost, (int)len);
            }
            else
            {
                int ll = BufferItemSize - (int)ost;

                //System.Array.Clear(mBuffers[id], (int)ost, (int)ll);
                Clear(mBuffers[id], (int)ost, (int)ll);
                if (len - ll < BufferItemSize)
                {
                    id++;
                    // System.Array.Clear(mBuffers[id], 0, (int)(len - ll));
                    Clear(mBuffers[id], 0, (int)(len - ll));
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / BufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Clear(mBuffers[id], 0, (int)BufferItemSize);
                        // System.Array.Clear(mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % BufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Clear(mBuffers[id],0, (int)otmp);
                        //System.Array.Clear(mBuffers[id], 0, otmp);
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
        public void WriteBytes(long offset, byte[] values, int valueoffset, int len)
        {

            CheckAndResize(offset + len);

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
                    Buffer.BlockCopy(values, ll + valueoffset, mBuffers[id], 0, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / BufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % BufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, otmp);
                    }
                }
            }

            Position = offset + len;
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
                    Buffer.BlockCopy(values, ll + valueoffset, mBuffers[id], 0, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / BufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % BufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, otmp);
                    }
                }
            }

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
            CheckAndResize(offset + 1);
            hd = RelocationAddress(offset, out ost);
            MemoryHelper.WriteByte((void*)hd, ost, value);
            Position = offset + 1;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByteDirect(long offset, byte value)
        {
            IntPtr hd;
            long ost;
            hd = RelocationAddress(offset, out ost);
            MemoryHelper.WriteByte((void*)hd, ost, value);
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
                CheckAndResize(offset + 8);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteDateTime((void*)hd, ost, value);
                Position = offset + 8;
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
            if (IsCrossDataBuffer(offset, 4))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                CheckAndResize(offset + 4);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteInt32((void*)hd, ost, value);
                Position = offset + 4;
            }
        }


        public void WriteIntDirect(long offset, int value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 4))
            {
                WriteBytesDirect(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteInt32((void*)hd, ost, value);
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
            if (IsCrossDataBuffer(offset, 4))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                CheckAndResize(offset + 4);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUInt32((void*)hd, ost, value);
                Position = offset + 4;
            }            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUIntDirect(long offset, uint value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 4))
            {
                WriteBytesDirect(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUInt32((void*)hd, ost, value);
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
            if (IsCrossDataBuffer(offset, 2))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                CheckAndResize(offset + 2);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteShort((void*)hd, ost, value);
                Position = offset + 2;
            }           
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShortDirect(long offset, short value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 2))
            {
                WriteBytesDirect(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteShort((void*)hd, ost, value);
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
            if (IsCrossDataBuffer(offset, 2))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                CheckAndResize(offset + 2);
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUShort((void*)hd, ost, value);
                Position = offset + 2;
            }          
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShortDirect(long offset, ushort value)
        {
            IntPtr hd;
            long ost;
            if (IsCrossDataBuffer(offset, 2))
            {
                WriteBytesDirect(offset, BitConverter.GetBytes(value));
            }
            else
            {
                hd = RelocationAddress(offset, out ost);
                MemoryHelper.WriteUShort((void*)hd, ost, value);
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
            CheckAndResize(offset + sdata.Length + 1);
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

            if (len + ost <= BufferItemSize)
            {
                Buffer.BlockCopy(mBuffers[id], (int)ost,re, 0, len);
            }
            else
            {
                int ll = BufferItemSize - (int)ost;

                Buffer.BlockCopy(mBuffers[id], (int)ost,re,0, ll);

                if (len - ll <= BufferItemSize)
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
        public virtual void Dispose()
        {
            //mDataBuffer = null;
            mIsDisposed = true;
            mBuffers.Clear();
            mHandles.Clear();
            LoggerService.Service.Erro("MemoryBlock", Name + " Disposed ");
            //GC.Collect();
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="index"></param>
        ///// <returns></returns>
        //public byte this[int index]
        //{
        //    get
        //    {
        //        long ost;
        //        var hd = RelocationAddressToArrayIndex(index, out ost);
        //        return this.mBuffers[hd][ost];
        //    }
        //    set
        //    {
        //        long ost;
        //        var hd = RelocationAddressToArrayIndex(index, out ost);
        //        this.mBuffers[hd][ost] = value;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public byte this[long index]
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
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(MemoryBlock target,long sourceStart, long targetStart, long len)
        {
            if (target == null) return;

            long osts,ostt;
            
            var hds = RelocationAddressToArrayIndex(sourceStart, out osts);

            //计算从源数据需要读取数据块索引
            //Array Index,Array Address,Start Index,Data Len
            List<Tuple<int, int, long>> mSourceIndex = new List<Tuple<int, int, long>>();
            if(osts+len<BufferItemSize)
            {
                mSourceIndex.Add(new Tuple<int, int,  long>(hds, (int)osts,  len));
            }
            else
            {
                int ll = BufferItemSize - (int)osts;

                mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, ll));

                if (len - ll < BufferItemSize)
                {
                    hds++;
                    mSourceIndex.Add(new Tuple<int, int,  long>(hds, 0,  len-ll));
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / BufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0,  BufferItemSize));
                    }
                    int otmp = ll % BufferItemSize;
                    if (otmp > 0)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int,long>(hds, 0,  otmp));
                    }
                }
            }

            long targetAddr = targetStart;

            //拷贝数据到目标数据块中
            foreach (var vv in mSourceIndex)
            {
                var hdt = RelocationAddressToArrayIndex(targetAddr, out ostt);
                if (ostt + vv.Item3 < BufferItemSize)
                {
                    Buffer.BlockCopy(this.mBuffers[vv.Item1], vv.Item2, target.mBuffers[hdt], (int)ostt, (int)vv.Item3);
                }
                else
                {
                    var count = vv.Item3+ ostt - BufferItemSize;
                    Buffer.BlockCopy(this.mBuffers[vv.Item1], vv.Item2, target.mBuffers[hdt], (int)ostt, (int)(BufferItemSize - ostt));
                    hdt++;
                    Buffer.BlockCopy(this.mBuffers[vv.Item1], vv.Item2, target.mBuffers[hdt], (int)0, (int)count);
                }
                targetAddr += vv.Item3;
            }
            //Buffer.BlockCopy(this.mDataBuffer, sourceStart, target.mDataBuffer, targgetStart, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public IMemoryBlock WriteToStream(Stream stream, long offset, long len)
        {
            try
            {
                long osts = 0;
                var hds = RelocationAddressToArrayIndex(offset, out osts);

                if ((osts + len) < BufferItemSize)
                {
                    WriteToStream(stream, Buffers[hds], (int)osts, (int)len);
                }
                else
                {
                    List<Tuple<int, int, long>> mSourceIndex = new List<Tuple<int, int, long>>();
                    int ll = BufferItemSize - (int)osts;

                    mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, ll));

                    if (len - ll < BufferItemSize)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, len - ll));
                    }
                    else
                    {
                        int bcount = (int)((len - ll) / BufferItemSize);
                        int i = 0;
                        for (i = 0; i < bcount; i++)
                        {
                            hds++;
                            mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, BufferItemSize));
                        }
                        int otmp = (int)((len - ll) % BufferItemSize);
                        if (otmp > 0)
                        {
                            hds++;
                            mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, otmp));
                        }

                        foreach (var vv in mSourceIndex)
                        {
                            WriteToStream(stream, Buffers[vv.Item1], vv.Item2, (int)vv.Item3);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("MarshalMemoryBlock", "WriteToStream:" + ex.Message);
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="source"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        private void WriteToStream(Stream stream, byte[] source, int start, int len)
        {
            stream.Write(source, start, len);
        }

        bool IMemoryBlock.IsBusy()
        {
            return mRefCount > 0;
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
        /// <param name="values"></param>
        public void WriteMemory(long offset, Memory<byte> values)
        {
            WriteMemory(offset, values, 0, values.Length);
        }

        public void WriteMemory(long offset, Memory<byte> values, int valueoffset, int len)
        {
            CheckAndResize(offset + len);

            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;
            var vpp = values.Pin();
            if (len + ost < mBufferItemSize)
            {

                Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer + valueoffset), (void*)(mHandles[id] + (int)ost), mBufferItemSize, len);
            }
            else
            {
                int ll = mBufferItemSize - (int)ost;


                Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer + valueoffset), (void*)(mHandles[id] + (int)ost), mBufferItemSize, ll);

                if (len - ll < mBufferItemSize)
                {
                    id++;
                    Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer + valueoffset + ll), (void*)(mHandles[id]), mBufferItemSize, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / mBufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer + valueoffset + ll + i * mBufferItemSize), (void*)(mHandles[id]), mBufferItemSize, mBufferItemSize);
                    }
                    int otmp = ll % mBufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer + valueoffset + ll + i * mBufferItemSize), (void*)(mHandles[id]), mBufferItemSize, otmp);
                    }
                }
            }
            vpp.Dispose();
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
            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            if (len + ost < mBufferItemSize)
            {
                Buffer.MemoryCopy((void*)(values + valueOffset), (void*)(mHandles[id] + (int)ost), mBufferItemSize - ost, len);

                //double dtmp1 = MemoryHelper.ReadDouble((void*)(values + valueOffset), 0);

                //double dtmp2 = MemoryHelper.ReadDouble((void*)(void*)(mHandles[id] + (int)ost), 0);

                //LoggerService.Service.Info("memoryBlock",dtmp1 + "," + dtmp2);

                // Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, len);
            }
            else
            {
                //LoggerService.Service.Warn("WriteBytesDirect", " len+ost:"+len+ost);
                int ll = mBufferItemSize - (int)ost;

                Buffer.MemoryCopy((void*)(values + valueOffset), (void*)(mHandles[id] + (int)ost), mBufferItemSize - ost, ll);

                // Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, ll);

                if (len - ll < mBufferItemSize)
                {
                    id++;
                    Buffer.MemoryCopy((void*)(values + valueOffset + ll), (void*)(mHandles[id]), mBufferItemSize, len - ll);

                    // Buffer.BlockCopy(values, ll + valueoffset, mBuffers[id], 0, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / mBufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;

                        Buffer.MemoryCopy((void*)(values + valueOffset + ll + i * mBufferItemSize), (void*)(mHandles[id]), mBufferItemSize, mBufferItemSize);

                        // Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % mBufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Buffer.MemoryCopy((void*)(values + valueOffset + ll + i * mBufferItemSize), (void*)(mHandles[id]), mBufferItemSize, otmp);

                        // Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, otmp);
                    }
                }
            }
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

        public List<short> ReadShorts(long offset, int count)
        {
            List<short> re = new List<short>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadShort(offset + 2 * i));
            }
            return re;
        }

        public List<ushort> ReadUShorts(long offset, int count)
        {
            List<ushort> re = new List<ushort>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadUShort(offset + 2 * i));
            }
            return re;
        }

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
        public static List<int> ToIntList(this MemoryBlock memory)
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
        public static List<double> ToDoubleList(this MemoryBlock memory)
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
        public static List<double> ToFloatList(this MemoryBlock memory)
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
        public static List<short> ToShortList(this MemoryBlock memory)
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
        public static List<DateTime> ToDatetimeList(this MemoryBlock memory)
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
            //LoggerService.Service.Info("MemoryBlock", memory.Name + " is busy.....");
            // memory.IsBusy = true;
            //memory.StartMemory[0] = 1;
            memory.IncRef();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this MemoryBlock memory)
        {
            //LoggerService.Service.Info("MemoryBlock", memory.Name+ " is ready !");
            // memory.IsBusy = false;
            //memory.StartMemory[0] = 0;
            memory.DecRef();
        }

        public static void SaveToFile(this MemoryBlock memory, string fileName)
        {
            var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
            stream.Write(BitConverter.GetBytes(memory.Buffers.Count),0,4);
            stream.Write(BitConverter.GetBytes(memory.BufferItemSize), 0, 4);
            foreach(var vv in memory.Buffers)
            {
                stream.Write(vv, 0, vv.Length);
            }
            stream.Flush();
            stream.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="fileName"></param>
        public static MemoryBlock LoadFileToMemory(this string fileName)
        {
            var stream = (System.IO.File.Open(fileName, FileMode.Open, FileAccess.Read));
            byte[] bb = new byte[4];
            stream.Read(bb, 0, 4);
            int mbcount = BitConverter.ToInt32(bb,0);
            stream.Read(bb, 0, 4);
            int bufferItemSize = BitConverter.ToInt32(bb, 0);

            MemoryBlock memory = new MemoryBlock(bufferItemSize * mbcount);
            for (int i = 0; i < mbcount; i++)
            {
                long size = Math.Min(memory.BufferItemSize, stream.Length - stream.Position);
                stream.Read(memory.Buffers[i], 0, (int)size);
            }
            stream.Close();
            return memory;
        }
    }
}
