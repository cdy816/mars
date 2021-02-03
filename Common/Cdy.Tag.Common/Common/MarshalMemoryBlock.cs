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
using System.Diagnostics;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 划分内存
    /// </summary>
    public unsafe class MarshalMemoryBlock : IDisposable, IMemoryBlock
    {

        #region ... Variables  ...

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


        protected int mBufferItemSize = 1024 * 1024 * 128;

        public const int BufferSize256 = 1024 * 1024 * 256;

        public const int BufferSize512 = 1024 * 1024 * 512;

        public const int BufferSize128 = 1024 * 1024 * 128;

        public const int BufferSize64 = 1024 * 1024 * 64;

        public const int BufferSize32 = 1024 * 1024 * 32;

        public const int BufferSize16 = 1024 * 1024 * 16;

        public const int BufferSize8 = 1024 * 1024 * 8;

        public const int BufferSize4 = 1024 * 1024 * 4;

        public const int BufferSize2 = 1024 * 1024 * 2;

        public const int BufferSize1 = 1024 * 1024 * 1;

        //public static byte[] zoreData = new byte[1024 * 10];

        private int mRefCount = 0;

        private bool mIsDisposed = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        /// <summary>
        /// 
        /// </summary>
        public MarshalMemoryBlock()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <param name="blockSize"></param>
        public MarshalMemoryBlock(long size, int blockSize)
        {
            mBufferItemSize = blockSize > 0 ? blockSize : 1;
            Init(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public MarshalMemoryBlock(long size)
        {
            Init(size);
        }

        #endregion ...Constructor...

        #region ... Properties ...

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
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<IntPtr> Buffers
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
                return mHandles[0];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public IntPtr EndMemory
        {
            get
            {
                return mHandles[mHandles.Count-1];
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
                return mHandles.Count * (long)mBufferItemSize;
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

        ///// <summary>
        ///// 附加信息
        ///// </summary>
        //public IntPtr AttachDataPointer { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...

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
        protected void Init(long size)
        {
            int count = (int)(size / mBufferItemSize);

            count = size % mBufferItemSize > 0 ? count + 1 : count;           

            mHandles = new List<IntPtr>();
            for (int i = 0; i < count; i++)
            {
                mHandles.Add(Marshal.AllocHGlobal(mBufferItemSize));
            }
            mAllocSize = size;
        }

        /// <summary>
        /// 重新分配对象
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc(long size)
        {
            if(mHandles!=null && mHandles.Count>0)
            {
                foreach (var vv in mHandles)
                {
                    Marshal.FreeHGlobal(vv);
                }
                mHandles.Clear();
            }
            Init(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc2(long size)
        {
            mBufferItemSize = (int)size;
            ReAlloc(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void Resize(long size)
        {
            int count = (int)(size / mBufferItemSize) + 1;
            //List<IntPtr> nBuffers = new List<IntPtr>(count);

            if (count > mHandles.Count)
            {
                //int i = 0;
                //for (i = 0; i < mHandles.Count; i++)
                //{
                //   // nBuffers.Add(mHandles[i]);
                //}
                for(int j= mHandles.Count; j<count;j++)
                {
                    mHandles.Add(Marshal.AllocHGlobal(mBufferItemSize));
                }
               // mHandles = nBuffers;
            }
            //else if (count < mHandles.Count)
            //{
            //    for(int i=0;i<count;i++)
            //    {
            //        nBuffers.Add(mHandles[i]);
            //    }
            //    mHandles = nBuffers;
            //}

          
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
                    int count = (int)(size / mBufferItemSize);
                    count = size % mBufferItemSize > 0 ? count + 1 : count;

                    if (mHandles.Count < count)
                    {
                        List<IntPtr> nBuffers = new List<IntPtr>(count);
                        int i = 0;
                        for (i = 0; i < mHandles.Count; i++)
                        {
                            nBuffers.Add(mHandles[i]);
                            //nBuffers[i] = mBuffers[i];
                        }

                        for (int j = i; j < count; j++)
                        {
                            nBuffers.Add(Marshal.AllocHGlobal(mBufferItemSize));
                           // nBuffers[j] = new byte[BufferItemSize];
                        }

                        mHandles = nBuffers;
                        //GC.Collect();
                    }
                    mAllocSize = size;
                    //GC.Collect();

                    LoggerService.Service.Info("CheckAndResize", "CheckAndResize " + this.Name + " " + size, ConsoleColor.Red);
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
            try
            {
                foreach (var vv in mHandles)
                {
                    if (mIsDisposed) break;

                    Unsafe.InitBlockUnaligned((void*)vv, 0, (uint)mBufferItemSize);
                    //for (int i = 0; i < mBufferItemSize / zoreData.Length; i++)
                    //{
                    //    if (mIsDisposed) break;
                    //    Marshal.Copy(zoreData, 0, vv + i * zoreData.Length, zoreData.Length);
                    //}

                    //if(mBufferItemSize % zoreData.Length >0)
                    //{
                    //    Marshal.Copy(zoreData, 0, vv + (mBufferItemSize > zoreData.Length ? mBufferItemSize - zoreData.Length : 0), mBufferItemSize > zoreData.Length ? zoreData.Length : mBufferItemSize);
                    //}

                }
            }
            catch
            {

            }
            //mUsedSize = 0;
            mPosition = 0;
            return this;
           // LoggerService.Service.Info("MemoryBlock", Name + " is clear !");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="start"></param>
        /// <param name="len"></param>
        private void Clear(IntPtr target, int start, int len)
        {
            //int i = 0;
            try
            {
                //for (i = 0; i < len / zoreData.Length; i++)
                //{
                //    Marshal.Copy(zoreData, 0, target + start + i * zoreData.Length, zoreData.Length);
                //}
                //int zz = len % zoreData.Length;
                //if (zz > 0)
                //{
                //    Marshal.Copy(zoreData, 0, target + start + i * zoreData.Length, zz);
                //}
                Unsafe.InitBlockUnaligned((void*)(target + start), 0, (uint)len);
            }
            catch
            {

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
            offset = position % mBufferItemSize;
            int id = (int)(position / mBufferItemSize);
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
            offset = position % mBufferItemSize;
            return (int)(position / mBufferItemSize);
        }

        /// <summary>
        /// 数据是否跨数据块
        /// </summary>
        /// <param name="position"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private bool IsCrossDataBuffer(long position, int size)
        {
            return (position % mBufferItemSize) + size >= mBufferItemSize;
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
            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            if (len + ost < mBufferItemSize)
            {
                Clear(mHandles[id], (int)ost, (int)len);
                //System.Array.Clear(mBuffers[id], (int)ost, (int)len);
            }
            else
            {
                int ll = mBufferItemSize - (int)ost;

                Clear(mHandles[id], (int)ost, (int)ll);
                // System.Array.Clear(mBuffers[id], (int)ost, (int)ll);

                if (len - ll < mBufferItemSize)
                {
                    id++;
                    Clear(mHandles[id], (int)ost, (int)(len - ll));
                    //System.Array.Clear(mBuffers[id], 0, (int)(len - ll));
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / mBufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Clear(mHandles[id], 0, mBufferItemSize);
                       // System.Array.Clear(mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % mBufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Clear(mHandles[id], 0, otmp);
                        // System.Array.Clear(mBuffers[id], 0, otmp);
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

            CheckAndResize(offset + len);

            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;
            var vpp = values.Pin();
            if (len + ost < mBufferItemSize)
            {

                Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer+valueoffset),(void*)(mHandles[id] + (int)ost), mBufferItemSize, len);
            }
            else
            {
                int ll = mBufferItemSize - (int)ost;


                 Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer + valueoffset), (void*)(mHandles[id] + (int)ost), mBufferItemSize, ll);

                if (len - ll < mBufferItemSize)
                {
                    id++;
                    Buffer.MemoryCopy((void*)((IntPtr)vpp.Pointer + valueoffset+ll), (void*)(mHandles[id]), mBufferItemSize,len- ll);
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
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytes(long offset, byte[] values, int valueoffset, int len)
        {

            CheckAndResize(offset + len);

            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            if (len + ost <= mBufferItemSize)
            {
                Marshal.Copy(values, valueoffset, mHandles[id] + (int)ost, len);
               // Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, len);
            }
            else
            {
                int ll = mBufferItemSize - (int)ost;

                Marshal.Copy(values, valueoffset, mHandles[id] + (int)ost, ll);

               // Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, ll);

                if (len - ll <= mBufferItemSize)
                {
                    id++;
                    Marshal.Copy(values, valueoffset+ll, mHandles[id], len - ll);
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
                        Marshal.Copy(values, valueoffset+ ll + i * mBufferItemSize, mHandles[id], mBufferItemSize);
                        // Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % mBufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Marshal.Copy(values, valueoffset + ll + i * mBufferItemSize, mHandles[id], otmp);
                        //Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, otmp);
                    }
                }
            }

            Position = offset + len;
        }

        public void WriteBytesDirect(long offset,IntPtr values,int valueOffset,int len)
        {
            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            if (len + ost <= mBufferItemSize)
            {
                Buffer.MemoryCopy((void*)(values + valueOffset), (void*)(mHandles[id] + (int)ost), mBufferItemSize-ost,len);

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

                if (len - ll <= mBufferItemSize)
                {
                    id++;
                    Buffer.MemoryCopy((void*)(values + valueOffset+ll), (void*)(mHandles[id]), mBufferItemSize,len- ll);

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

                        Buffer.MemoryCopy((void*)(values + valueOffset+ll + i * mBufferItemSize), (void*)(mHandles[id]), mBufferItemSize, mBufferItemSize);

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long offset, byte[] values, int valueoffset, int len)
        {
            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            if (len + ost <= mBufferItemSize)
            {
                Marshal.Copy(values, valueoffset, mHandles[id] + (int)ost, len);
                // Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, len);
            }
            else
            {
                int ll = mBufferItemSize - (int)ost;

                Marshal.Copy(values, valueoffset, mHandles[id] + (int)ost, ll);
                // Buffer.BlockCopy(values, valueoffset, mBuffers[id], (int)ost, ll);

                if (len - ll <= mBufferItemSize)
                {
                    id++;
                    Marshal.Copy(values, valueoffset+ll, mHandles[id], len-ll);
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
                        Marshal.Copy(values, valueoffset + ll + i * mBufferItemSize, mHandles[id], mBufferItemSize);
                       // Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, BufferItemSize);
                    }
                    int otmp = ll % mBufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Marshal.Copy(values, valueoffset + ll + i * mBufferItemSize, mHandles[id], otmp);
                       // Buffer.BlockCopy(values, valueoffset + ll + i * BufferItemSize, mBuffers[id], 0, otmp);
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


        public List<int> ReadInts(long offset,int count)
        {
            List<int> re = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadInt(offset + 4 * i));
            }
            return re;
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
        public void ReadDoubleBytes(long offset,ref byte[] bytes)
        {
            mPosition = offset + 8;
            if (IsCrossDataBuffer(offset, 8))
            {
                ReadBytesInner(offset, 8,ref bytes);
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                Marshal.Copy(hd+ (int)ost, bytes,0 , 8);
                //return MemoryHelper.ReadDouble((void*)hd, ost);
            }
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
        /// <param name="count"></param>
        /// <returns></returns>
        public Memory<double> ReadDoubleByMemory(long offset,int count)
        {
            var re = MemoryPool<double>.Shared.Rent(count).Memory;
            for (int i = 0; i < count; i++)
            {
                re.Span[i]=(ReadDouble(offset + 8 * i));
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
                return new string((sbyte*)hd, (int)ost + 1, len, encoding);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public string ReadStringbyFixSize(long offset, Encoding encoding)
        {
            var len = ReadByte(offset);
            mPosition = offset + Const.StringSize;
            if (IsCrossDataBuffer(offset + 1, len))
            {
                return encoding.GetString(ReadBytesInner(offset + 1, len));
            }
            else
            {
                long ost;
                var hd = RelocationAddress(offset, out ost);
                return new string((sbyte*)hd, (int)ost + 1, len, encoding);
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

        private unsafe Memory<byte> ReadBytesInner2(long offset, int len)
        {
            var  mm = MemoryPool<byte>.Shared;
            var vm = mm.Rent(len).Memory;

            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            using(var vp = vm.Pin())
            {
                if (len + ost <= mBufferItemSize)
                {
                    Buffer.MemoryCopy((void*)(mHandles[id] + (int)ost), vp.Pointer, len, len);
                }
                else
                {
                    int ll = mBufferItemSize - (int)ost;

                  //  Marshal.Copy(mHandles[id] + (int)ost, re, 0, ll);

                    Buffer.MemoryCopy((void*)(mHandles[id] + (int)ost), vp.Pointer, ll, ll);

                    if (len - ll <= mBufferItemSize)
                    {
                        id++;
                        //Marshal.Copy(mHandles[id], re, ll, len - ll);
                        Buffer.MemoryCopy((void*)(mHandles[id] + (int)ost), (void*)((IntPtr)vp.Pointer+ll), len - ll, len - ll);
                    }
                    else
                    {
                        long ltmp = len - ll;
                        int bcount = ll / mBufferItemSize;
                        int i = 0;
                        for (i = 0; i < bcount; i++)
                        {
                            id++;
                         //   Marshal.Copy(mHandles[id], re, ll + i * BufferItemSize, BufferItemSize);
                            Buffer.MemoryCopy((void*)(mHandles[id] ), (void*)((IntPtr)vp.Pointer + +(int)ll + i * mBufferItemSize), mBufferItemSize, mBufferItemSize);
                        }
                        int otmp = ll % mBufferItemSize;
                        if (otmp > 0)
                        {
                            id++;
                            //Marshal.Copy(mHandles[id], re, ll + i * BufferItemSize, otmp);
                            Buffer.MemoryCopy((void*)(mHandles[id]), (void*)((IntPtr)vp.Pointer + +(int)ll + i * mBufferItemSize), otmp, otmp);
                        }
                    }
                }
            }
            

            return vm;
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
            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            if (len + ost <= mBufferItemSize)
            {
                Marshal.Copy(mHandles[id]+ (int)ost, re, 0, len);
               // Buffer.BlockCopy(mBuffers[id], (int)ost,re, 0, len);
            }
            else
            {
                int ll = mBufferItemSize - (int)ost;

                Marshal.Copy(mHandles[id] + (int)ost, re, 0, ll);

               // Buffer.BlockCopy(mBuffers[id], (int)ost,re,0, ll);

                if (len - ll <= mBufferItemSize)
                {
                    id++;
                    Marshal.Copy(mHandles[id], re, ll, len - ll);
                   // Buffer.BlockCopy(mBuffers[id], 0, re, ll, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / mBufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Marshal.Copy(mHandles[id], re, ll + i * mBufferItemSize, mBufferItemSize);
                       // Buffer.BlockCopy(mBuffers[id], 0, re, ll + i * BufferItemSize, BufferItemSize);
                    }
                    int otmp = ll % mBufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Marshal.Copy(mHandles[id], re, ll + i * mBufferItemSize, otmp);
                        //Buffer.BlockCopy(mBuffers[id], 0,re, ll + i * BufferItemSize, otmp);
                    }
                }
            }

            return re;
        }

        private byte[] ReadBytesInner(long offset, int len,ref byte[] re)
        {
            int id = (int)(offset / mBufferItemSize);

            long ost = offset % mBufferItemSize;

            if (len + ost <= mBufferItemSize)
            {
                Marshal.Copy(mHandles[id] + (int)ost, re, 0, len);
                // Buffer.BlockCopy(mBuffers[id], (int)ost,re, 0, len);
            }
            else
            {
                int ll = mBufferItemSize - (int)ost;

                Marshal.Copy(mHandles[id] + (int)ost, re, 0, ll);

                // Buffer.BlockCopy(mBuffers[id], (int)ost,re,0, ll);

                if (len - ll <= mBufferItemSize)
                {
                    id++;
                    Marshal.Copy(mHandles[id], re, ll, len - ll);
                    // Buffer.BlockCopy(mBuffers[id], 0, re, ll, len - ll);
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = ll / mBufferItemSize;
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        id++;
                        Marshal.Copy(mHandles[id], re, ll + i * mBufferItemSize, mBufferItemSize);
                        // Buffer.BlockCopy(mBuffers[id], 0, re, ll + i * BufferItemSize, BufferItemSize);
                    }
                    int otmp = ll % mBufferItemSize;
                    if (otmp > 0)
                    {
                        id++;
                        Marshal.Copy(mHandles[id], re, ll + i * mBufferItemSize, otmp);
                        //Buffer.BlockCopy(mBuffers[id], 0,re, ll + i * BufferItemSize, otmp);
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
        /// <param name="address"></param>
        /// <param name="start"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public MarshalMemoryBlock ReadBytes(IntPtr address,int start,int size)
        {
            MarshalMemoryBlock mmb = new MarshalMemoryBlock(size, size);
            Buffer.MemoryCopy((void*)(address + start), (void*)mmb.Handles[0], size, size);

            return mmb;
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
        /// <returns></returns>
        public string ReadStringbyFixSize()
        {
            return ReadStringbyFixSize(mPosition, Encoding.Unicode);
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="len"></param>
        /// <returns></returns>
        public Memory<byte> ReadBytesByMemory(int len)
        {
            var re = ReadBytesInner2(mPosition, len);
            mPosition += len;
            return re;
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        public virtual void Dispose()
        {
            //mDataBuffer = null;
            mIsDisposed = true;
            if (mHandles.Count > 0)
            {
                foreach (var vv in mHandles)
                {
                    Marshal.FreeHGlobal(vv);
                }
                mHandles.Clear();
            }
          //  LoggerService.Service.Erro("MarshalMemoryBlock", Name +" Disposed " );

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
                long ost;
                var hd = RelocationAddressToArrayIndex(index, out ost);
                return Marshal.ReadByte(this.mHandles[hd]+(int)ost);
            }
            set
            {
                long ost;
                var hd = RelocationAddressToArrayIndex(index, out ost);
                Marshal.WriteByte(this.mHandles[hd] + (int)ost, value);
                //this.mBuffers[hd][ost] = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyFrom(MarshalFixedMemoryBlock source,long sourceStart,long targetStart,long len)
        {
            long targetAddr = targetStart;
            long olen, ostt;

            long sourceAddr = source.Handles.ToInt64()+ sourceStart;

            var hdt = RelocationAddressToArrayIndex(targetAddr, out ostt);
            if (ostt + len < mBufferItemSize)
            {
                Buffer.MemoryCopy((void*)sourceAddr, (void*)(mHandles[hdt] + (int)ostt), mBufferItemSize - ostt, len);
            }
            else
            {
                Buffer.MemoryCopy((void*)sourceAddr, (void*)(mHandles[hdt] + (int)ostt), mBufferItemSize - ostt, (mBufferItemSize - ostt));
                hdt++;
                olen = mBufferItemSize - ostt;

                while (olen<len)
                {
                    long ltmp = Math.Min((len - olen), mBufferItemSize);
                    Buffer.MemoryCopy((void*)(sourceAddr + olen), (void*)(mHandles[hdt]), mBufferItemSize, ltmp);
                    olen += ltmp;
                    hdt++;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceSrart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(FixedMemoryBlock target,long sourceStart, long targetStart,long len)
        {
            if (target == null) return;

            long osts;

            var hds = RelocationAddressToArrayIndex(sourceStart, out osts);

            //计算从源数据需要读取数据块索引
            //Array Index,Start Address,Data Len
            List<Tuple<int, int, long>> mSourceIndex = new List<Tuple<int, int, long>>();
            if (osts + len < mBufferItemSize)
            {
                mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, len));
            }
            else
            {
                int ll = mBufferItemSize - (int)osts;

                mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, ll));

                if ((len - ll) < mBufferItemSize)
                {
                    hds++;
                    mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, len - ll));
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = (int)(ltmp / mBufferItemSize);
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, mBufferItemSize));
                    }
                    int otmp = (int)(ltmp % mBufferItemSize);
                    if (otmp > 0)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, otmp));
                    }
                }
            }

            long targetAddr = targetStart;

            //拷贝数据到目标数据块中
            foreach (var vv in mSourceIndex)
            {
                Marshal.Copy(this.mHandles[vv.Item1] + vv.Item2, target.Buffers, (int)targetAddr, (int)vv.Item3);
                targetAddr += vv.Item3;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(MarshalMemoryBlock target,long sourceStart, long targetStart, long len)
        {
            if (target == null) return;

            long osts,ostt;
            
            var hds = RelocationAddressToArrayIndex(sourceStart, out osts);

            //计算从源数据需要读取数据块索引
            //Array Index,Start Address,Data Len
            List<Tuple<int, int,  long>> mSourceIndex = new List<Tuple<int, int, long>>();
            if(osts+len<=mBufferItemSize)
            {
                mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, len));
            }
            else
            {
                int ll = mBufferItemSize - (int)osts;

                mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts,  ll));

                if ((len - ll) <= mBufferItemSize)
                {
                    hds++;
                    mSourceIndex.Add(new Tuple<int, int, long>(hds, 0,len-ll));
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = (int)(ltmp / mBufferItemSize);
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int,  long>(hds, 0, mBufferItemSize));
                    }
                    int otmp = (int)(ltmp % mBufferItemSize);
                    if (otmp > 0)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0,  otmp));
                    }
                }
            }

            long targetAddr = targetStart;

            //拷贝数据到目标数据块中
            foreach (var vv in mSourceIndex)
            {
                var hdt = RelocationAddressToArrayIndex(targetAddr, out ostt);
                if (ostt + vv.Item3 <= mBufferItemSize)
                {
                    Buffer.MemoryCopy((void*)(this.mHandles[vv.Item1] + vv.Item2), (void*)(target.mHandles[hdt] + (int)ostt), mBufferItemSize-ostt, vv.Item3);
                }
                else
                {
                    var count = vv.Item3 + ostt - mBufferItemSize;
                    Buffer.MemoryCopy((void*)(this.mHandles[vv.Item1] + vv.Item2), (void*)(target.mHandles[hdt] + (int)ostt), mBufferItemSize - ostt, (mBufferItemSize - ostt));
                    hdt++;
                    Buffer.MemoryCopy((void*)(this.mHandles[vv.Item1] + vv.Item2), (void*)(target.mHandles[hdt]), mBufferItemSize, (count));
                }
                targetAddr += vv.Item3;
            }
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

            long osts;
            var hds = RelocationAddressToArrayIndex(sourceStart, out osts);

            //计算从源数据需要读取数据块索引
            //Array Index,Start Address,Data Len
            List<Tuple<int, int, long>> mSourceIndex = new List<Tuple<int, int, long>>();
            if (osts + len <= mBufferItemSize)
            {
                mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, len));
            }
            else
            {
                int ll = mBufferItemSize - (int)osts;

                mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, ll));

                if ((len - ll) <= mBufferItemSize)
                {
                    hds++;
                    mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, len - ll));
                }
                else
                {
                    long ltmp = len - ll;
                    int bcount = (int)(ltmp / mBufferItemSize);
                    int i = 0;
                    for (i = 0; i < bcount; i++)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, mBufferItemSize));
                    }
                    int otmp = (int)(ltmp % mBufferItemSize);
                    if (otmp > 0)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, otmp));
                    }
                }
            }

            long targetAddr = targetStart;

            //拷贝数据到目标数据块中
            foreach (var vv in mSourceIndex)
            {
                Buffer.MemoryCopy((void*)(this.mHandles[vv.Item1] + vv.Item2), (void*)(target + (int)targetAddr), vv.Item3, vv.Item3);
                targetAddr += vv.Item3;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public IMemoryBlock WriteToStream(Stream stream,long offset,long len)
        {
            try
            {
                long osts = 0;
                var hds = RelocationAddressToArrayIndex(offset, out osts);

                if ((osts + len) <= mBufferItemSize)
                {
                    WriteToStream(stream, mHandles[hds], (int)osts, (int)len);
                }
                else
                {
                    List<Tuple<int, int, long>> mSourceIndex = new List<Tuple<int, int, long>>();
                    int ll = mBufferItemSize - (int)osts;

                    mSourceIndex.Add(new Tuple<int, int, long>(hds, (int)osts, ll));

                    if (len - ll <= mBufferItemSize)
                    {
                        hds++;
                        mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, len - ll));
                    }
                    else
                    {
                        int bcount = (int)((len - ll) / mBufferItemSize);
                        int i = 0;
                        for (i = 0; i < bcount; i++)
                        {
                            hds++;
                            mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, mBufferItemSize));
                        }
                        int otmp = (int)((len - ll) % mBufferItemSize);
                        if (otmp > 0)
                        {
                            hds++;
                            mSourceIndex.Add(new Tuple<int, int, long>(hds, 0, otmp));
                        }

                        foreach (var vv in mSourceIndex)
                        {
                            WriteToStream(stream, this.mHandles[vv.Item1], vv.Item2, (int)vv.Item3);
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Erro("MarshalMemoryBlock", "WriteToStream:"+ex.Message);
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
        public void WriteToStream(Stream stream,IntPtr source,int start,int len)
        {
            using (System.IO.UnmanagedMemoryStream stream1 = new UnmanagedMemoryStream((byte*)(source + start), len))
            {
                stream1.CopyTo(stream);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        /// <param name="source"></param>
        /// <param name="len"></param>
        public void WriteToStream(Stream stream,IntPtr source,int len)
        {
            using (System.IO.UnmanagedMemoryStream stream1 = new UnmanagedMemoryStream((byte*)(source), len))
            {
                stream1.CopyTo(stream);
            }
        }


        public void ReadFromStream(Stream stream,int len)
        {
            int count = len / mBufferItemSize;
            for(int i=0;i<count;i++)
            {
                stream.Read(new Span<byte>((void*)this.Buffers[i], mBufferItemSize));
            }

            if(len%mBufferItemSize>0)
            {
                //size = 0;
                int isize = len % mBufferItemSize;

                stream.Read(new Span<byte>((void*)this.Buffers[count], isize));
            }
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

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class MarshalMemoryBlockExtends
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public static List<long> ToLongList(this MarshalMemoryBlock memory)
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
        public static List<int> ToIntList(this MarshalMemoryBlock memory)
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
        public static List<double> ToDoubleList(this MarshalMemoryBlock memory)
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
        public static List<double> ToFloatList(this MarshalMemoryBlock memory)
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
        public static List<short> ToShortList(this MarshalMemoryBlock memory)
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
        public static List<DateTime> ToDatetimeList(this MarshalMemoryBlock memory)
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
        public static List<string> ToStringList(this MarshalMemoryBlock memory, Encoding encoding)
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
        public static List<string> ToStringList(this MarshalMemoryBlock memory,int offset, Encoding encoding)
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
        public static void MakeMemoryBusy(this MarshalMemoryBlock memory)
        {
            memory.IncRef();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this MarshalMemoryBlock memory)
        {
            memory.DecRef();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="fileName"></param>
        public static void Dump(this MarshalMemoryBlock memory,string fileName)
        {
            using (var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                byte[] bvals = new byte[1024];
                foreach (var vv in memory.Buffers)
                {
                    for (int i = 0; i < memory.BufferItemSize / 1024; i++)
                    {
                        Marshal.Copy(vv + i * 1024, bvals, 0, 1024);
                        stream.Write(bvals, 0, 1024);
                    }
                }
                stream.Flush();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static void RecordToLog(this MarshalMemoryBlock memory,Stream stream)
        {
                byte[] bvals = new byte[1024];
                long totalsize = memory.AllocSize;
                long csize = 0;
                foreach (var vv in memory.Buffers)
                {
                    for (int i = 0; i < memory.BufferItemSize / 1024; i++)
                    {
                        Marshal.Copy(vv + i * 1024, bvals, 0, 1024);
                        int isize = (int)Math.Min(totalsize - csize, 1024);
                        csize += isize;
                        stream.Write(bvals, 0, isize);
                        if (csize >= totalsize)
                            break;
                    }
                    if (csize >= totalsize)
                        break;
                }
                stream.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="time"></param>
        public static void Dump(this MarshalMemoryBlock memory,DateTime time)
        {
            string fileName = memory.Name + "_" + time.ToString("yyyy_MM_dd_HH_mm_ss") + ".dmp";
            fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalMemoryBlock).Assembly.Location), fileName);
            Dump(memory, fileName);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="memory"></param>
        //public static void Dump(this MarshalMemoryBlock memory)
        //{
        //    string fileName = memory.Name + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm_ss")+".dmp";
        //    fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalMemoryBlock).Assembly.Location), fileName);
        //    Dump(memory, fileName);
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="file"></param>
        ///// <returns></returns>
        //public static MarshalMemoryBlock LoadDumpFromFile(this string file)
        //{
        //    if(System.IO.File.Exists(file))
        //    {
        //        using (var stream = System.IO.File.OpenRead(file))
        //        {
        //            MarshalMemoryBlock block = new MarshalMemoryBlock(stream.Length);
        //            byte[] bvals = new byte[1024 * 64];
        //            int len = 0;
        //            do
        //            {
        //                len = stream.Read(bvals, 0, bvals.Length);
        //                block.WriteBytes(block.Position, bvals, 0, len);
        //            }
        //            while (len > 0);
        //        }
        //    }
        //    return null;
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="fileName"></param>
        public static void SaveToFile(this MarshalMemoryBlock memory, string fileName)
        {

            string sd = System.IO.Path.GetDirectoryName(fileName);
            if(!System.IO.Directory.Exists(sd))
            {
                System.IO.Directory.CreateDirectory(sd);
            }

            var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
          
            stream.Write(BitConverter.GetBytes(memory.Buffers.Count), 0, 4);
            stream.Write(BitConverter.GetBytes(memory.BufferItemSize), 0, 4);
            foreach (var vv in memory.Buffers)
            {
                memory.WriteToStream(stream, vv, memory.BufferItemSize);
            }
            stream.Flush();
            stream.Close();
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="fileName"></param>
        public static unsafe MarshalMemoryBlock LoadFileToMarshalMemory(this string fileName)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var stream = (System.IO.File.Open(fileName, FileMode.Open, FileAccess.Read));
            byte[] bb = new byte[4];
            stream.Read(bb, 0, 4);
            int mbcount = BitConverter.ToInt32(bb,0);
            stream.Read(bb, 0, 4);
            int bufferItemSize = BitConverter.ToInt32(bb, 0);
            //byte[] byt = new byte[bufferItemSize];
            MarshalMemoryBlock memory = new MarshalMemoryBlock(bufferItemSize * mbcount,bufferItemSize);

            for (int i = 0; i < mbcount; i++)
            {
                stream.Read(new Span<byte>((void*)memory.Buffers[i],bufferItemSize));
                //stream.Read(new Span<byte>(byt));
            }
            stream.Close();
            sw.Stop();

            Debug.Print("加载文件 "+ fileName +" 耗时:" + sw.ElapsedMilliseconds);
            return memory;
        }
    }
}
