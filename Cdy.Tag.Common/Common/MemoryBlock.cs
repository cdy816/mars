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

        /// <summary>
        /// 
        /// </summary>
        private void* handle;

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
        /// <param name="curposition"></param>
        private void GetInnerLocation(long position, int size, out IntPtr innerAddr, out long offset,out long curposition)
        {
            offset = position % BufferItemSize;
            int id = (int)(position / BufferItemSize);
            if (offset + size >= BufferItemSize)
            {
                //如果发现数据正好跨数据块了，则跳过块到下一个区块
                id += 1;
                offset = offset + size - BufferItemSize;
            }
            innerAddr = mHandles[id];
            curposition = id * BufferItemSize + offset;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="position"></param>
        /// <param name="size"></param>
        /// <param name="innerAddr"></param>
        /// <param name="offset"></param>
        private void GetDataLocation(long position, int size, out IntPtr innerAddr, out long offset)
        {
            offset = position % BufferItemSize;
            int id = (int)(position / BufferItemSize);
            innerAddr = mHandles[id];
        }

        /// <summary>
        /// 数据是否跨数据块
        /// </summary>
        /// <param name="position"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private bool IsTransDataBuffer(long position, int size)
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
            if (IsTransDataBuffer(offset,8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                GetDataLocation(offset, 8, out hd, out ost);
                MemoryHelper.WriteInt64((void*)hd, ost, value);
            }
            mPosition = offset + 8;
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
            if (IsTransDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                GetDataLocation(offset, 8, out hd, out ost);
                MemoryHelper.WriteUInt64((void*)hd, ost, value);
            }
            mPosition = offset + 8;
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
            if (IsTransDataBuffer(offset, 4))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                GetDataLocation(offset, 8, out hd, out ost);
                MemoryHelper.WriteFloat((void*)hd, ost, value);
            }
            mPosition = offset + 4;
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
            if (IsTransDataBuffer(offset, 8))
            {
                WriteBytes(offset, BitConverter.GetBytes(value));
            }
            else
            {
                GetDataLocation(offset, 8, out hd, out ost);
                MemoryHelper.WriteDouble((void*)hd, ost, value);
            }
            mPosition = offset + 8;
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
        public void WriteBytes(long offset, byte[] values, int valueoffset, int len)
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
         //   Buffer.BlockCopy(values, valueoffset, this.mDataBuffer, (int)offset, len);
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
            GetDataLocation(offset, 1, out hd, out ost);
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
            if (IsTransDataBuffer(offset, 8))
            {
                WriteBytes(offset, MemoryHelper.GetBytes(value));
            }
            else
            {
                GetDataLocation(offset, 8, out hd, out ost);
                MemoryHelper.WriteDateTime((void*)hd, ost, value);
            }
            mPosition = offset + 8;

           
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
        public void WriteUInt(long offset, uint value)
        {
            MemoryHelper.WriteUInt32(handle, offset, value);
            mPosition = offset + sizeof(uint);
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
        public void WriteUShort(long offset, ushort value)
        {
            MemoryHelper.WriteUShort(handle, offset, value);
            mPosition = offset + sizeof(ushort);
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
        public string ReadString(long offset)
        {
            return ReadString(offset, Encoding.Unicode);
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
        public void CopyTo(MemoryBlock target,int sourceStart,int targgetStart,int len)
        {
            if (target == null) return;
            Buffer.BlockCopy(this.mDataBuffer, sourceStart, target.mDataBuffer, targgetStart, len);
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
