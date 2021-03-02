//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/8 15:17:34.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace DBRuntime.His
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe class HisDataMemoryBlockCollection3 : MarshalMemoryBlock,IDisposable
    {

        #region ... Variables  ...

        // HisDataMemory Structor: MemoryBaseAddressPointer(8) + TimerAdress offset(4)+ValueAddress offset(4)+QualityAddress offset(4)+TimeLen(1) + Size(4) + Position(4)  = 29

        public const int HisDataMemoryItemSize = 29;

        private Dictionary<int, int> mTagAddress = new Dictionary<int, int>();

        private int mCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public HisDataMemoryBlockCollection3() : base()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public HisDataMemoryBlockCollection3(int count) : base(count * 29, 100000 * 29)
        {

        }



        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public byte Id { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public string Name { get; set; }

        /// <summary>
        /// 变量内存地址缓存
        /// Value  每项的含义：起始地址,时间地址偏移,值地址偏移,质量地址偏移,时间单位,数据大小,地址（备用）
        /// </summary>
        public Dictionary<int, int> TagAddress
        {
            get
            {
                return mTagAddress;
            }
            set
            {
                if (mTagAddress != value)
                {
                    mTagAddress = value;
                }
            }
        }

        /// <summary>
        /// 相对基准时间
        /// </summary>
        public DateTime BaseTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime CurrentDatetime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndDateTime { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(long target, long targetStart,long source, long sourceStart, long len)
        {
            Buffer.MemoryCopy((void*)(new IntPtr(source + sourceStart)), (void*)(new IntPtr(target + targetStart)), len, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="timeroffset"></param>
        /// <param name="valueoffset"></param>
        /// <param name="qualityoffset"></param>
        /// <param name="size"></param>
        /// <param name="timeMemorylen"></param>
        public long AddTagAddress(int id, int timeroffset, int valueoffset, int qualityoffset,int size,byte timeMemorylen)
        {
            if (!mTagAddress.ContainsKey(id))
            {
                mTagAddress.Add(id, mCount);
                int pp = mCount * HisDataMemoryItemSize;
                var re = CalMemory(size).ToInt64();
                this.WriteLong(pp, re);
                this.WriteInt(pp + 8, timeroffset);
                this.WriteInt(pp + 12, valueoffset);
                this.WriteInt(pp + 16, qualityoffset);
                this.WriteByte(pp + 20, timeMemorylen);
                this.WriteInt(pp + 21, size);
                this.WriteInt(pp + 25, 0);
                mCount++;
                Clear(new IntPtr(re), size);
                return re;
            }
            else
            {
                return (this.ReadLong(mTagAddress[id] * HisDataMemoryItemSize));
            }
        }

        /// <summary>
        /// 重新分配变量的内存
        /// </summary>
        /// <param name="id"></param>
        /// <param name="timeroffset"></param>
        /// <param name="valueoffset"></param>
        /// <param name="qualityoffset"></param>
        /// <param name="size"></param>
        /// <param name="timeMemorylen"></param>
        public long ReAllocTagAddress(int id, int timeroffset, int valueoffset, int qualityoffset, int size, byte timeMemorylen)
        {
            if (mTagAddress.ContainsKey(id))
            {
                IntPtr ip = new IntPtr(this.ReadLong(mTagAddress[id] * HisDataMemoryItemSize));
                Marshal.FreeHGlobal(ip);
                var re = CalMemory(size).ToInt64();
                var pp = mTagAddress[id] * HisDataMemoryItemSize;
                this.WriteLong(pp, re);
                this.WriteInt(pp + 8, timeroffset);
                this.WriteInt(pp + 12, valueoffset);
                this.WriteInt(pp + 16, qualityoffset);
                this.WriteByte(pp + 20, timeMemorylen);
                this.WriteInt(pp + 21, size);
                this.WriteInt(pp + 25, 0);
                Clear(new IntPtr(re), size);
                return re;
            }
            return 0;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //public long ReadDataBaseAddress(int id)
        //{
        //    if (mTagAddress.ContainsKey(id))
        //    {
        //        var vdd = mTagAddress[id] * HisDataMemoryItemSize;
        //        return this.ReadLong(vdd);
        //    }
        //    else
        //    {
        //        return 0;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public long ReadDataBaseAddressByIndex(int index)
        {
            var vdd = index * HisDataMemoryItemSize;
            return this.ReadLong(vdd);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //public int ReadTimerOffsetAddress(int id)
        //{
        //    if (mTagAddress.ContainsKey(id))
        //    {
        //        var vdd = mTagAddress[id] * HisDataMemoryItemSize+8;
        //        return this.ReadInt(vdd);
        //    }
        //    else
        //    {
        //        return -1;
        //    }
        //}


        public int ReadTimerOffsetAddressByIndex(int index)
        {
            var vdd = index * HisDataMemoryItemSize + 8;
            return this.ReadInt(vdd);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //public int ReadValueOffsetAddress(int id)
        //{
        //    if (mTagAddress.ContainsKey(id))
        //    {
        //        var vdd = mTagAddress[id] * HisDataMemoryItemSize + 12;
        //        return this.ReadInt(vdd);
        //    }
        //    else
        //    {
        //        return -1;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public int ReadValueOffsetAddressByIndex(int index)
        {
            var vdd = index * HisDataMemoryItemSize + 12;
            return this.ReadInt(vdd);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //public int ReadQualityOffsetAddress(int id)
        //{
        //    if (mTagAddress.ContainsKey(id))
        //    {
        //        var vdd = mTagAddress[id] * HisDataMemoryItemSize + 16;
        //        return this.ReadInt(vdd);
        //    }
        //    else
        //    {
        //        return -1;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns>
        public int ReadQualityOffsetAddressByIndex(int index)
        {
            var vdd = index * HisDataMemoryItemSize + 16;
            return this.ReadInt(vdd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public byte ReadTimeLenByIndex(int index)
        {
            var vdd = index * HisDataMemoryItemSize + 20;
            return this.ReadByte(vdd);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public int ReadDataSizeByIndex(int index)
        {
            var vdd = index * HisDataMemoryItemSize + 21;
            return this.ReadInt(vdd);
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //public byte ReadTimeLen(int id)
        //{
        //    if (mTagAddress.ContainsKey(id))
        //    {
        //        var vdd = mTagAddress[id] * HisDataMemoryItemSize + 20;
        //        return this.ReadByte(vdd);
        //    }
        //    else
        //    {
        //        return 2;
        //    }
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <returns></returns>
        //public int ReadDataSize(int id)
        //{
        //    if (mTagAddress.ContainsKey(id))
        //    {
        //        var vdd = mTagAddress[id] * HisDataMemoryItemSize + 21;
        //        return this.ReadInt(vdd);
        //    }
        //    else
        //    {
        //        return 0;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        private IntPtr CalMemory(int size)
        {
            return Marshal.AllocHGlobal(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void RemoveTagAdress(int id)
        {
            if(mTagAddress.ContainsKey(id))
            {
                var cid = mTagAddress[id];
                var pp = new IntPtr(this.ReadLong(mTagAddress[id] * HisDataMemoryItemSize));
                Marshal.FreeHGlobal(pp);
                mTagAddress.Remove(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ptr"></param>
        /// <param name="size"></param>
        public void Clear(IntPtr ptr,int size)
        {
            Unsafe.InitBlockUnaligned((void*)ptr, 0, (uint)size);
        }

        /// <summary>
        /// 
        /// </summary>
        public  new void Clear()
        {
            if (TagAddress == null) return;
            foreach (var vv in TagAddress)
            {
                var ppp = vv.Value * HisDataMemoryItemSize;
                var pp = new IntPtr(this.ReadLong(ppp));
                Unsafe.InitBlockUnaligned((void*)pp, 0, (uint)this.ReadInt(ppp + 21));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public new void Dispose()
        {
            while (this.IsBusy()) Thread.Sleep(1);
            foreach (var vv in mTagAddress)
            {
                var ppp = vv.Value * HisDataMemoryItemSize;
                var pp = new IntPtr(this.ReadLong(ppp));
                Marshal.FreeHGlobal(pp);
            }
            mTagAddress.Clear();
            mTagAddress = null;
            base.Dispose();
        }


        #region ReadAndWrite

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public DateTime ReadDateTime(long address,long offset)
        {
            return MemoryHelper.ReadDateTime((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<DateTime> ReadDateTimes(long address, long offset, int count)
        {
            List<DateTime> re = new List<DateTime>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadDateTime(address,offset + 8 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public int ReadInt(long address, long offset)
        {
            return MemoryHelper.ReadInt32((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<int> ReadInts(long address, long offset, int count)
        {
            List<int> re = new List<int>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadInt(address,offset + 4 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public uint ReadUInt(long address, long offset)
        {
            return MemoryHelper.ReadUInt32((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<uint> ReadUInts(long address, long offset, int count)
        {
            List<uint> re = new List<uint>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadUInt(address,offset + 4 * i));
            }
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public short ReadShort(long address, long offset)
        {
            return MemoryHelper.ReadShort((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ushort ReadUShort(long address, long offset)
        {
            return MemoryHelper.ReadUShort((void*)new IntPtr(address), offset);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public float ReadFloat(long address, long offset)
        {
            return MemoryHelper.ReadFloat((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<float> ReadFloats(long address, long offset, int count)
        {
            List<float> re = new List<float>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadFloat(address,offset + 4 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public double ReadDouble(long address, long offset)
        {
            return MemoryHelper.ReadDouble((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<double> ReadDoubles(long address, long offset, int count)
        {
            List<double> re = new List<double>(count);
            for (int i = 0; i < count; i++)
            {
                re.Add(ReadDouble(address,offset + 8 * i));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public long ReadLong(long address, long offset)
        {
            return MemoryHelper.ReadInt64((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ulong ReadULong(long address, long offset)
        {
            return MemoryHelper.ReadUInt64((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte ReadByte(long address, long offset)
        {
            return MemoryHelper.ReadByte((void*)new IntPtr(address), offset);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long address, long offset, Encoding encoding)
        {
            var len = ReadByte(offset);
            return encoding.GetString(ReadBytesInner(address, offset + 1, len));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long address, long offset)
        {
            return ReadString(address,offset, Encoding.Unicode);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        private byte[] ReadBytesInner(long address, long offset, int len)
        {
            byte[] re = new byte[len];
            Marshal.Copy(new IntPtr(address + offset), re, 0, len);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="target"></param>
        /// <param name="len"></param>
        public void ReadBytes(long address, long offset, byte[] target, int len)
        {
            Marshal.Copy(new IntPtr(address + offset), target, 0, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte[] ReadBytes(long address, long offset, int len)
        {
            byte[] re = ReadBytesInner(address,offset, len);
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLongDirect(long address,long offset, long value)
        {
            MemoryHelper.WriteInt64((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteULongDirect(long address, long offset, ulong value)
        {
            MemoryHelper.WriteUInt64((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteFloatDirect(long address, long offset, float value)
        {
            MemoryHelper.WriteFloat((void*)new IntPtr(address), offset, value);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDoubleDirect(long address, long offset, double value)
        {
            MemoryHelper.WriteDouble((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteMemoryDirect(long address, long offset, Memory<byte> values)
        {
            WriteMemoryDirect(address,offset, values, 0, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteMemoryDirect(long address, long offset, Memory<byte> values, int valueoffset, int len)
        {
            Buffer.MemoryCopy((void*)((IntPtr)values.Pin().Pointer + valueoffset), (void*)(new IntPtr(address + offset)), len, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytesDirect(long address, long offset, byte[] values)
        {
            WriteBytesDirect(address,offset, values, 0, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueOffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long address, long offset, IntPtr values, int valueOffset, int len)
        {
            Buffer.MemoryCopy((void*)(values + valueOffset), (void*)(new IntPtr(address + offset)), len, len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long address, long offset, byte[] values, int valueoffset, int len)
        {
            Marshal.Copy(values, valueoffset, new IntPtr(address + offset), len);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByteDirect(long address, long offset, byte value)
        {
            MemoryHelper.WriteByte((void*)new IntPtr(address), offset, value);
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDatetimeDirect(long address, long offset, DateTime value)
        {
            MemoryHelper.WriteDateTime((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteIntDirect(long address, long offset, int value)
        {
            MemoryHelper.WriteInt32((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUIntDirect(long address, long offset, uint value)
        {
            MemoryHelper.WriteUInt32((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShortDirect(long address, long offset, short value)
        {
            MemoryHelper.WriteShort((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShortDirect(long address, long offset, ushort value)
        {
            MemoryHelper.WriteUShort((void*)new IntPtr(address), offset, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public void WriteStringDirect(long address, long offset, string value, Encoding encode)
        {
            var sdata = encode.GetBytes(value);
            WriteByteDirect(address,offset, (byte)sdata.Length);
            WriteBytesDirect(address,offset + 1, sdata);
        }

        

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class HisDataMemoryBlockCollection2Extends
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static MarshalFixedMemoryBlock RecordDataToLog(this HisDataMemoryBlockCollection3 memory, Stream stream)
        {
            MarshalFixedMemoryBlock mfb = new MarshalFixedMemoryBlock(memory.TagAddress.Count * 12+4);
            mfb.WriteInt(0,(int)mfb.Length);
            foreach (var vv in memory.TagAddress)
            {
                mfb.Write(vv.Key);
                mfb.Write(stream.Position);
                RecordToLog2(new IntPtr(memory.ReadDataBaseAddressByIndex(vv.Value)), memory.ReadDataSizeByIndex(vv.Value), stream);
            }
            return mfb;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <param name="size"></param>
        /// <param name="stream"></param>
        public unsafe static void RecordToLog(this IntPtr address, int size, Stream stream)
        {
            using (System.IO.UnmanagedMemoryStream ums = new UnmanagedMemoryStream((byte*)address, size))
            {
                ums.CopyTo(stream);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="size"></param>
        /// <param name="stream"></param>
        public unsafe static void RecordToLog2(this IntPtr memory,int size, Stream stream)
        {
            int ltmp = (int)size;
            var source = memory;

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
        public static void MakeMemoryBusy(this HisDataMemoryBlockCollection3 memory)
        {
            memory.IncRef();
            LoggerService.Service.Info("MemoryBlock", "make " + memory.Name + " is busy.....");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this HisDataMemoryBlockCollection3 memory)
        {
            memory.DecRef();
            LoggerService.Service.Info("MemoryBlock", "make " + memory.Name + " is ready !");
        }


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="memory"></param>
        //public static void Dump(this HisDataMemoryBlockCollection3 memory)
        //{
        //    string fileName = memory.Name + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm_ss") + ".dmp";
        //    fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalFixedMemoryBlock).Assembly.Location), fileName);
        //    using (var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
        //    {
        //        foreach (var vv in memory.TagAddress)
        //        {
        //            vv.Value?.Dump(stream);
        //        }
        //    }
        //}
    }

}
