//==============================================================
//  Copyright (C) 2020 Chongdaoyang  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/2 21:40:41.
//  Version 1.0
//  CDYWORK
//==============================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cdy.Tag
{

    public interface IMemoryBlock
    {

        #region ... Variables  ...


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        #endregion ...Constructor...

        #region ... Properties ...
        
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int BufferItemSize { get; }

        /// <summary>
        /// 
        /// </summary>
        public List<IntPtr> Handles { get; }

        /// <summary>
        /// 
        /// </summary>
        public long Length
        {
            get;
        }

        /// <summary>
        /// 
        /// </summary>
        public long Position
        {
            get;
            set;
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
            get;
        }


        ///// <summary>
        ///// 
        ///// </summary>
        //internal IntPtr Handles
        //{
        //    get;
        //    set;
        //}


        #endregion ...Properties...

        #region ... Methods    ...

        public IMemoryBlock WriteToStream(Stream stream, long offset, long len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void CheckAndResize(long size);


        /// <summary>
        /// 是否繁忙
        /// </summary>
        /// <returns></returns>
        public bool IsBusy();



        /// <summary>
        /// 重新分配对象
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc(long size);


        /// <summary>
        /// 清空内存
        /// </summary>
        public IMemoryBlock Clear();




        #region ReadAndWrite



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(DateTime value);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(long value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>

        public void Write(ulong value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(int value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(uint value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(short value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(ushort value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(float value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(double value);



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="encoding"></param>
        public void Write(string value, Encoding encoding);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(string value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(byte value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(byte[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public void Write(byte[] values, int offset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(Memory<byte> values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLong(long offset, long value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLongDirect(long offset, long value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteULong(long offset, ulong value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteFloat(long offset, float value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDouble(long offset, double value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteMemory(long offset, Memory<byte> values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytes(long offset, byte[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytesDirect(long offset, byte[] values);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteMemory(long offset, Memory<byte> values, int valueoffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytes(long offset, byte[] values, int valueoffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueOffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long offset, IntPtr values, int valueOffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long offset, byte[] values, int valueoffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByte(long offset, byte value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByteDirect(long offset, byte value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteByte(byte value);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDatetime(long offset, DateTime value);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteInt(long offset, int value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteIntDirect(long offset, int value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUInt(long offset, uint value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUIntDirect(long offset, uint value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShort(long offset, short value);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShortDirect(long offset, short value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShort(long offset, ushort value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShortDirect(long offset, ushort value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public void WriteString(long offset, string value, Encoding encode);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public void WriteStringDirect(long offset, string value, Encoding encode);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public DateTime ReadDateTime(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<DateTime> ReadDateTimes(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public int ReadInt(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<int> ReadInts(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public uint ReadUInt(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<uint> ReadUInts(long offset, int count);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public short ReadShort(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ushort ReadUShort(long offset);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public float ReadFloat(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<float> ReadFloats(long offset, int count);



        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public double ReadDouble(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<double> ReadDoubles(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public long ReadLong(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ulong ReadULong(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte ReadByte(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset, Encoding encoding);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte[] ReadBytes(long offset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public byte ReadByte();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long ReadLong();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<long> ReadLongs(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ulong ReadULong();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<ulong> ReadULongs(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int ReadInt();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public uint ReadUInt();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public float ReadFloat();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public double ReadDouble();



        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public short ReadShort();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<short> ReadShorts(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ushort ReadUShort();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<ushort> ReadUShorts(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DateTime ReadDateTime();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public string ReadString(Encoding encoding);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string ReadString();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<string> ReadStrings(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="len"></param>
        /// <returns></returns>
        public byte[] ReadBytes(int len);

        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public interface IMemoryFixedBlock
    {

        #region ... Variables  ...


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public long Length
        {
            get;
        }

        /// <summary>
        /// 
        /// </summary>
        public long Position
        {
            get;
            set;
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
            get;
        }


        ///// <summary>
        ///// 
        ///// </summary>
        //internal IntPtr Handles
        //{
        //    get;
        //    set;
        //}


        #endregion ...Properties...

        #region ... Methods    ...

        public void CheckAndResize(long size);


        /// <summary>
        /// 是否繁忙
        /// </summary>
        /// <returns></returns>
        public bool IsBusy();



        /// <summary>
        /// 重新分配对象
        /// </summary>
        /// <param name="size"></param>
        public void ReAlloc(long size);


        /// <summary>
        /// 清空内存
        /// </summary>
        public IMemoryFixedBlock Clear();




        #region ReadAndWrite



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(DateTime value);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(long value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>

        public void Write(ulong value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(int value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(uint value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(short value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(ushort value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(float value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(double value);



        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="encoding"></param>
        public void Write(string value, Encoding encoding);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(string value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Write(byte value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(byte[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public void Write(byte[] values, int offset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(Memory<byte> values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLong(long offset, long value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteLongDirect(long offset, long value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteULong(long offset, ulong value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteFloat(long offset, float value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDouble(long offset, double value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteMemory(long offset, Memory<byte> values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytes(long offset, byte[] values);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        public void WriteBytesDirect(long offset, byte[] values);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteMemory(long offset, Memory<byte> values, int valueoffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytes(long offset, byte[] values, int valueoffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueOffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long offset, IntPtr values, int valueOffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="values"></param>
        /// <param name="valueoffset"></param>
        /// <param name="len"></param>
        public void WriteBytesDirect(long offset, byte[] values, int valueoffset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByte(long offset, byte value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteByteDirect(long offset, byte value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void WriteByte(byte value);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteDatetime(long offset, DateTime value);
        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteInt(long offset, int value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteIntDirect(long offset, int value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUInt(long offset, uint value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUIntDirect(long offset, uint value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShort(long offset, short value);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteShortDirect(long offset, short value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShort(long offset, ushort value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        public void WriteUShortDirect(long offset, ushort value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public void WriteString(long offset, string value, Encoding encode);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="value"></param>
        /// <param name="encode"></param>
        public void WriteStringDirect(long offset, string value, Encoding encode);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public DateTime ReadDateTime(long offset);

        public List<DateTime> ReadDateTimes(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public int ReadInt(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<int> ReadInts(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public uint ReadUInt(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<uint> ReadUInts(long offset, int count);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public short ReadShort(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ushort ReadUShort(long offset);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public float ReadFloat(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<float> ReadFloats(long offset, int count);



        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public double ReadDouble(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<double> ReadDoubles(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public long ReadLong(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public ulong ReadULong(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte ReadByte(long offset);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset, Encoding encoding);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public string ReadString(long offset);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="target"></param>
        /// <param name="len"></param>
        public void ReadBytes(long offset, byte[] target, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <returns></returns>
        public byte[] ReadBytes(long offset, int len);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public byte ReadByte();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public long ReadLong();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<long> ReadLongs(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ulong ReadULong();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<ulong> ReadULongs(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int ReadInt();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public uint ReadUInt();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public float ReadFloat();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public double ReadDouble();



        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public short ReadShort();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<short> ReadShorts(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ushort ReadUShort();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<ushort> ReadUShorts(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public DateTime ReadDateTime();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="encoding"></param>
        /// <returns></returns>
        public string ReadString(Encoding encoding);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string ReadString();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <returns></returns>
        public List<string> ReadStrings(long offset, int count);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="len"></param>
        /// <returns></returns>
        public byte[] ReadBytes(int len);

        #endregion


        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(IMemoryFixedBlock target, long sourceStart, long targetStart, long len);


        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="sourceStart"></param>
        /// <param name="targetStart"></param>
        /// <param name="len"></param>
        public void CopyTo(IMemoryBlock target, long sourceStart, long targetStart, long len);


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
