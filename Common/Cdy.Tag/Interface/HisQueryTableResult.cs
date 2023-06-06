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
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe class HisQueryTableResult:IDisposable
    {

        #region ... Variables  ...
        private IntPtr handle;
        private Dictionary<string,TagType> mColumns = new Dictionary<string, TagType>();
        private int mColumnDataSize = 0;
        private int mMaxRowCount = 0;
        private int mRowCount = 0;
        private int mSize = 0;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, TagType> Columns
        {
            get { return mColumns; }
        }

        /// <summary>
        /// 
        /// </summary>
        public IntPtr Address
        {
            get
            {
                return handle;
            }
        }


        /// <summary>
        /// 当前添加数值个数
        /// </summary>
        public int RowCount
        {
            get
            {
                return mRowCount;
            }
            set
            {
                mRowCount = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int AvaiableSize
        {
            get
            {
                return RowCount * mColumnDataSize;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int MaxRowCount
        {
            get
            {
                return mMaxRowCount;
            }
        }

        


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 序列号Meta数据
        /// </summary>
        /// <returns></returns>
        public string SeriseMeta()
        {
            StringBuilder sb=new StringBuilder();
            sb.AppendLine(mRowCount.ToString());
            sb.AppendLine(mColumnDataSize.ToString());
            foreach(var vv in mColumns)
            {
                sb.Append(vv.Key+","+(byte)vv.Value+";");
            }
            if(mColumns.Count > 0)
            {
                sb.Length = sb.Length - 1;
            }
            sb.AppendLine();
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="str"></param>
        public void FromStringToMeta(string str)
        {
            string[] sval = str.Split("\r\n",StringSplitOptions.RemoveEmptyEntries);
            this.mRowCount = int.Parse(sval[0]);
            this.mColumnDataSize = int.Parse(sval[1]);
            string[] ss = sval[2].Split(";",StringSplitOptions.RemoveEmptyEntries);
            this.Columns.Clear();
            foreach (var vvv in ss)
            {
                var stmp = vvv.Split(",");
                if(stmp.Length>1)
                this.Columns.Add(stmp[0], (TagType)int.Parse(stmp[1]));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="columnName"></param>
        /// <param name="type"></param>
        public void AddColumn(string columnName,TagType type)
        {
            if (!mColumns.ContainsKey(columnName))
            {
                mColumns.Add(columnName, type);
            }
            else
            {
                mColumns[columnName] = type;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="csize"></param>
        public void Init2(int csize)
        {
            handle = Marshal.AllocHGlobal(csize);
            mSize = csize;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public void Init(int mRowCount)
        {
            int isize = 8;
            foreach(var vv in mColumns)
            {
                isize += GetDataLen(vv.Value);
            }
            mColumnDataSize = isize;
            mMaxRowCount = mRowCount;

            int csize = mRowCount * (mColumnDataSize+8);

            int cc = csize / 1024;
            if(csize % 1024!=0)
            {
                cc++;
            }

            csize = cc * 1024;

            handle = Marshal.AllocHGlobal(csize);
            mSize = csize;

            new Span<byte>((void*)handle,csize).Clear();
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="value"></param>
        /// <param name="time"></param>
        /// <param name="qulity"></param>
        public void Add(DateTime time,params object[] values)
        {
            int i = 0;
            var baseaddr = mRowCount * mColumnDataSize;
            int moffset = 0;
            MemoryHelper.WriteDateTime((void*)handle, baseaddr + moffset, time);
            moffset += 8;
            foreach (var vv in mColumns)
            {
                if(i>=values.Length)
                {
                    break;
                }
                switch(vv.Value)
                {
                    case TagType.Bool:
                        MemoryHelper.WriteByte((void*)handle, baseaddr + moffset,((bool)GetTargetValue(vv.Value, values[i]))?(byte)1:(byte)0);
                        moffset += 1;
                        break;
                    case TagType.Byte:
                        MemoryHelper.WriteByte((void*)handle, baseaddr + moffset, (byte)GetTargetValue(vv.Value, values[i]));
                        moffset += 1;
                        break;
                    case TagType.Short:
                        MemoryHelper.WriteShort((void*)handle, baseaddr + moffset, (short)GetTargetValue(vv.Value, values[i]));
                        moffset += 2;
                        break;
                    case TagType.UShort:
                        MemoryHelper.WriteUShort((void*)handle, baseaddr + moffset, (ushort)GetTargetValue(vv.Value, values[i]));
                        moffset += 2;
                        break;
                    case TagType.Int:
                        MemoryHelper.WriteInt32((void*)handle, baseaddr + moffset, (int)GetTargetValue(vv.Value, values[i]));
                        moffset += 4;
                        break;
                    case TagType.UInt:
                        MemoryHelper.WriteUInt32((void*)handle, baseaddr + moffset, (uint)GetTargetValue(vv.Value, values[i]));
                        moffset += 4;
                        break;
                    case TagType.Long:
                        MemoryHelper.WriteInt64((void*)handle, baseaddr + moffset, (long)GetTargetValue(vv.Value, values[i]));
                        moffset += 8;
                        break;
                    case TagType.ULong:
                        MemoryHelper.WriteUInt64((void*)handle, baseaddr + moffset, (ulong)GetTargetValue(vv.Value, values[i]));
                        moffset += 8;
                        break;
                    case TagType.Double:
                        MemoryHelper.WriteDouble((void*)handle, baseaddr + moffset, (double)GetTargetValue(vv.Value, values[i]));
                        moffset += 8;
                        break;
                    case TagType.Float:
                        MemoryHelper.WriteFloat((void*)handle, baseaddr + moffset, (float)GetTargetValue(vv.Value, values[i]));
                        moffset += 4;
                        break;
                    case TagType.String:
                        moffset += MemoryHelper.WriteString((void*)handle, baseaddr + moffset, (string)GetTargetValue(vv.Value, values[i]));
                        break;
                    case TagType.DateTime:
                        MemoryHelper.WriteDateTime((void*)handle, baseaddr + moffset, (DateTime)GetTargetValue(vv.Value, values[i]));
                        moffset += 8;
                        break;
                    case TagType.IntPoint:
                        var val = (IntPointData)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteInt32((void*)handle, baseaddr + moffset,val.X);
                        moffset += 4;
                        MemoryHelper.WriteInt32((void*)handle, baseaddr + moffset, val.Y);
                        moffset += 4;
                        break;
                    case TagType.UIntPoint:
                        var uval = (UIntPointData)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteUInt32((void*)handle, baseaddr + moffset, uval.X);
                        moffset += 4;
                        MemoryHelper.WriteUInt32((void*)handle, baseaddr + moffset, uval.Y);
                        moffset += 4;
                        break;
                    case TagType.LongPoint:
                        var lval = (LongPointData)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteInt64((void*)handle, baseaddr + moffset, lval.X);
                        moffset += 8;
                        MemoryHelper.WriteInt64((void*)handle, baseaddr + moffset, lval.Y);
                        moffset += 8;
                        break;
                    case TagType.ULongPoint:
                        var luval = (ULongPointData)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteUInt64((void*)handle, baseaddr + moffset, luval.X);
                        moffset += 8;
                        MemoryHelper.WriteUInt64((void*)handle, baseaddr + moffset, luval.Y);
                        moffset += 8;
                        break;
                    case TagType.IntPoint3:
                        var val3 = (IntPoint3Data)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteInt32((void*)handle, baseaddr + moffset, val3.X);
                        moffset += 4;
                        MemoryHelper.WriteInt32((void*)handle, baseaddr + moffset, val3.Y);
                        moffset += 4;
                        MemoryHelper.WriteInt32((void*)handle, baseaddr + moffset, val3.Z);
                        moffset += 4;
                        break;
                    case TagType.UIntPoint3:
                        var uval3 = (UIntPoint3Data)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteUInt32((void*)handle, baseaddr + moffset, uval3.X);
                        moffset += 4;
                        MemoryHelper.WriteUInt32((void*)handle, baseaddr + moffset, uval3.Y);
                        moffset += 4;
                        MemoryHelper.WriteUInt32((void*)handle, baseaddr + moffset, uval3.Z);
                        moffset += 4;
                        break;
                    case TagType.LongPoint3:
                        var lval3 = (LongPoint3Data)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteInt64((void*)handle, baseaddr + moffset, lval3.X);
                        moffset += 8;
                        MemoryHelper.WriteInt64((void*)handle, baseaddr + moffset, lval3.Y);
                        moffset += 8;
                        MemoryHelper.WriteInt64((void*)handle, baseaddr + moffset, lval3.Z);
                        moffset += 8;
                        break;
                    case TagType.ULongPoint3:
                        var luval3 = (ULongPoint3Data)GetTargetValue(vv.Value, values[i]);
                        MemoryHelper.WriteUInt64((void*)handle, baseaddr + moffset, luval3.X);
                        moffset += 8;
                        MemoryHelper.WriteUInt64((void*)handle, baseaddr + moffset, luval3.Y);
                        moffset += 8;
                        MemoryHelper.WriteUInt64((void*)handle, baseaddr + moffset, luval3.Z);
                        moffset += 8;
                        break;
                }
                i++;
            }
            mRowCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="row"></param>
        /// <returns></returns>
        public Tuple<DateTime, object[]> Read(int row)
        {
            if (row >= mRowCount) return null;

            DateTime time;
            object[] vals = new object[mColumns.Count];
            var baseaddr = row * mColumnDataSize;
            int moffset = 0;
            time = MemoryHelper.ReadDateTime((void*)handle, baseaddr + moffset);
            moffset += 8;
            int i = 0;
            foreach ( var vv in mColumns )
            {
                switch (vv.Value)
                {
                    case TagType.Bool:
                        vals[i]= MemoryHelper.ReadByte((void*)handle, baseaddr + moffset);
                        moffset += 1;
                        break;
                    case TagType.Byte:
                        vals[i] = MemoryHelper.ReadByte((void*)handle, baseaddr + moffset);
                        moffset += 1;
                        break;
                    case TagType.Short:
                        vals[i] = MemoryHelper.ReadShort((void*)handle, baseaddr + moffset);
                        moffset += 2;
                        break;
                    case TagType.UShort:
                        vals[i] = MemoryHelper.ReadUShort((void*)handle, baseaddr + moffset);
                        moffset += 2;
                        break;
                    case TagType.Int:
                        vals[i] = MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset);
                        moffset += 4;
                        break;
                    case TagType.UInt:
                        vals[i] = MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset);
                        moffset += 4;
                        break;
                    case TagType.Long:
                        vals[i] = MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset);
                        moffset += 8;
                        break;
                    case TagType.ULong:
                        vals[i] = MemoryHelper.ReadUInt64((void*)handle, baseaddr + moffset);
                        moffset += 8;
                        break;
                    case TagType.Double:
                        vals[i] = MemoryHelper.ReadDouble((void*)handle, baseaddr + moffset);
                        moffset += 8;
                        break;
                    case TagType.Float:
                        vals[i] = MemoryHelper.ReadFloat((void*)handle, baseaddr + moffset);
                        moffset += 4;
                        break;
                    case TagType.String:
                        vals[i]= MemoryHelper.ReadString((void*)handle, baseaddr + moffset,out int size);
                        moffset+= size;
                        break;
                    case TagType.DateTime:
                        vals[i] = MemoryHelper.ReadDateTime((void*)handle, baseaddr + moffset);
                        moffset += 8;
                        break;
                    case TagType.IntPoint:
                        vals[i] = new IntPointData(MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset),MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset+4));
                        moffset += 8;
                        break;
                    case TagType.UIntPoint:
                        vals[i] = new UIntPointData(MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset), MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset+4));
                        moffset += 8;
                        break;
                    case TagType.LongPoint:
                        vals[i] = new LongPointData(MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 8));
                        moffset += 16;
                        break;
                    case TagType.ULongPoint:
                        vals[i] = new ULongPointData(MemoryHelper.ReadUInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadUInt64((void*)handle, baseaddr + moffset + 8));
                        moffset += 16;
                        break;
                    case TagType.IntPoint3:
                        vals[i] = new IntPoint3Data(MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset + 4), MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset + 8));
                        moffset += 12;
                        break;
                    case TagType.UIntPoint3:
                        vals[i] = new UIntPoint3Data(MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset), MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset + 4), MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset + 8));
                        moffset += 12;
                        break;
                    case TagType.LongPoint3:
                        vals[i] = new LongPoint3Data(MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 8), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 16));
                        moffset += 24;
                        break;
                    case TagType.ULongPoint3:
                        vals[i] = new ULongPoint3Data(MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 8), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 16));
                        moffset += 24;
                        break;
                }
                i++;
            }
            return new Tuple<DateTime, object[]>(time,vals);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Tuple<DateTime, object[]>> ReadRows()
        {
            for(int i=0;i<mRowCount;i++)
            {
                yield return Read(i);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        public IEnumerable<object> ReadColumns(string column)
        {
            int moffset = 8;
            foreach(var vv in mColumns)
            {
                if(vv.Key == column)
                {
                    break;
                }
                moffset += GetDataLen(vv.Value);
            }
            TagType tp = mColumns[column];
            object val = null;
            List<object> re = new List<object>();
            for(int i=0;i<mRowCount;i++)
            {
                var baseaddr = i * mColumnDataSize;
                switch (tp)
                {
                    case TagType.Bool:
                         val = MemoryHelper.ReadByte((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.Byte:
                        val = MemoryHelper.ReadByte((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.Short:
                        val = MemoryHelper.ReadShort((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.UShort:
                        val = MemoryHelper.ReadUShort((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.Int:
                        val = MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.UInt:
                        val = MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.Long:
                        val = MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.ULong:
                        val = MemoryHelper.ReadUInt64((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.Double:
                        val = MemoryHelper.ReadDouble((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.Float:
                        val = MemoryHelper.ReadFloat((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.String:
                        val = MemoryHelper.ReadString((void*)handle, baseaddr + moffset, out int size);
                        break;
                    case TagType.DateTime:
                        val = MemoryHelper.ReadDateTime((void*)handle, baseaddr + moffset);
                        break;
                    case TagType.IntPoint:
                        val = new IntPointData(MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset + 4));
                        break;
                    case TagType.UIntPoint:
                        val = new UIntPointData(MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset), MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset + 4));
                        break;
                    case TagType.LongPoint:
                        val = new LongPointData(MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 8));
                        break;
                    case TagType.ULongPoint:
                        val = new ULongPointData(MemoryHelper.ReadUInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadUInt64((void*)handle, baseaddr + moffset + 8));
                        break;
                    case TagType.IntPoint3:
                        val = new IntPoint3Data(MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset + 4), MemoryHelper.ReadInt32((void*)handle, baseaddr + moffset + 8));
                        break;
                    case TagType.UIntPoint3:
                        val = new UIntPoint3Data(MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset), MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset + 4), MemoryHelper.ReadUInt32((void*)handle, baseaddr + moffset + 8));
                        break;
                    case TagType.LongPoint3:
                        val = new LongPoint3Data(MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 8), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 16));
                        break;
                    case TagType.ULongPoint3:
                        val = new ULongPoint3Data(MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 8), MemoryHelper.ReadInt64((void*)handle, baseaddr + moffset + 16));
                        break;
                }
                re.Add(val);
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public object GetTargetValue(TagType typ,object value)
        {
            switch (typ)
            {
                case TagType.Bool:
                    return BoolValueConvert(value);
                case TagType.Byte:
                    return ByteValueConvert(value);
                case TagType.Short:
                    return ShortValueConvert(value);
                case TagType.UShort:
                    return UShortValueConvert(value);
                case TagType.Int:
                    return IntValueConvert(value);
                case TagType.UInt:
                    return UIntValueConvert(value);
                case TagType.Long:
                    return LongValueConvert(value);
                case TagType.ULong:
                    return ULongValueConvert(value);
                case TagType.Float:
                    return FloatValueConvert(value);
                case TagType.Double:
                    return DoubleValueConvert(value);
                case TagType.DateTime:
                    return DatetimeValueConvert(value);
                case TagType.String:
                    return StringValueConvert(value);
                case TagType.IntPoint:
                    return IntPointData.ToIntPointData(value);
                case TagType.UIntPoint:
                    return UIntPointData.ToPointData(value);
                case TagType.IntPoint3:
                    return IntPoint3Data.ToPointData(value);
                case TagType.UIntPoint3:
                    return UIntPoint3Data.ToPointData(value);
                case TagType.LongPoint:
                    return LongPointData.ToPointData(value);
                case TagType.ULongPoint:
                    return ULongPointData.ToPointData(value);
                case TagType.LongPoint3:
                    return LongPoint3Data.ToPointData(value);
                case TagType.ULongPoint3:
                    return ULongPoint3Data.ToPointData(value);
            }
            return value;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool BoolValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch(code)
            {
                
                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return false;
                default:
                    try
                    {
                        return Convert.ToBoolean(value);
                    }
                    catch
                    {
                    }
                    break;
            }
           return  false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public byte ByteValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                default:
                    try
                    {
                        return Convert.ToByte(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public short ShortValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                default:
                    try
                    {
                        return Convert.ToInt16(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public ushort UShortValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                default:
                    try
                    {
                        return Convert.ToUInt16(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }


        public int IntValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                default:
                    try
                    {
                        return Convert.ToInt32(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public uint UIntValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                default:
                    try
                    {
                        return Convert.ToUInt32(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public long LongValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                    return 0;
                case TypeCode.DateTime:
                    return ((DateTime)(value)).Ticks;
                default:
                    try
                    {
                        return Convert.ToInt64(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public ulong ULongValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                    return 0;
                case TypeCode.DateTime:
                    return (ulong)((DateTime)(value)).Ticks;
                default:
                    try
                    {
                        return Convert.ToUInt64(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public double DoubleValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                default:
                    try
                    {
                        return Convert.ToDouble(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        public double FloatValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                case TypeCode.DateTime:
                    return 0;
                default:
                    try
                    {
                        return Convert.ToSingle(value);
                    }
                    catch
                    {
                    }
                    break;
            }
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public string StringValueConvert(object value)
        {
            if (value == null) return string.Empty;

            return Convert.ToString(value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public DateTime DatetimeValueConvert(object value)
        {
            var code = Convert.GetTypeCode(value);
            switch (code)
            {

                case TypeCode.Empty:
                case TypeCode.Object:
                    return DateTime.MinValue;
                case TypeCode.DateTime:
                case TypeCode.String:
                    return Convert.ToDateTime(value);
                case TypeCode.Int64:
                case TypeCode.UInt64:
                    return DateTime.FromBinary(Convert.ToInt64(value));
            }
            return DateTime.MinValue;
        }

        public void CheckAndResize()
        {
            if(mRowCount>=mMaxRowCount)
            {
                Resize((int)(mRowCount * 1.5));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Resize(int count)
        {
            if(count<mMaxRowCount) return;

            var newsize = count * (8 + mColumnDataSize);

            int cc = newsize / 1024;
            if (newsize % 1024 != 0)
            {
                cc++;
            }

            newsize = cc * 1024;

            IntPtr nhd = Marshal.AllocHGlobal(newsize);

            Buffer.MemoryCopy((void*)handle, (void*)nhd, newsize, mSize);

            Marshal.FreeHGlobal(handle);
            handle = nhd;

            mMaxRowCount = count;
            mSize = newsize;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public HisQueryTableResult ConvertUTCTimeToLocal()
        {
            for(int i=0;i<mRowCount;i++)
            {
                DateTime dt = MemoryHelper.ReadDateTime((void*)handle, i * mColumnDataSize);
                MemoryHelper.WriteDateTime((void*)handle, i * mColumnDataSize,dt.ToLocalTime());
            }
            return this;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int GetDataLen(TagType type)
        {
            switch(type)
            {
                case TagType.Bool:
                    return 1;
                case TagType.Byte:
                    return 1;
                case TagType.Short:
                case TagType.UShort:
                    return 2;
                case TagType.Int:
                case TagType.UInt:
                    return 4;
                case TagType.Long:
                case TagType.ULong:
                    return 8;
                case TagType.Float:
                    return 4;
                case TagType.Double:
                case TagType.DateTime:
                    return 8;
                case TagType.String:
                    return Const.StringSize;
                case TagType.IntPoint:
                case TagType.UIntPoint:
                    return 8;
                case TagType.IntPoint3:
                case TagType.UIntPoint3:
                    return 12;
                case TagType.LongPoint:
                case TagType.ULongPoint:
                    return 16;
                case TagType.LongPoint3:
                case TagType.ULongPoint3:
                    return 24;
            }
          
            return 0;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            mRowCount = 0;
            Unsafe.InitBlockUnaligned((void*)handle, 0, (uint)mSize);
        }


        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            Marshal.FreeHGlobal(handle);
        }



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

}
