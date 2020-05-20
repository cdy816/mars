//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/29 13:52:48.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public struct IntPointData
    {
        public static IntPointData Empty = new IntPointData();

        public IntPointData(int x,int y)
        {
            X = x;
            Y = y;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
        public IntPointData(uint x, uint y)
        {
            X = (int)x;
            Y = (int)y;
        }
        /// <summary>
        /// 
        /// </summary>
        public int X { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public int Y { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IntPointData ToIntPointData(object value)
        {
            if (value is IntPointData) return (IntPointData)value;
            else if (value is UIntPointData) return new IntPointData(((UIntPointData)value).X, ((UIntPointData)value).Y);
            else if (value is IntPoint3Data) return new IntPointData(((IntPoint3Data)value).X, ((IntPoint3Data)value).Y);
            else if (value is UIntPoint3Data) return new IntPointData(((UIntPoint3Data)value).X, ((UIntPoint3Data)value).Y);
            else if (value is LongPointData) return new IntPointData((int)(((LongPointData)value).X), (int)(((LongPointData)value).Y));
            else if (value is ULongPointData) return new IntPointData((int)(((ULongPointData)value).X), (int)(((ULongPointData)value).Y));
            else if (value is LongPoint3Data) return new IntPointData((int)(((LongPoint3Data)value).X), (int)(((LongPoint3Data)value).Y));
            else if (value is ULongPoint3Data) return new IntPointData((int)(((ULongPoint3Data)value).X), (int)(((ULongPoint3Data)value).Y));
            return Empty;
        }

        public override string ToString()
        {
            return X+","+Y;
        }

    }
    /// <summary>
    /// 
    /// </summary>
    public struct IntPoint3Data
    {
        public IntPoint3Data(int x, int y,int z)
        {
            X = x;
            Y = y;
            Z = z;
        }

        public IntPoint3Data(uint x, uint y, uint z)
        {
            X = (int)x;
            Y = (int)y;
            Z = (int)z;
        }
        public int X { get; set; }
        public int Y { get; set; }
        public int Z { get; set; }

        public override string ToString()
        {
            return X + "," + Y+","+Z;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct UIntPointData
    {
        public UIntPointData(uint x, uint y)
        {
            X = x;
            Y = y;
        }

        public UIntPointData(int x, int y)
        {
            X = (uint)x;
            Y = (uint)y;
        }
        public uint X { get; set; }
        public uint Y { get; set; }

        public override string ToString()
        {
            return X + "," + Y;
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public struct UIntPoint3Data
    {
        public UIntPoint3Data(uint x, uint y, uint z)
        {
            X = x;
            Y = y;
            Z = z;
        }

        public UIntPoint3Data(int x, int y, int z)
        {
            X = (uint)x;
            Y = (uint)y;
            Z = (uint)z;
        }
        public uint X { get; set; }
        public uint Y { get; set; }
        public uint Z { get; set; }


        public override string ToString()
        {
            return X + "," + Y + "," + Z;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct LongPointData
    {
        public LongPointData(long x, long y)
        {
            X = x;
            Y = y;
        }

        public LongPointData(ulong x, ulong y)
        {
            X = (long)x;
            Y = (long)y;
        }
        public long X { get; set; }
        public long Y { get; set; }

        public override string ToString()
        {
            return X + "," + Y;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct LongPoint3Data
    {
        public LongPoint3Data(long x, long y, long z)
        {
            X = x;
            Y = y;
            Z = z;
        }

        public LongPoint3Data(ulong x, ulong y, ulong z)
        {
            X = (long)x;
            Y = (long)y;
            Z = (long)z;
        }
        public long X { get; set; }
        public long Y { get; set; }
        public long Z { get; set; }

        public override string ToString()
        {
            return X + "," + Y + "," + Z;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct ULongPointData
    {
        public ULongPointData(ulong x, ulong y)
        {
            X = x;
            Y = y;
        }

        public ULongPointData(long x, long y)
        {
            X = (ulong)x;
            Y = (ulong)y;
        }
        public ulong X { get; set; }
        public ulong Y { get; set; }

        public override string ToString()
        {
            return X + "," + Y;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct ULongPoint3Data
    {
        public ULongPoint3Data(ulong x, ulong y, ulong z)
        {
            X = x;
            Y = y;
            Z = z;
        }

        public ULongPoint3Data(long x, long y, long z)
        {
            X = (ulong)x;
            Y = (ulong)y;
            Z = (ulong)z;
        }

        public ulong X { get; set; }
        public ulong Y { get; set; }
        public ulong Z { get; set; }

        public override string ToString()
        {
            return X + "," + Y + "," + Z;
        }
    }

}
