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
using System.Security;
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return X+","+Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IntPointData FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new IntPointData(int.Parse(sval[0]), int.Parse(sval[1]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator == (IntPointData b, IntPointData c)
        {
            return b.X == c.X && b.Y == c.Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(IntPointData b, IntPointData c)
        {
            return b.X != c.X || b.Y != c.Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is IntPointData)
            {
                return this == (IntPointData)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }


    }
    /// <summary>
    /// 
    /// </summary>
    public struct IntPoint3Data
    {
        public static IntPoint3Data Empty = new IntPoint3Data();

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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IntPoint3Data FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new IntPoint3Data(int.Parse(sval[0]), int.Parse(sval[1]), int.Parse(sval[2]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator ==(IntPoint3Data b, IntPoint3Data c)
        {
            return b.X == c.X && b.Y == c.Y && b.Z == c.Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(IntPoint3Data b, IntPoint3Data c)
        {
            return b.X != c.X || b.Y != c.Y || b.Z != c.Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if(obj is IntPoint3Data)
            {
                return this == (IntPoint3Data)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        // <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static IntPoint3Data ToPointData(object value)
        {
            if (value is IntPoint3Data) return (IntPoint3Data)value;
            else if (value is UIntPoint3Data) return new IntPoint3Data(((UIntPoint3Data)value).X, ((UIntPoint3Data)value).Y, ((UIntPoint3Data)value).Z);
            else if (value is IntPointData) return new IntPoint3Data(((IntPointData)value).X, ((IntPointData)value).Y, 0);
            else if (value is UIntPointData) return new IntPoint3Data(((UIntPointData)value).X, ((UIntPointData)value).Y, 0);
            else if (value is LongPointData) return new IntPoint3Data((int)(((LongPointData)value).X), (int)(((LongPointData)value).Y), 0);
            else if (value is ULongPointData) return new IntPoint3Data((int)(((ULongPointData)value).X), (int)(((ULongPointData)value).Y), 0);
            else if (value is LongPoint3Data) return new IntPoint3Data((int)(((LongPoint3Data)value).X), (int)(((LongPoint3Data)value).Y), (int)(((LongPoint3Data)value).Z));
            else if (value is ULongPoint3Data) return new IntPoint3Data((int)(((ULongPoint3Data)value).X), (int)(((ULongPoint3Data)value).Y), (int)(((ULongPoint3Data)value).Z));
            return Empty;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct UIntPointData
    {
        public static UIntPointData Empty = new UIntPointData();

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


        public static UIntPointData FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new UIntPointData(uint.Parse(sval[0]), uint.Parse(sval[1]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator ==(UIntPointData b, UIntPointData c)
        {
            return b.X == c.X && b.Y == c.Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(UIntPointData b, UIntPointData c)
        {
            return b.X != c.X || b.Y != c.Y;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is UIntPointData)
            {
                return this == (UIntPointData)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static UIntPointData ToPointData(object value)
        {
            if (value is UIntPointData) return (UIntPointData)value;
            else if (value is IntPointData) return new UIntPointData(((IntPointData)value).X, ((IntPointData)value).Y);
            else if (value is IntPoint3Data) return new UIntPointData(((IntPoint3Data)value).X, ((IntPoint3Data)value).Y);
            else if (value is UIntPoint3Data) return new UIntPointData(((UIntPoint3Data)value).X, ((UIntPoint3Data)value).Y);
            else if (value is LongPointData) return new UIntPointData((int)(((LongPointData)value).X), (int)(((LongPointData)value).Y));
            else if (value is ULongPointData) return new UIntPointData((int)(((ULongPointData)value).X), (int)(((ULongPointData)value).Y));
            else if (value is LongPoint3Data) return new UIntPointData((int)(((LongPoint3Data)value).X), (int)(((LongPoint3Data)value).Y));
            else if (value is ULongPoint3Data) return new UIntPointData((int)(((ULongPoint3Data)value).X), (int)(((ULongPoint3Data)value).Y));
            return Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
    }
    /// <summary>
    /// 
    /// </summary>
    public struct UIntPoint3Data
    {
        public static UIntPoint3Data Empty = new UIntPoint3Data();
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return X + "," + Y + "," + Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static UIntPoint3Data FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new UIntPoint3Data(uint.Parse(sval[0]), uint.Parse(sval[1]), uint.Parse(sval[2]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator ==(UIntPoint3Data b, UIntPoint3Data c)
        {
            return b.X == c.X && b.Y == c.Y && b.Z == c.Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(UIntPoint3Data b, UIntPoint3Data c)
        {
            return b.X != c.X || b.Y != c.Y || b.Z != c.Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is UIntPoint3Data)
            {
                return this == (UIntPoint3Data)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static UIntPoint3Data ToPointData(object value)
        {
            if (value is UIntPoint3Data) return (UIntPoint3Data)value;
            else if (value is IntPoint3Data) return new UIntPoint3Data(((IntPoint3Data)value).X, ((IntPoint3Data)value).Y, ((IntPoint3Data)value).Z);
            else if (value is IntPointData) return new UIntPoint3Data(((IntPointData)value).X, ((IntPointData)value).Y,0);
            else if (value is UIntPointData) return new UIntPoint3Data(((UIntPointData)value).X, ((UIntPointData)value).Y,0);
            else if (value is LongPointData) return new UIntPoint3Data((int)(((LongPointData)value).X), (int)(((LongPointData)value).Y),0);
            else if (value is ULongPointData) return new UIntPoint3Data((int)(((ULongPointData)value).X), (int)(((ULongPointData)value).Y),0);
            else if (value is LongPoint3Data) return new UIntPoint3Data((int)(((LongPoint3Data)value).X), (int)(((LongPoint3Data)value).Y), (int)(((LongPoint3Data)value).Z));
            else if (value is ULongPoint3Data) return new UIntPoint3Data((int)(((ULongPoint3Data)value).X), (int)(((ULongPoint3Data)value).Y), (int)(((ULongPoint3Data)value).Z));
            return Empty;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct LongPointData
    {
        public static LongPointData Empty = new LongPointData();
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
        /// <summary>
        /// 
        /// </summary>
        public long X { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public long Y { get; set; }

        public override string ToString()
        {
            return X + "," + Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static LongPointData FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new LongPointData(long.Parse(sval[0]), long.Parse(sval[1]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator ==(LongPointData b, LongPointData c)
        {
            return b.X == c.X && b.Y == c.Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(LongPointData b, LongPointData c)
        {
            return b.X != c.X || b.Y != c.Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is LongPointData)
            {
                return this == (LongPointData)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static LongPointData ToPointData(object value)
        {
            if (value is LongPointData) return (LongPointData)value;
            else if (value is UIntPointData) return new LongPointData(((UIntPointData)value).X, ((UIntPointData)value).Y);
            else if (value is IntPoint3Data) return new LongPointData(((IntPoint3Data)value).X, ((IntPoint3Data)value).Y);
            else if (value is UIntPoint3Data) return new LongPointData(((UIntPoint3Data)value).X, ((UIntPoint3Data)value).Y);
            else if (value is IntPointData) return new LongPointData((((IntPointData)value).X), (int)(((IntPointData)value).Y));
            else if (value is ULongPointData) return new LongPointData(((ULongPointData)value).X, ((ULongPointData)value).Y);
            else if (value is LongPoint3Data) return new LongPointData((int)(((LongPoint3Data)value).X), (int)(((LongPoint3Data)value).Y));
            else if (value is ULongPoint3Data) return new LongPointData((int)(((ULongPoint3Data)value).X), (int)(((ULongPoint3Data)value).Y));
            return Empty;
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public struct LongPoint3Data
    {
        public static LongPoint3Data Empty = new LongPoint3Data();
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
        /// <summary>
        /// 
        /// </summary>
        public long X { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public long Y { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public long Z { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return X + "," + Y + "," + Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static LongPoint3Data FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new LongPoint3Data(long.Parse(sval[0]), long.Parse(sval[1]), long.Parse(sval[2]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator ==(LongPoint3Data b, LongPoint3Data c)
        {
            return b.X == c.X && b.Y == c.Y && b.Z == c.Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(LongPoint3Data b, LongPoint3Data c)
        {
            return b.X != c.X || b.Y != c.Y || b.Z != c.Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is LongPoint3Data)
            {
                return this == (LongPoint3Data)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }


        // <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static LongPoint3Data ToPointData(object value)
        {
            if (value is LongPoint3Data) return (LongPoint3Data)value;
            else if (value is UIntPoint3Data) return new LongPoint3Data(((UIntPoint3Data)value).X, ((UIntPoint3Data)value).Y, ((UIntPoint3Data)value).Z);
            else if (value is IntPointData) return new LongPoint3Data(((IntPointData)value).X, ((IntPointData)value).Y, 0);
            else if (value is UIntPointData) return new LongPoint3Data(((UIntPointData)value).X, ((UIntPointData)value).Y, 0);
            else if (value is LongPointData) return new LongPoint3Data((int)(((LongPointData)value).X), (int)(((LongPointData)value).Y), 0);
            else if (value is ULongPointData) return new LongPoint3Data((int)(((ULongPointData)value).X), (int)(((ULongPointData)value).Y), 0);
            else if (value is IntPoint3Data) return new LongPoint3Data((int)(((IntPoint3Data)value).X), (int)(((IntPoint3Data)value).Y), (int)(((IntPoint3Data)value).Z));
            else if (value is ULongPoint3Data) return new LongPoint3Data((int)(((ULongPoint3Data)value).X), (int)(((ULongPoint3Data)value).Y), (int)(((ULongPoint3Data)value).Z));
            return Empty;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public struct ULongPointData
    {
        public static ULongPointData Empty = new ULongPointData();
        /// <summary>
        /// 
        /// </summary>
        /// <param name="x"></param>
        /// <param name="y"></param>
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
        /// <summary>
        /// 
        /// </summary>
        public ulong X { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ulong Y { get; set; }

        public override string ToString()
        {
            return X + "," + Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static ULongPointData FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new ULongPointData(ulong.Parse(sval[0]), ulong.Parse(sval[1]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator ==(ULongPointData b, ULongPointData c)
        {
            return b.X == c.X && b.Y == c.Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(ULongPointData b, ULongPointData c)
        {
            return b.X != c.X || b.Y != c.Y;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is ULongPointData)
            {
                return this == (ULongPointData)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static ULongPointData ToPointData(object value)
        {
            if (value is ULongPointData) return (ULongPointData)value;
            else if (value is UIntPointData) return new ULongPointData(((UIntPointData)value).X, ((UIntPointData)value).Y);
            else if (value is IntPoint3Data) return new ULongPointData(((IntPoint3Data)value).X, ((IntPoint3Data)value).Y);
            else if (value is UIntPoint3Data) return new ULongPointData(((UIntPoint3Data)value).X, ((UIntPoint3Data)value).Y);
            else if (value is IntPointData) return new ULongPointData((((IntPointData)value).X), (int)(((IntPointData)value).Y));
            else if (value is LongPointData) return new ULongPointData(((LongPointData)value).X, ((LongPointData)value).Y);
            else if (value is LongPoint3Data) return new ULongPointData((int)(((LongPoint3Data)value).X), (int)(((LongPoint3Data)value).Y));
            else if (value is ULongPoint3Data) return new ULongPointData((int)(((ULongPoint3Data)value).X), (int)(((ULongPoint3Data)value).Y));
            return Empty;
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public struct ULongPoint3Data
    {
        public static ULongPoint3Data Empty = new ULongPoint3Data();
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

        public static ULongPoint3Data FromString(string value)
        {
            string[] sval = value.Split(new char[] { ',' });
            return new ULongPoint3Data(ulong.Parse(sval[0]), ulong.Parse(sval[1]), ulong.Parse(sval[2]));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator ==(ULongPoint3Data b, ULongPoint3Data c)
        {
            return b.X == c.X && b.Y == c.Y && b.Z == c.Z;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="b"></param>
        /// <param name="c"></param>
        /// <returns></returns>
        public static bool operator !=(ULongPoint3Data b, ULongPoint3Data c)
        {
            return b.X != c.X || b.Y != c.Y || b.Z != c.Z;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj is ULongPoint3Data)
            {
                return this == (ULongPoint3Data)(obj);
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        // <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static ULongPoint3Data ToPointData(object value)
        {
            if (value is ULongPoint3Data) return (ULongPoint3Data)value;
            else if (value is UIntPoint3Data) return new ULongPoint3Data(((UIntPoint3Data)value).X, ((UIntPoint3Data)value).Y, ((UIntPoint3Data)value).Z);
            else if (value is IntPointData) return new ULongPoint3Data(((IntPointData)value).X, ((IntPointData)value).Y, 0);
            else if (value is UIntPointData) return new ULongPoint3Data(((UIntPointData)value).X, ((UIntPointData)value).Y, 0);
            else if (value is LongPointData) return new ULongPoint3Data((int)(((LongPointData)value).X), (int)(((LongPointData)value).Y), 0);
            else if (value is ULongPointData) return new ULongPoint3Data((int)(((ULongPointData)value).X), (int)(((ULongPointData)value).Y), 0);
            else if (value is IntPoint3Data) return new ULongPoint3Data((int)(((IntPoint3Data)value).X), (int)(((IntPoint3Data)value).Y), (int)(((IntPoint3Data)value).Z));
            else if (value is LongPoint3Data) return new ULongPoint3Data((int)(((LongPoint3Data)value).X), (int)(((LongPoint3Data)value).Y), (int)(((LongPoint3Data)value).Z));
            return Empty;
        }

    }

}
