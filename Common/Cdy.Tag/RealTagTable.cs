//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/1/13 10:01:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class RealTagTable:MarshalMemoryBlock
    {

        #region ... Variables  ...

        //单个变量的结构：ID(4)Name(32)Group(64)Type(1)ReadWriteType(1)LinkAddress(64)Conveter(32)MaxValue(8)MinValue(8)Precision(1)Desc(128)ValueAddress(8)  总大小:351
        //
        public const int TagSize = 384;

        private int mTagCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public RealTagTable() : base()
        {
            this.mBufferItemSize = BufferSize64;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public RealTagTable(long count):this()
        {
            base.Init(count * TagSize);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns>变量在表中的编号</returns>
        public int AddTag(Tagbase tag)
        {
            var vp = mTagCount * TagSize;

            WriteInt(vp, tag.Id);
            WriteString(vp+4, tag.Name, Encoding.UTF8);
            WriteString(vp+36, tag.Name, Encoding.UTF8);
           // WriteString(tag.Group, 64);
            WriteByte(vp+100,(byte)tag.Type);
            WriteByte(vp + 101,(byte)tag.ReadWriteType);
            WriteString(vp + 102, tag.LinkAddress, Encoding.UTF8);
            WriteString(vp + 166,tag.Conveter!=null? tag.Conveter.Name:"", Encoding.UTF8);
            if (tag is NumberTagBase)
            {
                WriteDouble(vp + 198, (tag as NumberTagBase).MaxValue);
            }
            else
            {
                WriteDouble(vp + 198, 0);
            }
            if (tag is NumberTagBase)
            {
                WriteDouble(vp + 206, (tag as NumberTagBase).MinValue);
            }
            else
            {
                WriteDouble(vp + 206, 0);
            }

            if (tag is FloatingTagBase)
            {
                WriteByte(vp + 214, (tag as FloatingTagBase).Precision);
            }
            else
            {
                WriteByte(vp + 214, 0);
            }
            WriteString(vp + 215, tag.Conveter != null ? tag.Conveter.Name : "", Encoding.UTF8);

            var re = mTagCount;
            mTagCount++;
            //WriteString(tag.Desc, 128);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public int ReadId(int tagindex)
        {
            var vp = tagindex * TagSize;
            return ReadInt(vp);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public string ReadName(int tagindex)
        {
            var vp = tagindex * TagSize+4;
            return ReadStringInner(vp,32);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public string ReadGroup(int tagindex)
        {
            var vp = tagindex * TagSize + 36;
            return ReadStringInner(vp, 64);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public TagType ReadTagType(int tagindex)
        {
            return (TagType)(ReadByte(tagindex * TagSize + 100));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public bool IsNumberTag(int tagindex)
        {
            var tp = ReadTagType(tagindex);
            return  tp == TagType.Float || tp == TagType.Double || tp == TagType.Byte || tp == TagType.Int || tp == TagType.UInt || tp == TagType.Long || tp == TagType.ULong;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public bool IsFloatTag(int tagindex)
        {
            var tp = ReadTagType(tagindex);
            return tp == TagType.Double || tp == TagType.Float;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public byte ReadTagType2(int tagindex)
        {
            return (ReadByte(tagindex * TagSize + 100));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public ReadWriteMode ReadReadWriteMode(int tagindex)
        {
            return (ReadWriteMode)(ReadByte(tagindex * TagSize + 101));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public byte ReadReadWriteMode2(int tagindex)
        {
            return (ReadByte(tagindex * TagSize + 101));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public string ReadLinkAdress(int tagindex)
        {
            var vp = tagindex * TagSize + 102;
            return ReadStringInner(vp, 64);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public IValueConvert ReadConvert(int tagindex)
        {
            var vp = tagindex * TagSize + 166;
            if (ReadByte(vp) == 0) return null;

            string sname = ReadStringInner(vp, 32);
            return ValueConvertManager.manager.GetConvert(sname);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public double ReadMaxValue(int tagindex)
        {
            return ReadDouble(tagindex * TagSize + 198);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public double ReadMinValue(int tagindex)
        {
            return ReadDouble(tagindex * TagSize + 206);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public byte ReadPrecision(int tagindex)
        {
            return ReadByte(tagindex * TagSize + 214);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public string ReadDesc(int tagindex)
        {
            return ReadStringInner(tagindex * TagSize + 215,128);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <returns></returns>
        public long ReadValueAddress(int tagindex)
        {
            return ReadLong(tagindex * TagSize + 343);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagindex"></param>
        /// <param name="address"></param>
        public void WriteValueAddress(int tagindex,long address)
        {
            WriteLong(tagindex * TagSize + 343,address);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int GetTagValueSize(int tagindex)
        {
            var vp = ReadTagType(tagindex);
            switch (vp)
            {
                case TagType.Bool:
                case TagType.Byte:
                    return 10;
                case TagType.DateTime:
                case TagType.Double:
                case TagType.Long:
                case TagType.ULong:
                    return 17;
                case TagType.Float:
                case TagType.Int:
                case TagType.UInt:
                    return 13;
                case TagType.Short:
                case TagType.UShort:
                    return 11;
                case TagType.String:
                    return Const.StringSize + 9;
                case TagType.IntPoint:
                case TagType.UIntPoint:
                    return 13;
                case TagType.LongPoint:
                case TagType.ULongPoint:
                    return 25;
                case TagType.IntPoint3:
                case TagType.UIntPoint3:
                    return 21;
                case TagType.LongPoint3:
                case TagType.ULongPoint3:
                    return 33;
                default:
                    return 0;
            }
           
        }


        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="sval"></param>
        ///// <param name="size"></param>
        //private void WriteString(string sval)
        //{
        //    //var buffer = Encoding.UTF8.GetBytes(sval);
        //    //var buffer2 = ArrayPool<byte>.Shared.Rent(size);
        //    //Array.Clear(buffer2, 0, buffer2.Length);
        //    //Array.Copy(buffer, buffer2, Math.Min(buffer.Length, size));
        //    //WriteBytes(Position, buffer2);
        //    WriteString(Position,sval,Encoding.UTF8);
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="position"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        private string ReadStringInner(long position,int size)
        {
            return ReadString(position, Encoding.UTF8);
            //return Encoding.UTF8.GetString(ReadBytes(position, size));
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
    
}
