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
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DeadAreaCompressUnit : LosslessCompressUnit
    {
        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 2;

        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "死区压缩";

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase Clone()
        {
            return new DeadAreaCompressUnit();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="preVal"></param>
        /// <param name="newVal"></param>
        /// <param name="deadArea"></param>
        /// <param name="Deadtype"></param>
        /// <returns></returns>
        private bool CheckIsNeedRecord(double preVal,double newVal,double deadArea,int Deadtype)
        {
            if(Deadtype==0)
            {
                return Math.Abs(newVal - preVal) > deadArea;
            }
            else
            {
                return Math.Abs((newVal - preVal) / preVal) > deadArea;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected override Memory<byte> CompressValues<T>(MarshalMemoryBlock source, long offset, int count, CustomQueue<int> emptys, TagType type)
        {
            var deadArea = this.Parameters.ContainsKey("DeadValue") ? this.Parameters["DeadValue"] : 0;
            var deadType = (int)(this.Parameters.ContainsKey("DeadType") ? this.Parameters["DeadType"] : 0);

            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();

            bool isFirst = true;

            int ig = -1;
            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;

            switch (type)
            {
                case TagType.Byte:
                    byte sval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadByte(offset + i);
                            if (isFirst)
                            {
                                sval = id;
                                mMarshalMemory.Write(id);
                                isFirst = false;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(sval, id, deadArea, deadType))
                                {
                                    mMarshalMemory.Write(id);
                                    sval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Short:
                    short ssval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadShort(offset + i * 2);
                            if (isFirst)
                            {
                                mVarintMemory.WriteSInt32(id);
                                isFirst = false;
                                ssval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(ssval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt32(id - ssval);
                                    ssval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UShort:
                    ushort ussval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUShort(offset + i * 2);
                            if (isFirst)
                            {
                                mVarintMemory.WriteSInt32(id);
                                isFirst = false;
                                ussval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(ussval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt32(id - ussval);
                                    ussval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Int:
                    int isval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt(offset + i * 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                isFirst = false;
                                isval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(isval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt32(id - isval);
                                    isval = id;
                                }
                            }
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UInt:
                    uint usval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt(offset + i * 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                isFirst = false;
                                usval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(usval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt32((int)(id - usval));
                                    usval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Long:
                    long lsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong(offset + i * 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                isFirst = false;
                                lsval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(lsval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt64((id - lsval));
                                    lsval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.ULong:
                    ulong ulsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong(offset + i * 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                isFirst = false;
                                ulsval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(ulsval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt64((long)(id - ulsval));
                                    ulsval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Double:
                    double dsval = 0;
                    mDCompress.Reset();
                    mDCompress.Precision = this.Precision;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadDouble(offset + i * 8);
                            if (isFirst)
                            {
                                mDCompress.Append(id);
                                isFirst = false;
                                dsval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(dsval, id, deadArea, deadType))
                                {
                                    mDCompress.Append(id);
                                    // mMarshalMemory.Write(id);
                                    dsval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mDCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Float:
                    mFCompress.Reset();
                    mFCompress.Precision = this.Precision;
                    float fsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadFloat(offset + i * 4);
                            if (isFirst)
                            {
                                mFCompress.Append(id);
                                isFirst = false;
                                fsval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(fsval, id, deadArea, deadType))
                                {
                                    mFCompress.Append(id);
                                    fsval = id;
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mFCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                default:
                   return base.CompressValues<T>(source, offset, count, emptys, type);
            }
            return mVarintMemory.DataBuffer.AsMemory<byte>(0, (int)mVarintMemory.WritePosition);
        }
    }
}
