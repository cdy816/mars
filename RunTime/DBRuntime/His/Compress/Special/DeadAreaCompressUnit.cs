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
        protected override Memory<byte> CompressValues<T>(MarshalMemoryBlock source, long offset, int count, Queue<int> emptyIds)
        {
            var deadArea = this.Parameters.ContainsKey("DeadValue") ? this.Parameters["DeadValue"] : 0;
            var deadType = (int)(this.Parameters.ContainsKey("DeadType") ? this.Parameters["DeadType"] : 0);

            mMarshalMemory.Position = 0;
            mVarintMemory.Position = 0;

            bool isFirst = true;

            int ig = -1;
            emptyIds.TryDequeue(out ig);

            if (typeof(T) == typeof(byte))
            {
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
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mMarshalMemory.Write(id);
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
            }
            else if (typeof(T) == typeof(short))
            {
                short sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadShort(offset + i * 2);
                        if (isFirst)
                        {
                            mVarintMemory.WriteSInt32(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mVarintMemory.WriteSInt32(id - sval);
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }

            }
            else if (typeof(T) == typeof(ushort))
            {
                ushort sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadUShort(offset + i * 2);
                        if (isFirst)
                        {
                            mVarintMemory.WriteSInt32(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mVarintMemory.WriteSInt32(id - sval);
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }

            }
            else if (typeof(T) == typeof(int))
            {
                int sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadInt(offset + i * 4);
                        if (isFirst)
                        {
                            mVarintMemory.WriteInt32(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mVarintMemory.WriteSInt32(id - sval);
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }

            }
            else if (typeof(T) == typeof(uint))
            {
                uint sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadUInt(offset + i * 4);
                        if (isFirst)
                        {
                            mVarintMemory.WriteInt32(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mVarintMemory.WriteSInt32((int)(id - sval));
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
            }
            else if (typeof(T) == typeof(long))
            {
                long sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadLong(offset + i * 8);
                        if (isFirst)
                        {
                            mVarintMemory.WriteInt64(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mVarintMemory.WriteSInt64((id - sval));
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }

            }
            else if (typeof(T) == typeof(ulong))
            {
                ulong sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadULong(offset + i * 8);
                        if (isFirst)
                        {
                            mVarintMemory.WriteInt64(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval, id, deadArea, deadType))
                            {
                                mVarintMemory.WriteSInt64((long)(id - sval));
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }

            }
            else if (typeof(T) == typeof(double))
            {
                double sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadDouble(offset + i * 8);
                        if (isFirst)
                        {
                            mMarshalMemory.Write(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mMarshalMemory.Write(id);
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
            }
            else if (typeof(T) == typeof(float))
            {
                float sval = 0;
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadFloat(offset + i * 4);
                        if (isFirst)
                        {
                            mMarshalMemory.Write(id);
                            isFirst = false;
                            sval = id;
                        }
                        else
                        {
                            if (CheckIsNeedRecord(sval,id,deadArea,deadType))
                            {
                                mMarshalMemory.Write(id);
                                sval = id;
                            }
                        }
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
            }

            return mVarintMemory.Buffer.AsMemory<byte>(0, (int)mVarintMemory.Position);
        }
    }
}
