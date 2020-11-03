//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using DBRuntime.His.Compress;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cdy.Tag
{

    /// <summary>
    /// 
    /// </summary>
    public class LosslessCompressUnit2 : CompressUnitbase2
    {
        protected MemoryBlock mMarshalMemory;

        protected ProtoMemory mVarintMemory;

        protected DoubleCompressBuffer mDCompress;

        protected FloatCompressBuffer mFCompress;

        /// <summary>
        /// 
        /// </summary>
        protected CustomQueue<int> emptys = new CustomQueue<int>(604);

        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "无损压缩";

        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 1;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase2 Clone()
        {
            return new LosslessCompressUnit2();
        }

        #region Compress

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public override long Compress(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size)
        {
            target.WriteDatetime(targetAddr, this.StartTime);

            //LoggerService.Service.Debug("LosslessCompressUnit2", "Record time: "+this.StartTime.ToString("yyyy-MM-dd HH:mm:ss.fff"));

            target.Write(TimeTick);
            switch (TagType)
            {
                case TagType.Bool:
                    return Compress<bool>(source, sourceAddr, target, targetAddr+12, size,TagType) + 12;
                case TagType.Byte:
                    return Compress<byte>(source, sourceAddr, target, targetAddr+ 12, size, TagType) + 12;
                case TagType.UShort:
                    return Compress<ushort>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Short:
                    return Compress<short>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UInt:
                    return Compress<uint>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Int:
                    return Compress<int>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULong:
                    return Compress<ulong>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Long:
                    return Compress<long>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Double:
                     return Compress<double>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.Float:
                    return Compress<float>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.String:
                    return Compress<string>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.IntPoint:
                    return Compress<IntPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UIntPoint:
                    return Compress<UIntPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.LongPoint:
                    return Compress<LongPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULongPoint:
                    return Compress<ULongPointData>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.IntPoint3:
                    return Compress<IntPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.UIntPoint3:
                    return Compress<UIntPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.LongPoint3:
                    return Compress<LongPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
                case TagType.ULongPoint3:
                    return Compress<ULongPoint3Data>(source, sourceAddr, target, targetAddr + 12, size, TagType) + 12;
            }
            return 12;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="timerVals"></param>
        ///// <param name="emptyIds"></param>
        ///// <returns></returns>
        //protected Memory<byte> CompressTimers(List<int> timerVals, CustomQueue<int> emptyIds)
        //{
        //    int preids = 0;
        //    mVarintMemory.Reset();
        //    emptys.Reset();
        //    //emptys.WriteIndex = 0;
        //    //emptyIds.ReadIndex = 0;
        //    bool isFirst = true;
        //    for (int i = 0; i < timerVals.Count; i++)
        //    {
        //        if (timerVals[i] > 0||i==0)
        //        {
        //            var id = timerVals[i];
        //            if (isFirst)
        //            {
        //                mVarintMemory.WriteInt32(id);
        //                isFirst = false;
        //            }
        //            else
        //            {
        //                mVarintMemory.WriteInt32(id - preids);
        //            }
        //            preids = id;
        //        }
        //        else
        //        {
        //            emptyIds.Insert(i);
        //        }
        //    }
        //    return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        //}

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="startaddr"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected virtual Memory<byte> CompressTimers(IMemoryFixedBlock timerVals,long startaddr,int count, CustomQueue<int> emptyIds)
        {
            int preids = 0;
            mVarintMemory.Reset();
            emptys.Reset();

            byte tlen = (timerVals as HisDataMemoryBlock).TimeLen;

            bool isFirst = true;
            int id = 0;
            for (int i = 0; i < count; i++)
            {
                id = tlen == 2 ? timerVals.ReadUShort((int)startaddr + i * 2) : timerVals.ReadInt((int)startaddr + i * 4);

                if (id > 0 || i == 0)
                {
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt32(id);
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteInt32(id - preids);
                    }
                    preids = id;
                }
                else
                {
                    emptyIds.Insert(i);
                }
            }
            return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
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
        protected virtual Memory<byte> CompressValues<T>(IMemoryFixedBlock source,long offset,int count, CustomQueue<int> emptyIds,TagType type)
        {
            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            bool isFirst = true;
            switch (type)
            {
                case TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadByte((int)offset + i);
                            mMarshalMemory.Write(id);
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                            //    emptyIds.TryDequeue(out ig);
                        }
                    }
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Short:
                    short sval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadShort((int)offset + i * 2);
                            if (isFirst)
                            {
                                mVarintMemory.WriteSInt32(id);
                                isFirst = false;
                                sval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - sval);
                                sval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UShort:
                    ushort ssval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUShort((int)offset + i * 2);
                            if (isFirst)
                            {
                                mVarintMemory.WriteSInt32(id);
                                isFirst = false;
                                ssval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - ssval);
                                ssval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Int:
                    int isval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i * 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                isFirst = false;
                                isval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - isval);
                                isval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UInt:
                    uint uisval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                isFirst = false;
                                uisval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - uisval));
                            }
                            uisval = id;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Long:
                    long lsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                isFirst = false;
                                lsval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((id - lsval));
                                lsval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.ULong:
                    ulong ulsval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                isFirst = false;
                                ulsval = id;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(id - ulsval));
                                ulsval = id;
                            }
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.Double:
                    mDCompress.Reset();
                    mDCompress.Precision = this.Precision;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadDouble((int)offset + i * 8);
                            mDCompress.Append(id);
                           // mMarshalMemory.Write(id);
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    mDCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Float:
                    mFCompress.Reset();
                    mFCompress.Precision = this.Precision;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadFloat((int)offset + i * 4);
                            mFCompress.Append(id);
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    mFCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.IntPoint:
                    int psval = 0;
                    int psval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i * 8);
                            var id2 = source.ReadInt((int)offset + i * 8 + 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(id - psval);
                                mVarintMemory.WriteSInt32(id2 - psval2);
                            }
                            psval = id;
                            psval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UIntPoint:
                    uint upsval = 0;
                    uint upsval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 8);
                            var id2 = source.ReadUInt((int)offset + i * 8 + 4);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - upsval));
                                mVarintMemory.WriteSInt32((int)(id2 - upsval2));
                            }
                            upsval = id;
                            upsval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.IntPoint3:
                     psval = 0;
                     psval2 = 0;
                    int psval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i * 12);
                            var id2 = source.ReadInt((int)offset + i * 12 + 4);
                            var id3 = source.ReadInt((int)offset + i * 12 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                mVarintMemory.WriteInt32(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - psval));
                                mVarintMemory.WriteSInt32((int)(id2 - psval2));
                                mVarintMemory.WriteSInt32((int)(id3 - psval3));
                            }
                            psval = id;
                            psval2 = id2;
                            psval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.UIntPoint3:
                     upsval = 0;
                     upsval2 = 0;
                    uint upsval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 12);
                            var id2 = source.ReadUInt((int)offset + i * 12 + 4);
                            var id3 = source.ReadUInt((int)offset + i * 12 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(id);
                                mVarintMemory.WriteInt32(id2);
                                mVarintMemory.WriteInt32(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(id - upsval));
                                mVarintMemory.WriteSInt32((int)(id2 - upsval2));
                                mVarintMemory.WriteSInt32((int)(id3 - upsval3));
                            }
                            upsval = id;
                            upsval2 = id2;
                            upsval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.LongPoint:
                    long lpsval = 0;
                    long lpsval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 16);
                            var id2 = source.ReadLong((int)offset + i * 16 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64(id - lpsval);
                                mVarintMemory.WriteSInt64(id2 - lpsval2);
                            }
                            lpsval = id;
                            lpsval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.ULongPoint:
                    ulong ulpsval = 0;
                    ulong ulpsval2 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 16);
                            var id2 = source.ReadULong((int)offset + i * 16 + 8);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(id - ulpsval));
                                mVarintMemory.WriteSInt64((long)(id2 - ulpsval2));
                            }
                            ulpsval = id;
                            ulpsval2 = id2;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.LongPoint3:
                    lpsval = 0;
                    lpsval2 = 0;
                    long lpsval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 24);
                            var id2 = source.ReadLong((int)offset + i * 24 + 8);
                            var id3 = source.ReadLong((int)offset + i * 24 + 16);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                mVarintMemory.WriteInt64(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64(id - lpsval);
                                mVarintMemory.WriteSInt64(id2 - lpsval2);
                                mVarintMemory.WriteSInt64(id3 - lpsval3);
                            }
                            lpsval = id;
                            lpsval2 = id2;
                            lpsval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                case TagType.ULongPoint3:
                    ulpsval = 0;
                    ulpsval2 = 0;
                    ulong ulpsval3 = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 24);
                            var id2 = source.ReadULong((int)offset + i * 24 + 8);
                            var id3 = source.ReadULong((int)offset + i * 24 + 16);
                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(id);
                                mVarintMemory.WriteInt64(id2);
                                mVarintMemory.WriteInt64(id3);
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(id - ulpsval));
                                mVarintMemory.WriteSInt64((long)(id2 - ulpsval2));
                                mVarintMemory.WriteSInt64((long)(id3 - ulpsval3));
                            }
                            ulpsval = id;
                            ulpsval2 = id2;
                            ulpsval3 = id3;
                        }
                        else
                        {
                            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                        }
                    }
                    break;
                default:
                    break;
            }
            return mVarintMemory.DataBuffer.AsMemory<byte>(0, (int)mVarintMemory.WritePosition);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected Memory<byte> CompressValues(List<string> timerVals, CustomQueue<int> emptyIds)
        {
            mMarshalMemory.Position = 0;
            int ig = -1;
            ig = emptys.ReadIndex <= emptyIds.WriteIndex ? emptys.IncRead() : -1;
            for (int i = 0; i < timerVals.Count; i++)
            {
                if(i != ig)
                {
                    var id = timerVals[i];
                    mMarshalMemory.Write(id);
                }
                else
                {
                    ig = emptys.ReadIndex <= emptyIds.WriteIndex ? emptys.IncRead() : -1;
                }
            }
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="totalcount"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected Memory<byte> CompressQulitys(IMemoryFixedBlock source, long offset, int totalcount, CustomQueue<int> emptyIds)
        {
            int count = 1;
            byte qus = source.ReadByte((int)offset);
            //using (ProtoMemory memory = new ProtoMemory(qulitys.Length * 2))
            mVarintMemory.Reset();
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            //emptyIds.TryDequeue(out ig);
            mVarintMemory.WriteInt32(qus);
            for (int i = 1; i < totalcount; i++)
            {
                if (i != ig)
                {
                    byte bval = source.ReadByte((int)offset + i);
                    if (bval == qus)
                    {
                        count++;
                    }
                    else
                    {
                        mVarintMemory.WriteInt32(count);
                        qus = bval;
                        mVarintMemory.WriteInt32(qus);
                        count = 1;
                    }
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                    //    emptyIds.TryDequeue(out ig);
                }
            }
            mVarintMemory.WriteInt32(count);
            
            return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="qulitys"></param>
        /// <returns></returns>
        protected Memory<byte> CompressQulitys(byte[] qulitys, CustomQueue<int> emptyIds)
        {
            int count = 1;
            byte qus = qulitys[0];
            //using (ProtoMemory memory = new ProtoMemory(qulitys.Length * 2))
            mVarintMemory.Reset();
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            mVarintMemory.WriteInt32(qus);
            for (int i = 1; i < qulitys.Length; i++)
            {
                if (i != ig)
                {
                    if (qulitys[i] == qus)
                    {
                        count++;
                    }
                    else
                    {
                        mVarintMemory.WriteInt32(count);
                        qus = qulitys[i];
                        mVarintMemory.WriteInt32(qus);
                        count = 1;
                    }
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                }
            }
            mVarintMemory.WriteInt32(count);
            return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        protected Memory<byte> CompressBoolValues(IMemoryFixedBlock source, long offset, int totalcount, CustomQueue<int> emptyIds)
        {
            List<byte> re = new List<byte>(totalcount);
            byte bval = source.ReadByte((int)offset);
            byte scount = 1;
            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
            //emptyIds.TryDequeue(out ig);

            byte sval = (byte)(bval << 7);
            for(int i=0;i< totalcount; i++)
            {
                if (i != ig)
                {
                    var btmp = source.ReadByte((int)(offset + i));
                    if(btmp == bval && scount<127)
                    {
                        scount++;
                    }
                    else
                    {
                        sval = (byte)(sval | scount);
                        re.Add(sval);
                        scount = 1;
                        bval = btmp;
                        sval = (byte)(bval << 7);
                    }
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                    //   emptyIds.TryDequeue(out ig);
                }
            }
            sval = (byte)(sval | scount);
            re.Add(sval);

            mMarshalMemory.Position = 0;
            foreach (var vv in re)
            {
                mMarshalMemory.Write(vv);
            }
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        protected virtual long Compress<T>(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);

            byte tlen = (source as HisDataMemoryBlock).TimeLen;

            if (mMarshalMemory==null)
            {
                mMarshalMemory = new MemoryBlock(count * 10);
            }

            if(mVarintMemory==null)
            {
                mVarintMemory = new ProtoMemory(count * 10);
            }

            emptys.CheckAndResize(count);

           

            var datas = CompressTimers(source, sourceAddr, (int)count, emptys);
            long rsize = 0;
            int rcount = count - emptys.WriteIndex - 1;

            target.WriteInt(targetAddr,rcount);
            rsize += 4;
            target.Write((int)datas.Length);
            target.Write(datas);
            rsize += 4;
            rsize += datas.Length;
            
            switch (type)
            {
                case TagType.Bool:
                    var cval = CompressBoolValues(source, count * tlen + sourceAddr, count, emptys);
                    target.Write(cval.Length);
                    target.Write(cval);
                    rsize += 4;
                    rsize += cval.Length;
                    emptys.ReadIndex = 0;
                    var cqus = CompressQulitys(source, count * (tlen+1) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Byte:
                    cval = CompressValues<byte>(source, count * tlen + sourceAddr, count, emptys,TagType);
                    target.Write(cval.Length);
                    target.Write(cval);
                    rsize += 4;
                    rsize += cval.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 1) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UShort:
                    var ures = CompressValues<ushort>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ures.Length);
                    target.Write(ures);
                    rsize += 4;
                    rsize += ures.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 2) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Short:
                   var  res = CompressValues<short>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(res.Length);
                    target.Write(res);
                    rsize += 4;
                    rsize += res.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 2) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UInt:
                    var uires = CompressValues<uint>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(uires.Length);
                    target.Write(uires);
                    rsize += 4;
                    rsize += uires.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 4) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Int:
                    var ires = CompressValues<int>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ires.Length);
                    target.Write(ires);
                    rsize += 4;
                    rsize += ires.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 4) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULong:
                    var ulres = CompressValues<ulong>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ulres.Length);
                    target.Write(ulres);
                    rsize += 4;
                    rsize += ulres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Long:
                    var lres = CompressValues<long>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(lres.Length);
                    target.Write(lres);
                    rsize += 4;
                    rsize += lres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.DateTime:
                    var dres = CompressValues<ulong>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(dres.Length);
                    target.Write(dres);
                    rsize += 4;
                    rsize += dres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Double:

                    if (mDCompress == null)
                    {
                        mDCompress = new DoubleCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        mDCompress.CheckAndResizeTo(count);
                    }

                    var ddres = CompressValues<double>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ddres.Length);
                    target.Write(ddres);
                    rsize += 4;
                    rsize += ddres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen+8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Float:

                    if (mFCompress == null)
                    {
                        mFCompress = new FloatCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        mFCompress.CheckAndResizeTo(count);
                    }

                    var fres = CompressValues<float>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(fres.Length);
                    target.Write(fres);
                    rsize += 4;
                    rsize += fres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen+4) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.String:
                    var vals = source.ReadStrings(count * tlen + (int)sourceAddr, count);
                    var qus = source.ReadBytes(count);
                    var sres = CompressValues(vals, emptys);
                    target.Write(sres.Length);
                    target.Write(sres);
                    rsize += 4;
                    rsize += sres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(qus, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.IntPoint:
                    var ipres = CompressValues<IntPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen+8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UIntPoint:
                    ipres = CompressValues<UIntPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.LongPoint:
                    ipres = CompressValues<LongPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 16) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULongPoint:
                    ipres = CompressValues<ULongPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 16) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.IntPoint3:
                    ipres = CompressValues<IntPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 12) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UIntPoint3:
                    ipres = CompressValues<UIntPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 12) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.LongPoint3:
                    ipres = CompressValues<LongPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen + 24) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULongPoint3:
                    ipres = CompressValues<ULongPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * (tlen+24) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
            }
            return rsize;
        }
        #endregion

        #region Decompress

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        private List<int> DeCompressTimers(byte[] timerVals, int count)
        {
            List<int> re = new List<int>();
            using (ProtoMemory memory = new ProtoMemory(timerVals))
            {
                int sval = (int)memory.ReadInt32();
                re.Add(sval);
                int preval = sval;
                for (int i = 1; i < count; i++)
                {
                    var ss = memory.ReadInt32();
                    var val = (preval + ss);
                    re.Add(val);
                    preval = val;
                }
                return re;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        private List<byte> DeCompressQulity(byte[] values)
        {
            List<byte> re = new List<byte>();
            using (ProtoMemory memory = new ProtoMemory(values))
            {
                while(memory.ReadPosition<values.Length)
                {
                    byte sval = (byte)memory.ReadInt32(); //读取质量戳
                    int ival = memory.ReadInt32(); //读取质量戳重复次数
                    for(int i=0;i<ival;i++)
                    {
                        re.Add(sval);
                    }
                }
                return re;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="memory"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        protected virtual List<T> DeCompressValue<T>(byte[] value, int count)
        {

            if (typeof(T) == typeof(byte))
            {
                return value.ToList() as List<T>;
            }
            else if (typeof(T) == typeof(short))
            {
                List<short> re = new List<short>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (short)memory.ReadSInt32();
                    re.Add(vv);
                    for (int i = 1; i < count; i++)
                    {
                        var vss = (short)memory.ReadSInt32();
                        vv = (short)(vv + vss);
                        re.Add((short)(vv));
                       
                    }
                }
                return re as List<T>;

            }
            else if (typeof(T) == typeof(ushort))
            {
                List<ushort> re = new List<ushort>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (ushort)memory.ReadSInt32();
                    re.Add((ushort)vv);
                    for (int i = 1; i < count; i++)
                    {
                        var vss = (short)memory.ReadSInt32();
                        vv = (ushort)(vv + vss);
                        re.Add((ushort)(vv));
                       
                    }
                }
                return re as List<T>;

            }
            else if (typeof(T) == typeof(int))
            {
                List<int> re = new List<int>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (int)memory.ReadInt32();
                    re.Add(vv);
                    for (int i = 1; i < count; i++)
                    {
                        var vss = (int)memory.ReadSInt32();
                        vv= (int)(vv + vss);
                        re.Add(vv);
                  
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(uint))
            {
                List<uint> re = new List<uint>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (uint)memory.ReadInt32();
                    re.Add((uint)vv);
                    for (int i = 1; i < count; i++)
                    {
                        var vss = memory.ReadSInt32();
                        vv = (uint)(vv + vss);
                        re.Add(vv);
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(long))
            {
                List<long> re = new List<long>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (long)memory.ReadInt64();
                    re.Add(vv);
                    for (int i = 1; i < count; i++)
                    {
                        var vss = (long)memory.ReadSInt64();
                        vv = (long)(vv + vss);
                        re.Add(vv);
                       
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(ulong))
            {
                List<ulong> re = new List<ulong>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (ulong)memory.ReadInt64();
                    re.Add(vv);
                    for (int i = 1; i < count; i++)
                    {
                        var vss = memory.ReadSInt64();
                        vv = (ulong)((long)vv + vss);
                        re.Add(vv);
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(DateTime))
            {
                List<DateTime> re = new List<DateTime>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (ulong)memory.ReadInt64();
                    re.Add(MemoryHelper.ReadDateTime(BitConverter.GetBytes(vv)));
                    for (int i = 1; i < count; i++)
                    {
                        var vss = (ulong)memory.ReadSInt64();
                        re.Add(MemoryHelper.ReadDateTime(BitConverter.GetBytes((ulong)(vv + vss))));
                        vv = vss;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(double))
            {
                return  DoubleCompressBuffer.Decompress(value) as List<T>;

                //using (MemorySpan block = new MemorySpan(value))
                //{
                //    return block.ToDoubleList() as List<T>;
                //}
            }
            else if (typeof(T) == typeof(float))
            {
                return FloatCompressBuffer.Decompress(value) as List<T>;
                //using (MemorySpan block = new MemorySpan(value))
                //{
                //    return block.ToFloatList() as List<T>;
                //}
            }
            else if (typeof(T) == typeof(string))
            {
                using (MemorySpan block = new MemorySpan(value))
                {
                    return block.ToStringList(Encoding.Unicode) as List<T>;
                }
            }
            else if (typeof(T) == typeof(bool))
            {
                using (MemorySpan block = new MemorySpan(value))
                {
                    List<bool> re = new List<bool>();
                    var rtmp = block.ToByteList();

                    foreach (var vv in rtmp)
                    {
                        bool bval = ((vv & 0x80) >> 7) > 0;
                        byte bcount = (byte)(vv & 0x7F);
                        for (int i = 0; i < bcount; i++)
                        {
                            re.Add(bval);
                        }
                    }
                    return re as List<T>;
                }
            }
            else if (typeof(T) == typeof(IntPointData))
            {
                List<IntPointData> re = new List<IntPointData>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (int)memory.ReadInt32();
                    var vv2 = (int)memory.ReadInt32();
                    re.Add(new IntPointData(vv,vv2));
                    for (int i = 2; i < count-1; i=i+2)
                    {
                        var vss = (int)memory.ReadSInt32();
                        var vss2 = (int)memory.ReadSInt32();
                        re.Add(new IntPointData((int)(vv + vss), (int)(vv2 + vss2)));
                        vv = vss;
                        vv2 = vss2;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                List<UIntPointData> re = new List<UIntPointData>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (int)memory.ReadInt32();
                    var vv2 = (int)memory.ReadInt32();
                    re.Add(new UIntPointData((uint)vv, (uint)vv2));
                    for (int i = 2; i < count - 1; i = i + 2)
                    {
                        var vss = (int)memory.ReadSInt32();
                        var vss2 = (int)memory.ReadSInt32();
                        re.Add(new UIntPointData((uint)(vv + vss), (uint)(vv2 + vss2)));
                        vv = vss;
                        vv2 = vss2;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                List<LongPointData> re = new List<LongPointData>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (long)memory.ReadInt64();
                    var vv2 = (long)memory.ReadInt64();
                    re.Add(new LongPointData(vv, vv2));
                    for (int i = 2; i < count - 1; i = i + 2)
                    {
                        var vss = memory.ReadSInt64();
                        var vss2 = memory.ReadSInt64();
                        re.Add(new LongPointData((vv + vss), (vv2 + vss2)));
                        vv = vss;
                        vv2 = vss2;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                List<ULongPointData> re = new List<ULongPointData>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = memory.ReadInt64();
                    var vv2 = memory.ReadInt64();
                    re.Add(new ULongPointData((ulong)vv, (ulong)vv2));
                    for (int i = 2; i < count - 1; i = i + 2)
                    {
                        var vss = memory.ReadSInt64();
                        var vss2 = memory.ReadSInt64();
                        re.Add(new ULongPointData((ulong)(vv + vss), (ulong)(vv2 + vss2)));
                        vv = vss;
                        vv2 = vss2;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                List<IntPoint3Data> re = new List<IntPoint3Data>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (int)memory.ReadInt32();
                    var vv2 = (int)memory.ReadInt32();
                    var vv3 = (int)memory.ReadInt32();
                    re.Add(new IntPoint3Data(vv, vv2,vv3));
                    for (int i = 3; i < count - 2; i = i + 3)
                    {
                        var vss = (int)memory.ReadSInt32();
                        var vss2 = (int)memory.ReadSInt32();
                        var vss3 = (int)memory.ReadSInt32();
                        re.Add(new IntPoint3Data((int)(vv + vss), (int)(vv2 + vss2), (int)(vv3 + vss3)));
                        vv = vss;
                        vv2 = vss2;
                        vv3 = vss3;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                List<UIntPoint3Data> re = new List<UIntPoint3Data>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (int)memory.ReadInt32();
                    var vv2 = (int)memory.ReadInt32();
                    var vv3 = (int)memory.ReadInt32();
                    re.Add(new UIntPoint3Data((uint)vv, (uint)vv2, (uint)vv3));
                    for (int i = 3; i < count - 2; i = i + 3)
                    {
                        var vss = (int)memory.ReadSInt32();
                        var vss2 = (int)memory.ReadSInt32();
                        var vss3 = (int)memory.ReadSInt32();
                        re.Add(new UIntPoint3Data((uint)(vv + vss), (uint)(vv2 + vss2), (uint)(vv3 + vss3)));
                        vv = vss;
                        vv2 = vss2;
                        vv3 = vss3;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                List<LongPoint3Data> re = new List<LongPoint3Data>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (long)memory.ReadInt64();
                    var vv2 = (long)memory.ReadInt64();
                    var vv3 = (long)memory.ReadInt64();
                    re.Add(new LongPoint3Data((long)vv, (long)vv2, (long)vv3));
                    for (int i = 3; i < count - 2; i = i + 3)
                    {
                        var vss = memory.ReadInt64();
                        var vss2 = memory.ReadInt64();
                        var vss3 = memory.ReadInt64();
                        re.Add(new LongPoint3Data((long)(vv + vss), (long)(vv2 + vss2), (long)(vv3 + vss3)));
                        vv = vss;
                        vv2 = vss2;
                        vv3 = vss3;
                    }
                }
                return re as List<T>;
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                List<ULongPoint3Data> re = new List<ULongPoint3Data>();
                using (ProtoMemory memory = new ProtoMemory(value))
                {
                    var vv = (long)memory.ReadInt64();
                    var vv2 = (long)memory.ReadInt64();
                    var vv3 = (long)memory.ReadInt64();
                    re.Add(new ULongPoint3Data((ulong)vv, (ulong)vv2, (ulong)vv3));
                    for (int i = 3; i < count - 2; i = i + 3)
                    {
                        var vss = memory.ReadInt64();
                        var vss2 = memory.ReadInt64();
                        var vss3 = memory.ReadInt64();
                        re.Add(new ULongPoint3Data((ulong)(vv + vss), (ulong)(vv2 + vss2), (ulong)(vv3 + vss3)));
                        vv = vss;
                        vv2 = vss2;
                        vv3 = vss3;
                    }
                }
                return re as List<T>;
            }
            return null;
        }

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="valueCount"></param>
        /// <returns></returns>
        protected Dictionary<int,DateTime> GetTimers(MarshalMemoryBlock source,int sourceAddr,DateTime startTime,DateTime endTime,out int valueCount)
        {
            DateTime sTime = source.ReadDateTime(sourceAddr);

            int timeTick = source.ReadInt(sourceAddr + 8);

            Dictionary<int, DateTime> re = new Dictionary<int, DateTime>();
            var count = source.ReadInt();
            var datasize = source.ReadInt();
                       
            byte[] datas = source.ReadBytes(datasize);
            var timers = DeCompressTimers(datas, count);

            DateTime preTimer = DateTime.MinValue;

            for (int i = 0; i < timers.Count; i++)
            {
                var vtime = sTime.AddMilliseconds(timers[i] * timeTick);

                if (vtime < preTimer) continue;
                if (vtime >= startTime && vtime < endTime)
                    re.Add(i, vtime);
                else if(vtime>endTime && (vtime - endTime).TotalMilliseconds< timeTick)
                {
                    re.Add(i, vtime);
                }
                else if (vtime < startTime && (startTime - vtime).TotalMilliseconds < timeTick)
                {
                    re.Add(i, vtime);
                }
                preTimer = vtime;
            }
            valueCount = count;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="timeTick"></param>
        /// <param name="valueCount"></param>
        /// <returns></returns>
        protected Dictionary<int, DateTime> GetTimers(MarshalMemoryBlock source, int sourceAddr, out int valueCount)
        {
            DateTime sTime = source.ReadDateTime(sourceAddr);
            int timeTick = source.ReadInt(sourceAddr + 8);

            Dictionary<int, DateTime> re = new Dictionary<int, DateTime>();
            var count = source.ReadInt();
            var datasize = source.ReadInt();
            byte[] datas = source.ReadBytes(datasize);
            var timers = DeCompressTimers(datas, count);

            for (int i = 0; i < timers.Count; i++)
            {
                re.Add(i, sTime.AddMilliseconds(timers[i] * timeTick));
            }
            valueCount = count;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        private bool CheckTypeIsPointData(Type type)
        {
            return type == typeof(IntPointData) || type == typeof(UIntPointData) || type == typeof(LongPointData) || type == typeof(ULongPointData) || type == typeof(IntPoint3Data) || type == typeof(UIntPoint3Data) || type == typeof(LongPoint3Data) || type == typeof(ULongPoint3Data);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeTick"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public override int DeCompressAllValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime startTime, DateTime endTime, int timeTick, HisQueryResult<T> result)
        {
            int count = 0;
            var timers = GetTimers(source, sourceAddr, startTime, endTime, out count);

            if (timers.Count > 0)
            {
                var valuesize = source.ReadInt();
                var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

                var qusize = source.ReadInt();

                var qulityes = DeCompressQulity(source.ReadBytes(qusize));
                int resultCount = 0;
                for (int i = 0; i < count; i++)
                {
                    if (qulityes[i] < 100 && timers.ContainsKey(i))
                    {
                        result.Add<T>(value[i], timers[i], qulityes[i]);
                        resultCount++;
                    }
                }
                return resultCount;
            }
            return 0;

        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public override int DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result)
        {
            if (CheckTypeIsPointData(typeof(T)))
            {
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type, result);
            }

            int count = 0;
            var timers = GetTimers(source, sourceAddr, out count);

            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

            var qusize = source.ReadInt();

            var qulityes = DeCompressQulity(source.ReadBytes(qusize));
            int resultCount = 0;

            int j = 0;

            foreach (var time1 in time)
            {
                for (int i = j; i < timers.Count - 1; i++)
                {
                    var skey = timers[i];

                    var snext = timers[i + 1];
                    j = i;

                    if ((time1==skey) ||(time1 < skey && (skey - time1).TotalSeconds<1))
                    {
                        var val = value[i];
                        result.Add(val, time1, qulityes[i]);
                        resultCount++;
                        
                        break;
                    }
                    else if (time1 > skey && time1 < snext)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = value[i];
                                result.Add(val, time1, qulityes[i]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.After:
                                val = value[i + 1];
                                result.Add(val, time1, qulityes[i+1]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (typeof(T) == typeof(bool)|| typeof(T) == typeof(string)|| typeof(T) == typeof(DateTime))
                                {
                                    var ppval = (time1 - skey).TotalMilliseconds;
                                    var ffval = (snext - time1).TotalMilliseconds;

                                    if (ppval < ffval)
                                    {
                                        val = value[i];
                                        result.Add(val, time1, qulityes[i]);
                                    }
                                    else
                                    {
                                        val = value[i + 1];
                                        result.Add(val, time1, qulityes[i + 1]);
                                    }
                                    resultCount++;
                                }
                                else
                                {
                                    if (qulityes[i] < 20 && qulityes[i + 1] < 20)
                                    {
                                        var pval1 = (time1 - skey).TotalMilliseconds;
                                        var tval1 = (snext - skey).TotalMilliseconds;
                                        var sval1 = value[i];
                                        var sval2 = value[i + 1];

                                        var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);
                                        
                                        result.Add((object)val1, time1, 0);
                                    }
                                    else if (qulityes[i] < 20)
                                    {
                                        val = value[i];
                                        result.Add(val, time1, qulityes[i]);
                                    }
                                    else if (qulityes[i + 1] < 20)
                                    {
                                        val = value[i + 1];
                                        result.Add(val, time1, qulityes[i + 1]);
                                    }
                                    else
                                    {
                                        result.Add(default(T), time1, (byte)QualityConst.Null);
                                    }
                                    resultCount++;
                                }
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey).TotalMilliseconds;
                                var fval = (snext - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = value[i];
                                    result.Add(val, time1, qulityes[i]);
                                }
                                else
                                {
                                    val = value[i+1];
                                    result.Add(val, time1, qulityes[i + 1]);
                                }
                                resultCount++;
                                break;
                        }
                        break;
                    }
                    else if (time1 == snext)
                    {
                        var val =value[i + 1];
                        result.Add(val, time1, qulityes[i+1]);
                        resultCount++;
                        break;
                    }

                }
            }


            return resultCount;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public override object DeCompressValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time, int timeTick, QueryValueMatchType type)
        {
            if (CheckTypeIsPointData(typeof(T)))
            {
                return DeCompressPointValue<T>(source, sourceAddr, time, timeTick, type);
            }

            int count = 0;
            var timers = GetTimers(source, sourceAddr + 8,  out count);

            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

            var qusize = source.ReadInt();

            var qulityes = DeCompressQulity(source.ReadBytes(qusize));

            int j = 0;

            for (int i = j; i < timers.Count - 1; i++)
            {
                var skey = timers[i];

                var snext = timers[i + 1];

                if ((time == skey) || (time < skey && (skey - time).TotalSeconds < 1))
                {
                    return value[i];
                }
                else if (time > skey && time < snext)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return value[i];
                        case QueryValueMatchType.After:
                            return value[i + 1];
                        case QueryValueMatchType.Linear:
                            if (typeof(T) == typeof(bool) || typeof(T) == typeof(string) || typeof(T) == typeof(DateTime))
                            {
                                var ppval = (time - skey).TotalMilliseconds;
                                var ffval = (snext - time).TotalMilliseconds;

                                if (ppval < ffval)
                                {
                                    return value[i];
                                }
                                else
                                {
                                    return value[i + 1];
                                }
                            }
                            else
                            {
                                if (qulityes[i] < 20 && qulityes[i + 1] < 20)
                                {
                                    var pval1 = (time - skey).TotalMilliseconds;
                                    var tval1 = (snext - skey).TotalMilliseconds;
                                    var sval1 = value[i];
                                    var sval2 = value[i + 1];

                                    var val1 = pval1 / tval1 * (Convert.ToDouble(sval2) - Convert.ToDouble(sval1)) + Convert.ToDouble(sval1);

                                    if (typeof(T) == typeof(double))
                                    {
                                        return val1;
                                    }
                                    else if (typeof(T) == typeof(float))
                                    {
                                        return (float)val1;
                                    }
                                    else if (typeof(T) == typeof(short))
                                    {
                                        return (short)val1;
                                    }
                                    else if (typeof(T) == typeof(ushort))
                                    {
                                        return (ushort)val1;
                                    }
                                    else if (typeof(T) == typeof(int))
                                    {
                                        return (int)val1;
                                    }
                                    else if (typeof(T) == typeof(uint))
                                    {
                                        return (uint)val1;
                                    }
                                    else if (typeof(T) == typeof(long))
                                    {
                                        return (long)val1;
                                    }
                                    else if (typeof(T) == typeof(ulong))
                                    {
                                        return (ulong)val1;
                                    }
                                    else if (typeof(T) == typeof(byte))
                                    {
                                        return (byte)val1;
                                    }
                                }
                                else if (qulityes[i] < 20)
                                {
                                    return value[i];
                                }
                                else if (qulityes[i + 1] < 20)
                                {
                                    return value[i + 1];
                                }
                                else
                                {
                                    return null;
                                }
                            }
                            break;
                        case QueryValueMatchType.Closed:
                            var pval = (time - skey).TotalMilliseconds;
                            var fval = (snext - time).TotalMilliseconds;

                            if (pval < fval)
                            {
                                return value[i];
                            }
                            else
                            {
                                return value[i + 1];
                            }
                    }
                    break;
                }
                else if (time == snext)
                {
                    return value[i + 1];
                }

            }

            return null;
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public  object DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, DateTime time1, int timeTick, QueryValueMatchType type)
        {
            int count = 0;
            var timers = GetTimers(source, sourceAddr + 8,out count);

            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

            var qusize = source.ReadInt();

            var qulityes = DeCompressQulity(source.ReadBytes(qusize));
            
            for (int i = 0; i < timers.Count - 1; i++)
            {
                var skey = timers[i];

                var snext = timers[i + 1];

                if ((time1 == skey) || (time1 < skey && (skey - time1).TotalSeconds < 1))
                {
                    return value[i];

                }
                else if (time1 > skey && time1 < snext)
                {
                    switch (type)
                    {
                        case QueryValueMatchType.Previous:
                            return value[i];
                        case QueryValueMatchType.After:
                            return value[i+1];
                        case QueryValueMatchType.Linear:
                            if (qulityes[i] < 20 && qulityes[i + 1] < 20)
                            {
                                return (T)LinerValue(skey, snext, time1, value[i], value[i + 1]);
                            }
                            else if (qulityes[i] < 20)
                            {
                                return value[i];
                            }
                            else if (qulityes[i + 1] < 20)
                            {
                                return value[i+1];
                            }
                            return null;
                        case QueryValueMatchType.Closed:
                            var pval = (time1 - skey).TotalMilliseconds;
                            var fval = (snext - time1).TotalMilliseconds;

                            if (pval < fval)
                            {
                                return value[i];
                            }
                            else
                            {
                                return value[i + 1];
                            }
                            
                    }
                    break;
                }
                else if (time1 == snext)
                {
                    return value[i + 1];
                }

            }

            return null;
            
        }


        #region

        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="time"></param>
        /// <param name="value1"></param>
        /// <param name="value2"></param>
        /// <returns></returns>
        private object LinerValue<T>(DateTime startTime,DateTime endTime,DateTime time,T value1,T value2)
        {
            var pval1 = (time - startTime).TotalMilliseconds;
            var tval1 = (endTime - startTime).TotalMilliseconds;

            if (typeof(T) == typeof(IntPointData))
            {
                var sval1 = (IntPointData)((object)value1);
                var sval2 = (IntPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new IntPointData((int)val1, (int)val2);
            }
            else if (typeof(T) == typeof(UIntPointData))
            {
                var sval1 = (UIntPointData)((object)value1);
                var sval2 = (UIntPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new UIntPointData((uint)val1, (uint)val2);
            }
            else if (typeof(T) == typeof(LongPointData))
            {
                var sval1 = (LongPointData)((object)value1);
                var sval2 = (LongPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new LongPointData((long)val1, (long)val2);
            }
            else if (typeof(T) == typeof(ULongPointData))
            {
                var sval1 = (ULongPointData)((object)value1);
                var sval2 = (ULongPointData)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                return new ULongPointData((ulong)val1, (ulong)val2);
            }
            else if (typeof(T) == typeof(IntPoint3Data))
            {
                var sval1 = (IntPoint3Data)((object)value1);
                var sval2 = (IntPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new IntPoint3Data((int)val1, (int)val2, (int)val3);
            }
            else if (typeof(T) == typeof(UIntPoint3Data))
            {
                var sval1 = (UIntPoint3Data)((object)value1);
                var sval2 = (UIntPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new UIntPoint3Data((uint)val1, (uint)val2, (uint)val3);
            }
            else if (typeof(T) == typeof(LongPoint3Data))
            {
                var sval1 = (LongPoint3Data)((object)value1);
                var sval2 = (LongPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new LongPoint3Data((long)val1, (long)val2, (long)val3);
            }
            else if (typeof(T) == typeof(ULongPoint3Data))
            {
                var sval1 = (ULongPoint3Data)((object)value1);
                var sval2 = (ULongPoint3Data)((object)value2);
                var val1 = pval1 / tval1 * (Convert.ToDouble(sval2.X) - Convert.ToDouble(sval1.X)) + Convert.ToDouble(sval1.X);
                var val2 = pval1 / tval1 * (Convert.ToDouble(sval2.Y) - Convert.ToDouble(sval1.Y)) + Convert.ToDouble(sval1.Y);
                var val3 = pval1 / tval1 * (Convert.ToDouble(sval2.Z) - Convert.ToDouble(sval1.Z)) + Convert.ToDouble(sval1.Z);
                return new ULongPoint3Data((ulong)val1, (ulong)val2, (ulong)val3);
            }

            return default(T);
        }

        
       
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="time"></param>
        /// <param name="timeTick"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        /// <returns></returns>
        public  int DeCompressPointValue<T>(MarshalMemoryBlock source, int sourceAddr, List<DateTime> time, int timeTick, QueryValueMatchType type, HisQueryResult<T> result)
        {
            int count = 0;
            var timers = GetTimers(source, sourceAddr + 8,out count);

            var valuesize = source.ReadInt();
            var value = DeCompressValue<T>(source.ReadBytes(valuesize), count);

            var qusize = source.ReadInt();

            var qulityes = DeCompressQulity(source.ReadBytes(qusize));
            int resultCount = 0;

            int j = 0;

            foreach (var time1 in time)
            {
                for (int i = j; i < timers.Count - 1; i++)
                {
                    var skey = timers[i];

                    var snext = timers[i + 1];

                    if ((time1 == skey) || (time1 < skey && (skey - time1).TotalSeconds < 1))
                    {
                        var val = value[i];
                        result.Add(val, time1, qulityes[i]);
                        resultCount++;

                        break;
                    }
                    else if (time1 > skey && time1 < snext)
                    {
                        switch (type)
                        {
                            case QueryValueMatchType.Previous:
                                var val = value[i];
                                result.Add(val, time1, qulityes[i]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.After:
                                val = value[i + 1];
                                result.Add(val, time1, qulityes[i + 1]);
                                resultCount++;
                                break;
                            case QueryValueMatchType.Linear:
                                if (qulityes[i] < 20 && qulityes[i + 1] < 20)
                                {
                                    result.Add(LinerValue(skey, snext, time1, value[i], value[i + 1]), time1, 0);
                                }
                                else if (qulityes[i] < 20)
                                {
                                    val = value[i];
                                    result.Add(val, time1, qulityes[i]);
                                }
                                else if (qulityes[i + 1] < 20)
                                {
                                    val = value[i + 1];
                                    result.Add(val, time1, qulityes[i + 1]);
                                }
                                else
                                {
                                    result.Add(0, time1, (byte)QualityConst.Null);
                                }
                                resultCount++;
                                break;
                            case QueryValueMatchType.Closed:
                                var pval = (time1 - skey).TotalMilliseconds;
                                var fval = (snext - time1).TotalMilliseconds;

                                if (pval < fval)
                                {
                                    val = value[i];
                                    result.Add(val, time1, qulityes[i]);
                                }
                                else
                                {
                                    val = value[i + 1];
                                    result.Add(val, time1, qulityes[i + 1]);
                                }
                                resultCount++;
                                break;
                        }
                        break;
                    }
                    else if (time1 == snext)
                    {
                        var val = value[i + 1];
                        result.Add(val, time1, qulityes[i + 1]);
                        resultCount++;
                        break;
                    }

                }
            }

            return resultCount;
        }
        
        #endregion


    }
}
