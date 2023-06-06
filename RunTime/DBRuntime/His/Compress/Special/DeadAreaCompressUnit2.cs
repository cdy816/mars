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
using System.Buffers;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DeadAreaCompressUnit2 : LosslessCompressUnit2
    {
        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 2;

        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "死区压缩";

        protected CustomQueue<int> emptys2 = new CustomQueue<int>(604);

        protected ProtoMemory mVarintMemory2;

        //Dictionary<int, double> dvals = new Dictionary<int, double>();

        //Dictionary<int, int> dtims = new Dictionary<int, int>();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase2 Clone()
        {
            return new DeadAreaCompressUnit2();
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
                return preVal > 0 ? Math.Abs((newVal - preVal) / preVal) > deadArea : true;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="startaddr"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        protected void FindEmpityIds(IMemoryFixedBlock timerVals, long startaddr, int count, CustomQueue<int> emptyIds)
        {
            emptys.Reset();
            int id = 0;

            byte tlen = 2;
            if(timerVals is HisDataMemoryBlock)
            {
                tlen = (timerVals as HisDataMemoryBlock).TimeLen;
            }

            for (int i = 0; i < count; i++)
            {
                id = tlen==2? timerVals.ReadUShort((int)startaddr + i * 2): timerVals.ReadUShort((int)startaddr + i * 4);
                if (id > 0 || i == 0)
                {
                    continue;
                }
                else
                {
                    emptyIds.Insert(i);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="startaddr"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected virtual Memory<byte> CompressTimers2(IMemoryFixedBlock timerVals, long startaddr, int count, CustomQueue<int> emptyIds)
        {
            int preids = 0;
            mVarintMemory2.Reset();
            //dtims.Clear();
            bool isFirst = true;
            int id = 0;

            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;

            byte tlen = (timerVals as HisDataMemoryBlock).TimeLen;

            int rcount = 0;
            int lastdec = 0;

            for (int i = 0; i < count; i++)
            {
                if (i != ig)
                {
                    //id = timerVals.ReadUShort((int)startaddr + i * 2);

                    id = tlen == 2 ? timerVals.ReadUShort((int)startaddr + i * 2) : timerVals.ReadInt((int)startaddr + i * 4);

                    if (isFirst)
                    {
                        mVarintMemory2.WriteSInt32(id);
                        isFirst = false;
                    }
                    else
                    {
                        var vld = id - preids;
                        mVarintMemory2.WriteSInt32(vld - lastdec);
                        lastdec = vld;
                        //mVarintMemory2.WriteInt32(id - preids);
                    }
                    rcount++;
                    //dtims.Add(i, id);
                    preids = id;
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                    //    emptyIds.TryDequeue(out ig);
                }
            }

            LoggerService.Service.Debug("DeadAreaCompress", "记录时间个数:" + rcount + " 空时间个数:" + (emptyIds.WriteIndex + 1));

            return mVarintMemory2.DataBuffer.AsMemory(0, (int)mVarintMemory2.WritePosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="startaddr"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        /// <returns></returns>
        protected virtual Memory<byte> CompressTimers2Old(IMemoryFixedBlock timerVals, long startaddr, int count, CustomQueue<int> emptyIds)
        {
            int preids = 0;
            mVarintMemory2.Reset();
            //dtims.Clear();
            bool isFirst = true;
            int id = 0;

            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;

            byte tlen = (timerVals as HisDataMemoryBlock).TimeLen;

            int rcount = 0;

            for (int i = 0; i < count; i++)
            {
                if (i != ig)
                {
                    //id = timerVals.ReadUShort((int)startaddr + i * 2);

                    id = tlen == 2 ? timerVals.ReadUShort((int)startaddr + i * 2) : timerVals.ReadInt((int)startaddr + i * 4);

                    if (isFirst)
                    {
                        mVarintMemory2.WriteInt32(id);
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory2.WriteInt32(id - preids);
                    }
                    rcount++;
                    //dtims.Add(i, id);
                    preids = id;
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                    //    emptyIds.TryDequeue(out ig);
                }
            }

            LoggerService.Service.Debug("DeadAreaCompress", "记录时间个数:"+rcount +" 空时间个数:"+ (emptyIds.WriteIndex+1));

            return mVarintMemory2.DataBuffer.AsMemory(0, (int)mVarintMemory2.WritePosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <param name="statisticTarget"></param>
        /// <param name="statisticAddr"></param>
        /// <param name="timeAddr"></param>
        /// <returns></returns>
        public override long CompressByArea(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, IMemoryBlock statisticTarget, long statisticAddr, ref long timeAddr)
        {
            return Compress(source, sourceAddr, target, targetAddr, size, statisticTarget, statisticAddr);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        protected override long Compress<T>(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);
            //var tims = source.ReadUShorts(sourceAddr, (int)count);

            byte tlen = (source as HisDataMemoryBlock).TimeLen;

            if (mMarshalMemory == null)
            {
                mMarshalMemory = new MemoryBlock(count * 10);
            }
            else
            {
                mMarshalMemory.CheckAndResize(count * 10);
            }

            if (mVarintMemory == null)
            {
                mVarintMemory = new ProtoMemory(count * 10);
            }
            else if (mVarintMemory.DataBuffer.Length < count * 10)
            {
                mVarintMemory.Dispose();
                mVarintMemory = new ProtoMemory(count * 10);
            }

            if (mVarintMemory2 == null)
            {
                mVarintMemory2 = new ProtoMemory(count * 10);
            }
            else if (mVarintMemory2.DataBuffer.Length < count * 10)
            {
                mVarintMemory2.Dispose();
                mVarintMemory2 = new ProtoMemory(count * 10);
            }

            emptys.CheckAndResize(count);
            emptys2.CheckAndResize(count);

            emptys2.Reset();

            long rsize = 0;

            switch (type)
            {
                case TagType.Bool:

                    var datas = CompressTimers(source, sourceAddr, (int)count, emptys);

                    var cval = CompressBoolValues(source, count * tlen + sourceAddr, count, emptys);

                    int rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;

                    //写入数据
                    target.Write(cval.Length);
                    target.Write(cval);
                    rsize += 4;
                    rsize += cval.Length;
                    emptys.ReadIndex = 0;

                    //写入质量戳
                    var cqus = CompressQualitys(source, count * (tlen + 1) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Byte:

                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    cval = CompressValues<byte>(source, count * tlen + sourceAddr, count, emptys, TagType);

                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;

                    //写入数据
                    target.Write(cval.Length);
                    target.Write(cval);
                    rsize += 4;
                    rsize += cval.Length;
                    emptys2.ReadIndex = 0;

                    //写入质量戳
                    cqus = CompressQualitys(source, count * (tlen + 1) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UShort:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    if (mUInt16Compress == null)
                    {
                        mUInt16Compress = new UInt16CompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mUInt16Compress.VarintMemory == null || mUInt16Compress.VarintMemory.DataBuffer == null)
                        {
                            mUInt16Compress.VarintMemory = mVarintMemory;
                        }
                        mUInt16Compress.CheckAndResizeTo(count);
                    }
                    var ures = CompressValues<ushort>(source, count * tlen + sourceAddr, count, emptys, TagType);

                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(ures.Length);
                    target.Write(ures);
                    rsize += 4;
                    rsize += ures.Length;
                    //质量戳
                    emptys2.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 2) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Short:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);

                    if (mInt16Compress == null)
                    {
                        mInt16Compress = new Int16CompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mInt16Compress.VarintMemory == null || mInt16Compress.VarintMemory.DataBuffer == null)
                        {
                            mInt16Compress.VarintMemory = mVarintMemory;
                        }
                        mInt16Compress.CheckAndResizeTo(count);
                    }
                    var res = CompressValues<short>(source, count * tlen + sourceAddr, count, emptys, TagType);

                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(res.Length);
                    target.Write(res);
                    rsize += 4;
                    rsize += res.Length;

                    emptys2.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 2) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UInt:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    if (mUIntCompress == null)
                    {
                        mUIntCompress = new UIntCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mUIntCompress.VarintMemory == null || mUIntCompress.VarintMemory.DataBuffer == null)
                        {
                            mUIntCompress.VarintMemory = mVarintMemory;
                        }
                        mUIntCompress.CheckAndResizeTo(count);
                    }
                    var uires = CompressValues<uint>(source, count * tlen + sourceAddr, count, emptys, TagType);

                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(uires.Length);
                    target.Write(uires);
                    rsize += 4;
                    rsize += uires.Length;
                    //质量
                    emptys2.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Int:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);

                    if (mIntCompress == null)
                    {
                        mIntCompress = new IntCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mIntCompress.VarintMemory == null || mIntCompress.VarintMemory.DataBuffer == null)
                        {
                            mIntCompress.VarintMemory = mVarintMemory;
                        }
                        mIntCompress.CheckAndResizeTo(count);
                    }

                    var ires = CompressValues<int>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(ires.Length);
                    target.Write(ires);
                    rsize += 4;
                    rsize += ires.Length;
                    emptys2.ReadIndex = 0;
                    //质量
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULong:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    if (mUInt64Compress == null)
                    {
                        mUInt64Compress = new UInt64CompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mUInt64Compress.VarintMemory == null || mUInt64Compress.VarintMemory.DataBuffer == null)
                        {
                            mUInt64Compress.VarintMemory = mVarintMemory;
                        }
                        mUInt64Compress.CheckAndResizeTo(count);
                    }
                    var ulres = CompressValues<ulong>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(ulres.Length);
                    target.Write(ulres);
                    rsize += 4;
                    rsize += ulres.Length;
                    //质量
                    emptys2.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Long:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    if (mInt64Compress == null)
                    {
                        mInt64Compress = new Int64CompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mInt64Compress.VarintMemory == null || mInt64Compress.VarintMemory.DataBuffer == null)
                        {
                            mInt64Compress.VarintMemory = mVarintMemory;
                        }
                        mInt64Compress.CheckAndResizeTo(count);
                    }
                    var lres = CompressValues<long>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(lres.Length);
                    target.Write(lres);
                    rsize += 4;
                    rsize += lres.Length;
                    //质量
                    emptys2.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.DateTime:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);

                    var dres = CompressValues<ulong>(source, count * tlen + sourceAddr, count, emptys, TagType);

                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(dres.Length);
                    target.Write(dres);
                    rsize += 4;
                    rsize += dres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Double:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    if (mDCompress == null)
                    {
                        mDCompress = new DoubleCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mDCompress.VarintMemory == null || mDCompress.VarintMemory.DataBuffer == null)
                        {
                            mDCompress.VarintMemory = mVarintMemory;
                        }
                        mDCompress.CheckAndResizeTo(count);
                    }

                    var ddres = CompressValues<double>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    rsize += 4;
                    target.Write(datas);
                    rsize += datas.Length;

                    if(rcount>0 && datas.Length==0)
                    {
                        LoggerService.Service.Debug("DeadAreaCompressUnit", "压缩后数据长度为0 :"+rcount);
                    }

                    //写入数据
                    target.Write(ddres.Length);
                    target.Write(ddres);
                    rsize += 4;
                    rsize += ddres.Length;
                    //质量
                    emptys2.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;

                    //StringBuilder sb = new StringBuilder();
                    //foreach(var vv in dvals)
                    //{
                    //    sb.Append(dtims[vv.Key] + "," + vv.Value+";");
                    //}
                    //System.IO.File.WriteAllText(DateTime.Now.Ticks.ToString() + ".deadarea", sb.ToString());

                    break;
                case TagType.Float:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    if (mFCompress == null)
                    {
                        mFCompress = new FloatCompressBuffer(count) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    }
                    else
                    {
                        if (mFCompress.VarintMemory == null || mFCompress.VarintMemory.DataBuffer == null)
                        {
                            mFCompress.VarintMemory = mVarintMemory;
                        }
                        mFCompress.CheckAndResizeTo(count);
                    }

                    var fres = CompressValues<float>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    datas = CompressTimers2(source, sourceAddr, (int)count, emptys2);
                    rcount = count - emptys2.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //写入数据
                    target.Write(fres.Length);
                    target.Write(fres);
                    rsize += 4;
                    rsize += fres.Length;
                    //质量
                    emptys2.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.String:

                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;

                    //写入数据
                    var vals = source.ReadStrings(count * tlen + (int)sourceAddr, count);
                    var qus = source.ReadBytes(count);
                    byte[] btmp;
                    var sres = CompressValues2(vals, emptys,out btmp);
                    target.Write(sres.Length);
                    target.Write(sres);
                    ArrayPool<byte>.Shared.Return(btmp);

                    rsize += 4;
                    rsize += sres.Length;
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(qus, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.IntPoint:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //数值
                    var ipres = CompressValues<IntPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UIntPoint:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //数值
                    ipres = CompressValues<UIntPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.LongPoint:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //值
                    ipres = CompressValues<LongPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 16) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULongPoint:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //值
                    ipres = CompressValues<ULongPointData>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 16) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.IntPoint3:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;

                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //值
                    ipres = CompressValues<IntPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 12) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UIntPoint3:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //值
                    ipres = CompressValues<UIntPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 12) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.LongPoint3:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //值
                    ipres = CompressValues<LongPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 24) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULongPoint3:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);
                    rcount = count - emptys.WriteIndex - 1;
                    //写入时间
                    target.WriteInt(targetAddr, rcount);
                    rsize += 4;
                    target.Write((int)datas.Length);
                    target.Write(datas);
                    rsize += 4;
                    rsize += datas.Length;
                    //值
                    ipres = CompressValues<ULongPoint3Data>(source, count * tlen + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQualitys(source, count * (tlen + 24) + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
            }
            return rsize;

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
        protected override Memory<byte> CompressValues<T>(IMemoryFixedBlock source, long offset, int count, CustomQueue<int> emptys, TagType type)
        {
            var deadArea = this.Parameters.ContainsKey("DeadValue") ? this.Parameters["DeadValue"] : 0;
            var deadType = (int)(this.Parameters.ContainsKey("DeadType") ? this.Parameters["DeadType"] : 0);

            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();

            bool isFirst = true;

            int ig = -1;
            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;

            switch (type)
            {
                case TagType.Byte:
                    byte sval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadByte((int)offset + i);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count + i);
                                if (qus >= 100)
                                {
                                    mMarshalMemory.Write(id);
                                }
                            }

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
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Short:
                    short ssval = 0;
                    mInt16Compress.Reset();
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadShort((int)offset + i * 2);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count*2 + i);
                                if (qus >= 100)
                                {
                                    mInt16Compress.Append(id);
                                    // mVarintMemory.WriteSInt32(id);
                                    ssval = id;
                                    continue;
                                }
                            }

                            if (isFirst)
                            {
                                mInt16Compress.Append(id);
                                // mVarintMemory.WriteSInt32(id- ssval);
                                isFirst = false;
                                ssval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(ssval, id, deadArea, deadType))
                                {
                                    mInt16Compress.Append(id);
                                    //mVarintMemory.WriteSInt32(id - ssval);
                                    ssval = id;
                                }
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    mInt16Compress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.UShort:
                    ushort ussval = 0;
                    mUInt16Compress.Reset();
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUShort((int)offset + i * 2);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count * 2 + i);
                                if (qus >= 100)
                                {
                                    mUInt16Compress.Append(id);
                                    //mVarintMemory.WriteSInt32(id);
                                    ussval = id;
                                    continue;
                                }
                            }

                            if (isFirst)
                            {
                                mUInt16Compress.Append(id);
                                //mVarintMemory.WriteSInt32(id - ussval);
                                isFirst = false;
                                ussval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(ussval, id, deadArea, deadType))
                                {
                                    mUInt16Compress.Append(id);
                                    // mVarintMemory.WriteSInt32(id - ussval);
                                    ussval = id;
                                }
                                else
                                {
                                    emptys2.Insert(i);
                                }

                            }
                        }
                        else
                        {

                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    mUInt16Compress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Int:
                    int isval = 0;
                    mIntCompress.Reset();
                    //bool hasdata = false;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i * 4);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count * 4 + i);
                                if (qus >= 100)
                                {
                                    mIntCompress.Append(id);
                                    //mVarintMemory.WriteInt32(id);
                                    isval = id;
                                    //hasdata = true;
                                    continue;
                                }
                            }

                            if (isFirst)
                            {
                                //if(hasdata)
                                //mVarintMemory.WriteSInt32(id - isval);
                                //else
                                //{
                                //    mVarintMemory.WriteInt32(id);
                                //}
                                mIntCompress.Append(id);
                                isFirst = false;
                                isval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(isval, id, deadArea, deadType))
                                {
                                    mIntCompress.Append(id);
                                    // mVarintMemory.WriteSInt32(id - isval);
                                    isval = id;
                                }
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    mIntCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.UInt:
                    uint usval = 0;
                    mUIntCompress.Reset();
                    //hasdata = false;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 4);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count * 4 + i);
                                if (qus >= 100)
                                {
                                    mUIntCompress.Append(id);
                                    //mVarintMemory.WriteInt32(id);
                                    usval = id;
                                    //hasdata = true;
                                    continue;
                                }
                            }

                            if (isFirst)
                            {
                                mUIntCompress.Append(id);
                                //if (hasdata)
                                //{
                                //    mVarintMemory.WriteSInt32((int)(id - usval));
                                //}
                                //else
                                //{
                                //    mVarintMemory.WriteInt32(id);
                                //}
                                isFirst = false;
                                usval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(usval, id, deadArea, deadType))
                                {
                                    mUIntCompress.Append(id);
                                    //mVarintMemory.WriteSInt32((int)(id - usval));
                                    usval = id;
                                }
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    mUIntCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Long:
                    long lsval = 0;
                    mInt64Compress.Reset();
                    //hasdata = false;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 8);
                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count * 8 + i);
                                if (qus >= 100)
                                {
                                    mInt64Compress.Append(id);
                                    // mVarintMemory.WriteInt64(id);
                                    //hasdata = true;
                                    lsval = id;
                                    continue;
                                }
                            }

                            if (isFirst)
                            {
                                mInt64Compress.Append(id);
                                //if (hasdata)
                                //{
                                //    mVarintMemory.WriteSInt64((id - lsval));
                                //}
                                //else
                                //{
                                //    mVarintMemory.WriteInt64(id);
                                //}
                                isFirst = false;
                                lsval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(lsval, id, deadArea, deadType))
                                {
                                    mInt64Compress.Append(id);
                                    // mVarintMemory.WriteSInt64((id - lsval));
                                    lsval = id;
                                }
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    mInt64Compress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.ULong:
                    ulong ulsval = 0;
                    mUInt64Compress.Reset();
                    //hasdata = false;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 8);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count * 8 + i);
                                if (qus >= 100)
                                {
                                    mUInt64Compress.Append(id);
                                    // mVarintMemory.WriteInt64(id);
                                    //hasdata = true;
                                    ulsval = id;
                                    continue;
                                }
                            }

                            if (isFirst)
                            {
                                mUInt64Compress.Append(id);
                                //if (hasdata)
                                //{
                                //    mVarintMemory.WriteSInt64((long)(id - ulsval));
                                //}
                                //else
                                //{
                                //    mVarintMemory.WriteInt64(id);
                                //}
                                isFirst = false;
                                ulsval = id;
                            }
                            else
                            {
                                if (CheckIsNeedRecord(ulsval, id, deadArea, deadType))
                                {
                                    mUInt64Compress.Append(id);
                                    // mVarintMemory.WriteSInt64((long)(id - ulsval));
                                    ulsval = id;
                                }
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    mUInt64Compress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                case TagType.Double:
                    double dsval = 0;
                    mDCompress.Reset();
                    mDCompress.Precision = this.Precision;

                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadDouble((int)offset + i * 8);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count * 8 + i);
                                if (qus >= 100)
                                {
                                    mDCompress.Append(id);
                                    dsval = id;
                                    continue;
                                }
                            }

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
                                    dsval = id;
                                }
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
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
                            var id = source.ReadFloat((int)offset + i * 4);

                            //对于第一个和最后一个的质量戳为100的情况
                            //直接记录
                            if (i == 0 || i == count - 1)
                            {
                                var qus = source.ReadByte(offset + count * 4 + i);
                                if (qus >= 100)
                                {
                                    mFCompress.Append(id);
                                    fsval = id;
                                    continue;
                                }
                            }
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
                                else
                                {
                                    emptys2.Insert(i);
                                }
                            }
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                            emptys2.Insert(i);
                        }
                    }
                    mFCompress.Compress();
                    return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
                default:
                   return base.CompressValues<T>(source, offset, count, emptys, type);
            }
            //return mVarintMemory.DataBuffer.AsMemory<byte>(0, (int)mVarintMemory.WritePosition);
        }
    }
}
