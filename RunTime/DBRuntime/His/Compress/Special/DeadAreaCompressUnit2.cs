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
        protected void FindEmpityIds(IMemoryBlock timerVals, long startaddr, int count, CustomQueue<int> emptyIds)
        {
            emptys.Reset();
            int id = 0;
            for (int i = 0; i < count; i++)
            {
                id = timerVals.ReadUShort((int)startaddr + i * 2);
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
        protected virtual Memory<byte> CompressTimers2(IMemoryBlock timerVals, long startaddr, int count, CustomQueue<int> emptyIds)
        {
            int preids = 0;
            mVarintMemory2.Reset();
            //dtims.Clear();
            bool isFirst = true;
            int id = 0;

            int ig = -1;
            ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;

            byte tlen = (timerVals as HisDataMemoryBlock).TimeLen;

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
                    //dtims.Add(i, id);
                    preids = id;
                }
                else
                {
                    ig = emptyIds.ReadIndex <= emptyIds.WriteIndex ? emptyIds.IncRead() : -1;
                    //    emptyIds.TryDequeue(out ig);
                }
            }
            return mVarintMemory2.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
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
        protected override long Compress<T>(IMemoryBlock source, long sourceAddr, MarshalMemoryBlock target, long targetAddr, long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);
            //var tims = source.ReadUShorts(sourceAddr, (int)count);

            if (mMarshalMemory == null)
            {
                mMarshalMemory = new MemoryBlock(count * 10);
            }

            if (mVarintMemory == null)
            {
                mVarintMemory = new ProtoMemory(count * 10);
            }

            if (mVarintMemory2 == null)
            {
                mVarintMemory2 = new ProtoMemory(count * 10);
            }

            if (emptys2.Length < count)
            {
                emptys2 = new CustomQueue<int>(count * 2);
            }
            else
            {
                emptys2.Reset();
            }

            long rsize = 0;

            switch (type)
            {
                case TagType.Bool:

                    var datas = CompressTimers(source, sourceAddr, (int)count, emptys);

                    var cval = CompressBoolValues(source, count * 2 + sourceAddr, count, emptys);

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
                    var cqus = CompressQulitys(source, count * 3 + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Byte:

                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    cval = CompressValues<byte>(source, count * 2 + sourceAddr, count, emptys, TagType);

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
                    cqus = CompressQulitys(source, count * 3 + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UShort:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    var ures = CompressValues<ushort>(source, count * 2 + sourceAddr, count, emptys, TagType);

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
                    cqus = CompressQulitys(source, count * 4 + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Short:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    var res = CompressValues<short>(source, count * 2 + sourceAddr, count, emptys, TagType);

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
                    cqus = CompressQulitys(source, count * 4 + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UInt:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    var uires = CompressValues<uint>(source, count * 2 + sourceAddr, count, emptys, TagType);

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
                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Int:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    var ires = CompressValues<int>(source, count * 2 + sourceAddr, count, emptys, TagType);
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
                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULong:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    var ulres = CompressValues<ulong>(source, count * 2 + sourceAddr, count, emptys, TagType);
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
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Long:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    var lres = CompressValues<long>(source, count * 2 + sourceAddr, count, emptys, TagType);
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
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, emptys2);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.DateTime:
                    datas = CompressTimers(source, sourceAddr, (int)count, emptys);

                    var dres = CompressValues<ulong>(source, count * 2 + sourceAddr, count, emptys, TagType);

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
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, emptys);
                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Double:
                    FindEmpityIds(source, sourceAddr, (int)count, emptys);
                    if (mDCompress == null) mDCompress = new DoubleCompressBuffer(310) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };

                    var ddres = CompressValues<double>(source, count * 2 + sourceAddr, count, emptys, TagType);
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
                    target.Write(ddres.Length);
                    target.Write(ddres);
                    rsize += 4;
                    rsize += ddres.Length;
                    //质量
                    emptys2.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, emptys2);
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
                    if (mFCompress == null) mFCompress = new FloatCompressBuffer(310) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };

                    var fres = CompressValues<float>(source, count * 2 + sourceAddr, count, emptys, TagType);
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
                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, emptys2);
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

                    var vals = source.ReadStrings(count * 2 + (int)sourceAddr, count);
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
                    var ipres = CompressValues<IntPointData>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, emptys);
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
                    ipres = CompressValues<UIntPointData>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, emptys);
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
                    ipres = CompressValues<LongPointData>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 18 + sourceAddr, count, emptys);
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
                    ipres = CompressValues<ULongPointData>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 18 + sourceAddr, count, emptys);
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
                    ipres = CompressValues<IntPoint3Data>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 14 + sourceAddr, count, emptys);
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
                    ipres = CompressValues<UIntPoint3Data>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 14 + sourceAddr, count, emptys);
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
                    ipres = CompressValues<LongPoint3Data>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 26 + sourceAddr, count, emptys);
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
                    ipres = CompressValues<ULongPoint3Data>(source, count * 2 + sourceAddr, count, emptys, TagType);
                    target.Write(ipres.Length);
                    target.Write(ipres);
                    rsize += 4;
                    rsize += ipres.Length;
                    //质量
                    emptys.ReadIndex = 0;
                    cqus = CompressQulitys(source, count * 26 + sourceAddr, count, emptys);
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
        protected override Memory<byte> CompressValues<T>(IMemoryBlock source, long offset, int count, CustomQueue<int> emptys, TagType type)
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
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadShort((int)offset + i * 2);
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
                    break;
                case TagType.UShort:
                    ushort ussval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUShort((int)offset + i * 2);
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
                                if (CheckIsNeedRecord(isval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt32(id - isval);
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
                    break;
                case TagType.UInt:
                    uint usval = 0;
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i * 4);
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
                                if (CheckIsNeedRecord(lsval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt64((id - lsval));
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
                                if (CheckIsNeedRecord(ulsval, id, deadArea, deadType))
                                {
                                    mVarintMemory.WriteSInt64((long)(id - ulsval));
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
                    break;
                case TagType.Double:
                    double dsval = 0;
                    mDCompress.Reset();
                    mDCompress.Precision = this.Precision;
                    //dvals.Clear();

                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadDouble((int)offset + i * 8);
                            if (isFirst)
                            {
                                mDCompress.Append(id);
                                isFirst = false;
                                dsval = id;
                                //dvals.Add(i, id);
                            }
                            else
                            {
                                if (CheckIsNeedRecord(dsval, id, deadArea, deadType))
                                {
                                    mDCompress.Append(id);
                                    // mMarshalMemory.Write(id);
                                    dsval = id;
                                    //dvals.Add(i, id);
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
            return mVarintMemory.DataBuffer.AsMemory<byte>(0, (int)mVarintMemory.WritePosition);
        }
    }
}
