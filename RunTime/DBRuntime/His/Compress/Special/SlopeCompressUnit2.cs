//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//  1. 2020/7/1 三沙永兴岛 优化斜率压缩算法  种道洋
//==============================================================
using DBRuntime.His;
using DBRuntime.His.Compress;
using Google.Protobuf.WellKnownTypes;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 斜率压缩算法
    /// 斜率压缩采用数据块有多个数据段组成，每个数据段以数据段的字节数。遇到无效数据后新启一个数据段
    /// </summary>
    public class SlopeCompressUnit2 : LosslessCompressUnit2
    {

        #region ... Variables  ...
        protected MemoryBlock mAvaiableDatabuffer;

        /// <summary>
        /// 
        /// </summary>
        protected CustomQueue<int> usedIndex = new CustomQueue<int>(604);

        protected ProtoMemory mVarintMemory2;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public override string Desc => "斜率死区";

        /// <summary>
        /// 
        /// </summary>
        public override int TypeCode => 3;


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override CompressUnitbase2 Clone()
        {
            return new SlopeCompressUnit2();
        }

        /// <summary>
        /// 时间2次增量压缩
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="usedIndex"></param>
        /// <returns></returns>
        protected Memory<byte> CompressTimers(int[] timerVals, CustomQueue<int> usedIndex,int count)
        {
            usedIndex.ReadIndex = 0;
            int preids = 0;
            int ig = usedIndex.ReadIndex <= usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
            bool isFirst = true;
            int lastdec = 0;
          
            mVarintMemory2.Reset();
            for (int i = 0; i < count; i++)
            {
                if (i == ig)
                {
                    var id = timerVals[i];
                    if (isFirst)
                    {
                        isFirst = false;
                        mVarintMemory2.WriteSInt32(id);
                    }
                    else
                    {
                        var vld = id - preids;
                        mVarintMemory2.WriteSInt32(vld - lastdec);
                        lastdec = vld;
                    }
                    preids = id;
                    ig = usedIndex.ReadIndex <= usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
                }
            }
            return mVarintMemory2.DataBuffer.AsMemory(0, (int)mVarintMemory2.WritePosition);
        }


        /// <summary>
        /// 时间一次增量压缩
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="usedIndex"></param>
        /// <returns></returns>
        protected  Memory<byte> CompressTimersOld(int[] timerVals, CustomQueue<int> usedIndex)
        {
            usedIndex.ReadIndex = 0;
            int preids = 0;
            int ig = usedIndex.ReadIndex <= usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
            bool isFirst = true;

            mVarintMemory2.Reset();
            for (int i = 0; i < timerVals.Length; i++)
            {
                if (i == ig)
                {
                    var id = timerVals[i];
                    if (isFirst)
                    {
                        isFirst = false;
                        mVarintMemory2.WriteInt32(id);
                    }
                    else
                    {
                        mVarintMemory2.WriteInt32(id - preids);
                    }
                    preids = id;
                    ig = usedIndex.ReadIndex <= usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
                }
            }
            return mVarintMemory2.DataBuffer.AsMemory(0, (int)mVarintMemory2.WritePosition);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        private void GetEmpityTimers(int[] timerVals,int count)
        {
            emptys.Reset();
            for (int i = 1; i < count; i++)
            {
                if (timerVals[i] <= 0)
                {
                    emptys.Insert(i);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="totalcount"></param>
        /// <param name="usedIndex"></param>
        /// <returns></returns>
        protected new  Memory<byte> CompressQualitys(IMemoryFixedBlock source, long offset, int totalcount, CustomQueue<int> usedIndex)
        {
            usedIndex.ReadIndex = 0;

            int count = 0;
            byte qus = 0;
            mVarintMemory.Reset();
            int ig = usedIndex.ReadIndex <= usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
            bool isFirst = true;
            
            for (int i = 0; i < totalcount; i++)
            {
                if (i == ig)
                {
                    byte bval = source.ReadByte(offset + i);
                    if (isFirst)
                    {
                        isFirst = false;
                        qus = bval;
                        mVarintMemory.WriteInt32(qus);
                        count++;
                    }
                    else
                    {
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
                    ig = usedIndex.ReadIndex <= usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
                }
            }
            mVarintMemory.WriteInt32(count);
            return mVarintMemory.DataBuffer.AsMemory(0, (int)mVarintMemory.WritePosition);
        }

        
        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="fval"></param>
        /// <param name="lastVal"></param>
        /// <param name="timSpan"></param>
        /// <returns></returns>
        private double CalSlope(object fval, object lastVal,int timSpan,byte tlen)
        {
            //转换成以秒为时间单位的斜率计算
            //timesSpan是以100ms为单位的时间间隔
            if (tlen == 2)
            {
                return (Convert.ToDouble(lastVal) - Convert.ToDouble(fval)) / (timSpan * TimeTick);
            }
            else
            {
                return (Convert.ToDouble(lastVal) - Convert.ToDouble(fval)) / (timSpan);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="preVal"></param>
        /// <param name="newVal"></param>
        /// <param name="area"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private bool CheckIsNeedRecord(double preVal, double newVal, double area, int type)
        {
            if (type == 0)
            {
                return Math.Abs(newVal - preVal) > area;
            }
            else
            {
                return Math.Abs((newVal - preVal) / preVal) > area;
            }
        }

        #region SlopeCompress
        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressByte(MemoryBlock values, CustomQueue<int> mUsedTimerIndex,byte tlen)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            byte mLastValue = 0;

            int mStartTime = 0;
            byte mStartValue = 0;

            mMarshalMemory.Position = 0;
           
            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadByte();

                        mMarshalMemory.Write(mLastValue);
                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadByte();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mMarshalMemory.Write(mLastValue);
                            }

                            mMarshalMemory.Write(vval);
                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mMarshalMemory.Write(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(mlastid);
                    mMarshalMemory.Write(mLastValue);
                }
            }
 
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressShort(MemoryBlock values,  CustomQueue<int> mUsedTimerIndex, byte tlen)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            short mLastValue = 0;

            int mStartTime = 0;
            short mStartValue = 0;

            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            mInt16Compress.Reset();

            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadShort();

                        //mMarshalMemory.Write(mLastValue);
                        mInt16Compress.Append(mLastValue);

                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadShort();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mInt16Compress.Append(mLastValue);
                            }

                            mInt16Compress.Append(vval);

                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mInt16Compress.Append(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    mUsedTimerIndex.Insert(mlastid);
                    mInt16Compress.Append(mLastValue);
                }
            }
            mInt16Compress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressUShort(MemoryBlock values, CustomQueue<int> mUsedTimerIndex, byte tlen)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            ushort mLastValue = 0;

            int mStartTime = 0;
            ushort mStartValue = 0;

            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            mUInt16Compress.Reset();

            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadUShort();

                        mUInt16Compress.Append(mLastValue);

                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadUShort();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mUInt16Compress.Append(mLastValue);
                            }

                            mUInt16Compress.Append(vval);

                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mUInt16Compress.Append(mLastValue);
                                    // mMarshalMemory.Write(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    mUsedTimerIndex.Insert(mlastid);
                    mUInt16Compress.Append(mLastValue);
                    // mMarshalMemory.Write(mLastValue);
                }
            }
            mUInt16Compress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressInt(MemoryBlock values, CustomQueue<int> mUsedTimerIndex, byte tlen)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            int mLastValue = 0;

            int mStartTime = 0;
            int mStartValue = 0;


            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            mIntCompress.Reset();

            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadInt();

                        mIntCompress.Append(mLastValue);

                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadInt();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mIntCompress.Append(mLastValue);
                            }
                            mIntCompress.Append(vval);

                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mIntCompress.Append(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    mUsedTimerIndex.Insert(mlastid);
                    mIntCompress.Append(mLastValue);
                }
            }
            mIntCompress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressUInt(MemoryBlock values,  CustomQueue<int> mUsedTimerIndex,byte tlen)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            uint mLastValue = 0;

            int mStartTime = 0;
            uint mStartValue = 0;

            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            mUIntCompress.Reset();

            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadUInt();

                        mUIntCompress.Append(mLastValue);
                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadUInt();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mUIntCompress.Append(mLastValue);
                            }

                            mUIntCompress.Append(vval);

                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mUIntCompress.Append(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    mUsedTimerIndex.Insert(mlastid);
                    mUIntCompress.Append(mLastValue);
                }
            }
            mUIntCompress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressLong(MemoryBlock values, CustomQueue<int> mUsedTimerIndex, byte tlen)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            long mLastValue = 0;

            int mStartTime = 0;
            long mStartValue = 0;


            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            mInt64Compress.Reset();

            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadLong();

                        mInt64Compress.Append(mLastValue);
                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadLong();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mInt64Compress.Append(mLastValue);
                            }

                            mInt64Compress.Append(vval);

                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mInt64Compress.Append(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    mUsedTimerIndex.Insert(mlastid);
                    mInt64Compress.Append(mLastValue);
                }
            }
            mInt64Compress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressULong(MemoryBlock values,CustomQueue<int> mUsedTimerIndex, byte tlen)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            ulong mLastValue = 0;

            int mStartTime = 0;
            ulong mStartValue = 0;

            mMarshalMemory.Position = 0;
            mVarintMemory.Reset();
            mUInt64Compress.Reset();

            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadULong();

                        mUInt64Compress.Append(mLastValue);

                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadULong();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mUInt64Compress.Append(mLastValue);
                            }

                            mUInt64Compress.Append(vval);

                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mUInt64Compress.Append(mLastValue);

                                    // mMarshalMemory.Write(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    mUsedTimerIndex.Insert(mlastid);
                    mUInt64Compress.Append(mLastValue);
                }
            }
            mUInt64Compress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressDouble(MemoryBlock values,CustomQueue<int> mUsedTimerIndex, byte tlen)
        {

            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            //emptys.ReadIndex = 0;
            //int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            int mLastTime = 0;
            double mLastValue = 0;

            int mStartTime = 0;
            double mStartValue = 0;

            mVarintMemory.Reset();
            mMarshalMemory.Position = 0;
            mDCompress.Reset();
            mDCompress.Precision = this.Precision;
            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadDouble();

                        mDCompress.Append(mLastValue);
                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadDouble();
                        if ((id-mlastid)>1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if(mLastTime!=mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mDCompress.Append(mLastValue);
                            }

                            mDCompress.Append(vval);
                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime  = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mDCompress.Append(mLastValue);

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(mlastid);
                    mDCompress.Append(mLastValue);
                }
            }
            mDCompress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressFloat(MemoryBlock values,  CustomQueue<int> mUsedTimerIndex, byte tlen)
        {

            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            int mLastTime = 0;
            float mLastValue = 0;

            int mStartTime = 0;
            float mStartValue = 0;

            mVarintMemory.Reset();
            mMarshalMemory.Position = 0;
            mFCompress.Reset();
            mFCompress.Precision = this.Precision;
            int id = 0;
            int mlastid = 0;
            if (count > 0)
            {
                for (int i = 0; i < count; i++)
                {
                    if (!isStarted)
                    {
                        isStarted = true;

                        mlastid = id = values.ReadInt();
                        mLastTime = mStartTime = values.ReadInt();
                        mLastValue = mStartValue = values.ReadFloat();

                        mFCompress.Append(mLastValue);
                        mUsedTimerIndex.Insert(id);
                        minslope = double.MinValue;
                    }
                    else
                    {
                        id = values.ReadInt();
                        var vkey = values.ReadInt();
                        var vval = values.ReadFloat();
                        if ((id - mlastid) > 1)
                        {
                            //说明之间有无效值，需要重新计算斜率

                            if (mLastTime != mStartTime)
                            {
                                mUsedTimerIndex.Insert(mlastid);
                                mFCompress.Append(mLastValue);
                            }

                            mFCompress.Append(vval);
                            mUsedTimerIndex.Insert(id);

                            mLastTime = mStartTime = vkey;
                            mLastValue = mStartValue = vval;
                            mlastid = id;
                            minslope = double.MinValue;

                            continue;
                        }
                        else
                        {
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime,tlen);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(mlastid);

                                    mFCompress.Append(mLastValue);

                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime, tlen);
                                }

                            }
                        }
                        mLastTime = vkey;
                        mLastValue = vval;
                        mlastid = id;
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(mlastid);
                    mFCompress.Append(mLastValue);
                }
            }
            mFCompress.Compress();
            return mMarshalMemory.StartMemory.AsMemory<byte>(0, (int)mMarshalMemory.Position);
        }
       
        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="offset"></param>
        /// <param name="count"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mTimers"></param>
        /// <param name="usedIndex"></param>
        /// <returns></returns>
        protected Memory<byte> CompressValues<T>(IMemoryFixedBlock source, long offset, int count, int[] mTimers,TagType type,byte tlen)
        {
            int ig = -1;
            emptys.ReadIndex = 0;

            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
            int ac = 0;
            mAvaiableDatabuffer.Position = 0;
            mAvaiableDatabuffer.Write((int)0);

            switch (type)
            {
                case TagType.Byte:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadByte((int)offset + i);

                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                   // return null;
                return SlopeCompressByte(mAvaiableDatabuffer,  usedIndex,tlen);
                case TagType.Short:
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

                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadShort((int)offset + i*2);
                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                return SlopeCompressShort(mAvaiableDatabuffer,  usedIndex, tlen);
                case TagType.UShort:
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

                    //Dictionary<ushort, ushort> mavaibleValues = new Dictionary<ushort, ushort>();
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUShort((int)offset + i*2);
                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);

                    return SlopeCompressUShort(mAvaiableDatabuffer,  usedIndex, tlen);
                case TagType.Int:
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
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt((int)offset + i*4);
                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                //    return null;
                return SlopeCompressInt(mAvaiableDatabuffer,  usedIndex, tlen);
                case TagType.UInt:
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
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt((int)offset + i*4);
                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                return SlopeCompressUInt(mAvaiableDatabuffer,  usedIndex, tlen);
                case TagType.Long:
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
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong((int)offset + i * 8);
                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                 return SlopeCompressLong(mAvaiableDatabuffer,  usedIndex, tlen);
                case TagType.ULong:
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
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong((int)offset + i * 8);
                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    //return null;
                 return SlopeCompressULong(mAvaiableDatabuffer,  usedIndex, tlen);
                case TagType.Double:
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
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            ac++;
                            var id = source.ReadDouble((int)offset + i * 8);
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressDouble(mAvaiableDatabuffer, usedIndex, tlen);
                case TagType.Float:
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
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadFloat((int)offset + i * 4);
                            ac++;
                            mAvaiableDatabuffer.Write(i);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex <= emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    //return null;
                return SlopeCompressFloat(mAvaiableDatabuffer,  usedIndex, tlen);
                default:
                    usedIndex = null;
                    return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="valuedata"></param>
        /// <param name="qualitydata"></param>
        /// <param name="timedata"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <returns></returns>
        private long FillData(Memory<byte> valuedata,Memory<byte> qualitydata, Memory<byte> timedata, IMemoryBlock target, long targetAddr)
        {
            int rsize = 0;

            target.WriteInt(targetAddr, usedIndex.WriteIndex+1);

            rsize += 4;
            target.Write((int)timedata.Length);
            target.Write(timedata);
            rsize += 4;
            rsize += timedata.Length;

            target.Write(valuedata.Length);
            target.WriteMemory(target.Position, valuedata, 0, valuedata.Length);

            rsize += 4;
            rsize += valuedata.Length;


            target.Write(qualitydata.Length);
            target.Write(qualitydata);
            rsize += 4;
            rsize += qualitydata.Length;
            return rsize;
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
        /// <returns></returns>
        protected override long Compress<T>(IMemoryFixedBlock source, long sourceAddr, IMemoryBlock target, long targetAddr, long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);

            byte tlen = (source as HisDataMemoryBlock).TimeLen;

            var tims = System.Buffers.ArrayPool<int>.Shared.Rent(count);

            //int[] tims = new int[count];

            if (tlen == 2)
            {
                for (int i = 0; i < count; i++)
                {
                    tims[i]=(source.ReadUShort(sourceAddr + i * 2));
                }
            }
            else
            {
                for (int i = 0; i < count; i++)
                {
                    tims[i]=(source.ReadInt(sourceAddr + i * 4));
                }
            }

            //tims = tlen == 2 ? source.ReadUShorts(sourceAddr, (int)count).Select(e => (int)e).ToList() : source.ReadInts(sourceAddr, (int)count);

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

            //
            if (mAvaiableDatabuffer == null)
            {
                mAvaiableDatabuffer = new MemoryBlock(count * 20);
            }
            else
            {
                mAvaiableDatabuffer.CheckAndResize(count * 20);
            }

            emptys.CheckAndResize(count);
            usedIndex.CheckAndResize(count);
            usedIndex.Reset();

            GetEmpityTimers(tims,count);

            long rsize = 0;

            switch (type)
            {
                case TagType.Byte:
                    var cval = CompressValues<byte>(source, count * tlen + sourceAddr, count, tims, type,tlen);
                    var timeData = CompressTimers(tims, usedIndex, count);
                    var cqus = CompressQualitys(source, count * (tlen + 1) + sourceAddr, count, usedIndex);

                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Short:
                    cval = CompressValues<short>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 2) + sourceAddr, count, usedIndex);

                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.UShort:
                    cval = CompressValues<ushort>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 2) + sourceAddr, count, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Int:
                    cval = CompressValues<int>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, usedIndex);

                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.UInt:
                    cval = CompressValues<uint>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Long:
                    cval = CompressValues<long>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, usedIndex);

                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.ULong:
                    cval = CompressValues<ulong>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Double:

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

                    cval = CompressValues<double>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 8) + sourceAddr, count, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Float:
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
                    cval = CompressValues<float>(source, count * tlen + sourceAddr, count, tims, type, tlen);
                    timeData = CompressTimers(tims, usedIndex, count);
                    cqus = CompressQualitys(source, count * (tlen + 4) + sourceAddr, count, usedIndex);

                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                default:
                    base.Compress<T>(source, sourceAddr, target, targetAddr, size, type);
                    break;
            }

            System.Buffers.ArrayPool<int>.Shared.Return(tims);
            return rsize;
        }
        
        
        
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...



    }
}
