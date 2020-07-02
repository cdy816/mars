//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//  1. 2020/7/1 三沙永兴岛 优化斜率压缩算法  种道洋
//==============================================================
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
    public class SlopeCompressUnit : LosslessCompressUnit
    {

        #region ... Variables  ...
        protected MemoryBlock mAvaiableDatabuffer;

        /// <summary>
        /// 
        /// </summary>
        protected CustomQueue<int> usedIndex = new CustomQueue<int>(604);

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
        public override CompressUnitbase Clone()
        {
            return new SlopeCompressUnit();
        }
               

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        /// <param name="usedIndex"></param>
        /// <returns></returns>
        protected new byte[] CompressTimers(List<ushort> timerVals, CustomQueue<int> usedIndex)
        {
            usedIndex.ReadIndex = 0;
            // int preids = timerVals[0];
            int preids = 0;
            int ig = usedIndex.ReadIndex < usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
            bool isFirst = true;

            mVarintMemory.Reset();
            //mVarintMemory.WriteInt32(preids);
            for (int i = 0; i < timerVals.Count; i++)
            {
                if (i == ig)
                {
                    var id = timerVals[i];
                    if (isFirst)
                    {
                        isFirst = false;
                        mVarintMemory.WriteInt32(id);
                        preids = id;
                    }
                    else
                    {
                        mVarintMemory.WriteInt32(id - preids);
                        preids = id;
                    }
                    ig = usedIndex.ReadIndex < usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
                }
            }
            return mVarintMemory.DataBuffer.AsSpan(0, (int)mVarintMemory.WritePosition).ToArray();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timerVals"></param>
        private void GetEmpityTimers(List<ushort> timerVals)
        {
            emptys.Reset();
            int preids = timerVals[0];
            mVarintMemory.Reset();
            for (int i = 1; i < timerVals.Count; i++)
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
        protected new  Memory<byte> CompressQulitys(MarshalMemoryBlock source, long offset, int totalcount, CustomQueue<int> usedIndex)
        {
            usedIndex.ReadIndex = 0;

            int count = 0;
            byte qus = 0;
            mVarintMemory.Reset();
            int ig = usedIndex.ReadIndex < usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
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
                    ig = usedIndex.ReadIndex < usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
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
        private double CalSlope(object fval, object lastVal,int timSpan)
        {
            //转换成以秒为时间单位的斜率计算
            //timesSpan是以100ms为单位的时间间隔

            return (Convert.ToDouble(lastVal) - Convert.ToDouble(fval)) / timSpan * 10;
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
        private Memory<byte> SlopeCompressByte(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            mMarshalMemory.Position = 0;
            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = -1;
            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            byte mLastValue = 0;

            ushort mStartTime = 0;
            byte mStartValue = 0;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;
                           
                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadByte();
                            mMarshalMemory.Write(mStartValue);
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadByte();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);
                                    mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }
                                
                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if(maxslope!=minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                mMarshalMemory.Write((mLastValue));
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressShort(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            mVarintMemory.Reset();

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            short mLastValue = 0;

            ushort mStartTime = 0;
            short mStartValue = 0;

            bool isFirst = true;

            short mPrevalue = 0;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadShort();

                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(mStartValue);
                                mPrevalue = mStartValue;
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(mStartValue - mPrevalue);
                                mPrevalue = mStartValue;
                            }
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadShort();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    if (isFirst)
                                    {
                                        mVarintMemory.WriteInt32(mLastValue);
                                        mPrevalue = mLastValue;
                                        isFirst = false;
                                    }
                                    else
                                    {
                                        mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                                        mPrevalue = mLastValue;
                                    }

                                   // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                if (isFirst)
                                {
                                    mVarintMemory.WriteInt32(mLastValue);
                                    mPrevalue = mLastValue;
                                    isFirst = false;
                                }
                                else
                                {
                                    mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                                    mPrevalue = mLastValue;
                                }
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt32(mLastValue);
                        mPrevalue = mLastValue;
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                        mPrevalue = mLastValue;
                    }
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressUShort(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            mVarintMemory.Reset();

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            ushort mLastValue = 0;

            ushort mStartTime = 0;
            ushort mStartValue = 0;

            bool isFirst = true;

            ushort mPrevalue = 0;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadUShort();

                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(mStartValue);
                                mPrevalue = mStartValue;
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(mStartValue - mPrevalue);
                                mPrevalue = mStartValue;
                            }
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadUShort();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    if (isFirst)
                                    {
                                        mVarintMemory.WriteInt32(mLastValue);
                                        mPrevalue = mLastValue;
                                        isFirst = false;
                                    }
                                    else
                                    {
                                        mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                                        mPrevalue = mLastValue;
                                    }

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                if (isFirst)
                                {
                                    mVarintMemory.WriteInt32(mLastValue);
                                    mPrevalue = mLastValue;
                                    isFirst = false;
                                }
                                else
                                {
                                    mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                                    mPrevalue = mLastValue;
                                }
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt32(mLastValue);
                        mPrevalue = mLastValue;
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                        mPrevalue = mLastValue;
                    }
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressInt(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            mVarintMemory.Reset();

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            int mLastValue = 0;

            ushort mStartTime = 0;
            int mStartValue = 0;

            bool isFirst = true;

            int mPrevalue = 0;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadInt();

                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(mStartValue);
                                mPrevalue = mStartValue;
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32(mStartValue - mPrevalue);
                                mPrevalue = mStartValue;
                            }
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadInt();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    if (isFirst)
                                    {
                                        mVarintMemory.WriteInt32(mLastValue);
                                        mPrevalue = mLastValue;
                                        isFirst = false;
                                    }
                                    else
                                    {
                                        mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                                        mPrevalue = mLastValue;
                                    }

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                if (isFirst)
                                {
                                    mVarintMemory.WriteInt32(mLastValue);
                                    mPrevalue = mLastValue;
                                    isFirst = false;
                                }
                                else
                                {
                                    mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                                    mPrevalue = mLastValue;
                                }
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt32(mLastValue);
                        mPrevalue = mLastValue;
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteSInt32(mLastValue - mPrevalue);
                        mPrevalue = mLastValue;
                    }
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressUInt(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            mVarintMemory.Reset();

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            uint mLastValue = 0;

            ushort mStartTime = 0;
            uint mStartValue = 0;

            bool isFirst = true;

            uint mPrevalue = 0;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadUInt();

                            if (isFirst)
                            {
                                mVarintMemory.WriteInt32(mStartValue);
                                mPrevalue = mStartValue;
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt32((int)(mStartValue - mPrevalue));
                                mPrevalue = mStartValue;
                            }
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadUInt();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    if (isFirst)
                                    {
                                        mVarintMemory.WriteInt32(mLastValue);
                                        mPrevalue = mLastValue;
                                        isFirst = false;
                                    }
                                    else
                                    {
                                        mVarintMemory.WriteSInt32((int)(mLastValue - mPrevalue));
                                        mPrevalue = mLastValue;
                                    }

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                if (isFirst)
                                {
                                    mVarintMemory.WriteInt32(mLastValue);
                                    mPrevalue = mLastValue;
                                    isFirst = false;
                                }
                                else
                                {
                                    mVarintMemory.WriteSInt32((int)(mLastValue - mPrevalue));
                                    mPrevalue = mLastValue;
                                }
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt32(mLastValue);
                        mPrevalue = mLastValue;
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteSInt32((int)(mLastValue - mPrevalue));
                        mPrevalue = mLastValue;
                    }
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position);
        }

        private Memory<byte> SlopeCompressLong(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            mVarintMemory.Reset();

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            long mLastValue = 0;

            ushort mStartTime = 0;
            long mStartValue = 0;

            bool isFirst = true;

            long mPrevalue = 0;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadLong();

                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(mStartValue);
                                mPrevalue = mStartValue;
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((mStartValue - mPrevalue));
                                mPrevalue = mStartValue;
                            }
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadLong();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    if (isFirst)
                                    {
                                        mVarintMemory.WriteInt64(mLastValue);
                                        mPrevalue = mLastValue;
                                        isFirst = false;
                                    }
                                    else
                                    {
                                        mVarintMemory.WriteSInt64((mLastValue - mPrevalue));
                                        mPrevalue = mLastValue;
                                    }

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                if (isFirst)
                                {
                                    mVarintMemory.WriteInt64(mLastValue);
                                    mPrevalue = mLastValue;
                                    isFirst = false;
                                }
                                else
                                {
                                    mVarintMemory.WriteSInt64((mLastValue - mPrevalue));
                                    mPrevalue = mLastValue;
                                }
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt64(mLastValue);
                        mPrevalue = mLastValue;
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteSInt64((mLastValue - mPrevalue));
                        mPrevalue = mLastValue;
                    }
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position);
        }


        private Memory<byte> SlopeCompressULong(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            mVarintMemory.Reset();

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            ulong mLastValue = 0;

            ushort mStartTime = 0;
            ulong mStartValue = 0;

            bool isFirst = true;

            ulong mPrevalue = 0;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadULong();

                            if (isFirst)
                            {
                                mVarintMemory.WriteInt64(mStartValue);
                                mPrevalue = mStartValue;
                                isFirst = false;
                            }
                            else
                            {
                                mVarintMemory.WriteSInt64((long)(mStartValue - mPrevalue));
                                mPrevalue = mStartValue;
                            }
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadULong();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    if (isFirst)
                                    {
                                        mVarintMemory.WriteInt64(mLastValue);
                                        mPrevalue = mLastValue;
                                        isFirst = false;
                                    }
                                    else
                                    {
                                        mVarintMemory.WriteSInt64((long)(mLastValue - mPrevalue));
                                        mPrevalue = mLastValue;
                                    }

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                if (isFirst)
                                {
                                    mVarintMemory.WriteInt64(mLastValue);
                                    mPrevalue = mLastValue;
                                    isFirst = false;
                                }
                                else
                                {
                                    mVarintMemory.WriteSInt64((long)(mLastValue - mPrevalue));
                                    mPrevalue = mLastValue;
                                }
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    if (isFirst)
                    {
                        mVarintMemory.WriteInt64(mLastValue);
                        mPrevalue = mLastValue;
                        isFirst = false;
                    }
                    else
                    {
                        mVarintMemory.WriteSInt64((long)(mLastValue - mPrevalue));
                        mPrevalue = mLastValue;
                    }
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="emptyIds"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressDouble(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {

            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            double mLastValue = 0;

            ushort mStartTime = 0;
            double mStartValue = 0;

            mVarintMemory.Reset();
            mMarshalMemory.Position = 0;
            mDCompress.Reset();
            mDCompress.Precision = this.Precision;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadDouble();

                            mDCompress.Append(mLastValue);
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadDouble();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    mDCompress.Append(mLastValue);

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                mDCompress.Append(mLastValue);
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
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
        private Memory<byte> SlopeCompressFloat(MemoryBlock values, CustomQueue<int> emptyIds, CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double minslope = 0;
            double maxslope = 0;

            bool isStarted = false;

            int count = values.ReadInt(0);

            emptys.ReadIndex = 0;
            int ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;

            ushort mLastTime = 0;
            float mLastValue = 0;

            ushort mStartTime = 0;
            float mStartValue = 0;

            mVarintMemory.Reset();
            mMarshalMemory.Position = 0;
            mFCompress.Reset();
            mFCompress.Precision = this.Precision;

            if (count > 0)
            {
                for (int i = 1; i < count; i++)
                {
                    if (ig != i)
                    {
                        if (!isStarted)
                        {
                            isStarted = true;

                            mLastTime = mStartTime = values.ReadUShort();
                            mLastValue = mStartValue = values.ReadFloat();

                            mFCompress.Append(mLastValue);
                            mUsedTimerIndex.Insert(i);
                            minslope = double.MinValue;
                        }
                        else
                        {
                            var vkey = values.ReadUShort();
                            var vval = values.ReadFloat();
                            if (minslope == double.MinValue)
                            {
                                maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                            }
                            else
                            {
                                var ds = CalSlope(mStartValue, vval, vkey - mStartTime);

                                minslope = Math.Min(ds, minslope);
                                maxslope = Math.Max(ds, maxslope);

                                if (CheckIsNeedRecord(minslope, maxslope, slopeArea, slopeType))
                                {
                                    mUsedTimerIndex.Insert(i - 1);

                                    mFCompress.Append(mLastValue);

                                    // mMarshalMemory.Write((mLastValue));
                                    mStartTime = mLastTime;
                                    mStartValue = mLastValue;
                                    maxslope = minslope = CalSlope(mStartValue, vval, vkey - mStartTime);
                                }

                            }
                            mLastTime = vkey;
                            mLastValue = vval;
                        }
                    }
                    else
                    {
                        if (isStarted)
                        {
                            isStarted = false;
                            if (maxslope != minslope)
                            {
                                mUsedTimerIndex.Insert(i - 1);
                                mFCompress.Append(mLastValue);
                            }
                        }
                    }
                }


                if (!mUsedTimerIndex.Contains(count) && isStarted)
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
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
        protected Memory<byte> CompressValues<T>(MarshalMemoryBlock source, long offset, int count,List<ushort> mTimers,TagType type)
        {
            int ig = -1;
            emptys.ReadIndex = 0;

            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
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
                            var id = source.ReadByte(offset + i);

                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                   // return null;
                return SlopeCompressByte(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.Short:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadShort(offset + i);
                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                return SlopeCompressShort(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.UShort:
                    Dictionary<ushort, ushort> mavaibleValues = new Dictionary<ushort, ushort>();
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUShort(offset + i);
                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);

                    return SlopeCompressUShort(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.Int:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadInt(offset + i);
                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                //    return null;
                return SlopeCompressInt(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.UInt:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadUInt(offset + i);
                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                return SlopeCompressUInt(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.Long:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadLong(offset + i * 8);
                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                 return SlopeCompressLong(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.ULong:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadULong(offset + i * 8);
                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    //return null;
                 return SlopeCompressULong(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.Double:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            ac++;
                            var id = source.ReadDouble(offset + i * 8);
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                            //mavaibleValues.Add(mTimers[i], id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressDouble(mAvaiableDatabuffer, emptys, usedIndex);
                case TagType.Float:
                    for (int i = 0; i < count; i++)
                    {
                        if (i != ig)
                        {
                            var id = source.ReadFloat(offset + i * 4);
                            ac++;
                            mAvaiableDatabuffer.Write(mTimers[i]);
                            mAvaiableDatabuffer.Write(id);
                        }
                        else
                        {
                            ig = emptys.ReadIndex < emptys.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    //return null;
                return SlopeCompressFloat(mAvaiableDatabuffer, emptys, usedIndex);
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
        private long FillData(Memory<byte> valuedata,Memory<byte> qualitydata,byte[] timedata,MarshalMemoryBlock target, long targetAddr)
        {
            int rsize = 0;

            target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
            rsize += 2;
            target.Write((int)timedata.Length);
            target.Write(timedata);
            rsize += 4;
            rsize += timedata.Length;

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
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <param name="sourceAddr"></param>
        /// <param name="target"></param>
        /// <param name="targetAddr"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        protected override  long Compress<T>(MarshalMemoryBlock source, long sourceAddr, MarshalMemoryBlock target, long targetAddr, long size, TagType type)
        {
            var count = (int)(size - this.QulityOffset);
            var tims = source.ReadUShorts(sourceAddr, (int)count);

            if (mMarshalMemory == null)
            {
                mMarshalMemory = new MemoryBlock(count * 10);
            }

            if (mVarintMemory == null)
            {
                mVarintMemory = new ProtoMemory(count * 10);
            }

            //
            if (mAvaiableDatabuffer == null)
            {
                mAvaiableDatabuffer = new MemoryBlock(count * 12);
            }

            usedIndex.Reset();
            GetEmpityTimers(tims);

            long rsize = 0;

            switch (type)
            {
                case TagType.Byte:
                    var cval = CompressValues<byte>(source, count * 2 + sourceAddr, count, tims, type);
                    var cqus = CompressQulitys(source, count * 3 + sourceAddr, count, usedIndex);
                    var timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Short:
                    cval = CompressValues<short>(source, count * 2 + sourceAddr, count, tims, type);

                    cqus = CompressQulitys(source, count * 4 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.UShort:
                    cval = CompressValues<ushort>(source, count * 2 + sourceAddr, count,  tims, type);
                     cqus = CompressQulitys(source, count * 4 + sourceAddr, count, usedIndex);
                     timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Int:
                    cval = CompressValues<int>(source, count * 2 + sourceAddr, count,  tims, type);
                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.UInt:
                    cval = CompressValues<uint>(source, count * 2 + sourceAddr, count,  tims, type);
                    
                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);

                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Long:
                    cval = CompressValues<long>(source, count * 2 + sourceAddr, count,  tims, type);
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.ULong:
                    cval = CompressValues<ulong>(source, count * 2 + sourceAddr, count,  tims, type);
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, usedIndex);

                    timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Double:

                    if (mDCompress == null) mDCompress = new DoubleCompressBuffer(310) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };

                    cval = CompressValues<double>(source, count * 2 + sourceAddr, count,  tims, type);
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, usedIndex);

                    timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                case TagType.Float:
                    if (mFCompress == null) mFCompress = new FloatCompressBuffer(310) { MemoryBlock = mMarshalMemory, VarintMemory = mVarintMemory };
                    cval = CompressValues<float>(source, count * 2 + sourceAddr, count,  tims, type);
                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);
                    rsize = FillData(cval, cqus, timeData, target, targetAddr);
                    break;
                default:
                    base.Compress<T>(source, sourceAddr, target, targetAddr, size, type);
                    break;
            }
            return rsize;
        }
        
        
        
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...



    }
}
