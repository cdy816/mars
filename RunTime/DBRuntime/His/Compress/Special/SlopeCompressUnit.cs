//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//  1. 2020/7/1 三沙永兴岛 优化斜率压缩算法  种道洋
//==============================================================
using Google.Protobuf.WellKnownTypes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class SlopeCompressUnit : LosslessCompressUnit
    {

        #region ... Variables  ...
        protected MemoryBlock mAvaiableDatabuffer;

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
            int preids = timerVals[0];
            int ig = -1;
            ig = usedIndex.ReadIndex < usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
            bool isFirst = true;

            mVarintMemory.Reset();
            mVarintMemory.WriteInt32(preids);
            for (int i = 1; i < timerVals.Count; i++)
            {
                if (i == ig)
                {
                    var id = timerVals[i];
                    if (isFirst)
                    {
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


        private void GetEmpityTimers(List<ushort> timerVals)
        {
            emptys.Reset();
           // Queue<int> emptyIds = new Queue<int>();
            int preids = timerVals[0];
            mVarintMemory.Reset();
            for (int i = 1; i < timerVals.Count; i++)
            {
                if (timerVals[i] <= 0)
                {
                    emptys.Insert(i);
                    //emptyIds.Enqueue(i);
                }
            }
            //return emptyIds;
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

            int count = 1;
            byte qus = source.ReadByte(offset);
            //using (VarintCodeMemory memory = new VarintCodeMemory(qulitys.Length * 2))
            mVarintMemory.Reset();
            int ig = -1;
            ig = usedIndex.ReadIndex < usedIndex.WriteIndex ? usedIndex.IncRead() : -1;
            mVarintMemory.WriteInt32(qus);
            for (int i = 1; i < totalcount; i++)
            {
                if (i == ig)
                {
                    byte bval = source.ReadByte(offset + i);
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
            return (Convert.ToDouble(lastVal) - Convert.ToDouble(fval)) / timSpan;
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
        private Memory<byte> SlopeCompressByte(MemoryBlock values,ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            byte mLastValue = 0;

            if(count > 0)
            {
                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadByte();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadByte();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

              
                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        private Memory<byte> SlopeCompressShort(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            short mLastValue = 0;

            if (count > 0)
            {
                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadShort();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadShort();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
           
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        //private Memory<byte> SlopeCompress(Dictionary<ushort, short> values, out Queue<int> mUsedTimerIndex)
        //{
        //    double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
        //    int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
        //    double slope = 0;

        //    mMarshalMemory.Position = 0;
        //    Queue<int> mTimerIndex = new Queue<int>();

        //    if (values.Count > 0)
        //    {
        //        var vals = values.ToArray();
        //        var pval = vals[0].Value;
        //        ushort ptim = vals[0].Key;

        //        mTimerIndex.Enqueue(0);

        //        mMarshalMemory.Write(pval);

        //        for (int i = 1; i < vals.Length; i++)
        //        {
        //            if (i == 1)
        //            {
        //                slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
        //            }
        //            else
        //            {
        //                if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
        //                {
        //                    mTimerIndex.Enqueue(i - 1);
        //                    mMarshalMemory.Write((vals[i - 1].Value));
        //                    slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
        //                }
        //            }
        //        }

        //        if (!mTimerIndex.Contains(vals.Length - 1))
        //        {
        //            int i = vals.Length - 1;
        //            mTimerIndex.Enqueue(i);
        //            mMarshalMemory.Write((vals[i].Value));
        //        }
        //    }
        //    mUsedTimerIndex = mTimerIndex;
        //    return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        //}


        //private Memory<byte> SlopeCompress(Dictionary<ushort, ushort> values, out Queue<int> mUsedTimerIndex)
        //{
        //    double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
        //    int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
        //    double slope = 0;

        //    mMarshalMemory.Position = 0;
        //    Queue<int> mTimerIndex = new Queue<int>();

        //    if (values.Count > 0)
        //    {
        //        var vals = values.ToArray();
        //        var pval = vals[0].Value;
        //        ushort ptim = vals[0].Key;

        //        mTimerIndex.Enqueue(0);

        //        mMarshalMemory.Write(pval);

        //        for (int i = 1; i < vals.Length; i++)
        //        {
        //            if (i == 1)
        //            {
        //                slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
        //            }
        //            else
        //            {
        //                if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
        //                {
        //                    mTimerIndex.Enqueue(i - 1);
        //                    mMarshalMemory.Write((vals[i - 1].Value));
        //                    slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
        //                }
        //            }
        //        }

        //        if (!mTimerIndex.Contains(vals.Length - 1))
        //        {
        //            int i = vals.Length - 1;
        //            mTimerIndex.Enqueue(i);
        //            mMarshalMemory.Write((vals[i].Value));
        //        }
        //    }
        //    mUsedTimerIndex = mTimerIndex;
        //    return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        //}

        private Memory<byte> SlopeCompressUShort(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            ushort mLastValue = 0;

            if (count > 0)
            {
                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadUShort();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadUShort();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
            //mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        //private Memory<byte> SlopeCompress(Dictionary<ushort, int> values, out Queue<int> mUsedTimerIndex)
        //{
        //    double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
        //    int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
        //    double slope = 0;

        //    mMarshalMemory.Position = 0;
        //    Queue<int> mTimerIndex = new Queue<int>();

        //    if (values.Count > 0)
        //    {
        //        var vals = values.ToArray();
        //        var pval = vals[0].Value;
        //        ushort ptim = vals[0].Key;

        //        mTimerIndex.Enqueue(0);

        //        mMarshalMemory.Write(pval);

        //        for (int i = 1; i < vals.Length; i++)
        //        {
        //            if (i == 1)
        //            {
        //                slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
        //            }
        //            else
        //            {
        //                if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
        //                {
        //                    mTimerIndex.Enqueue(i - 1);
        //                    mMarshalMemory.Write((vals[i - 1].Value));
        //                    slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
        //                }
        //            }
        //        }

        //        if (!mTimerIndex.Contains(vals.Length - 1))
        //        {
        //            int i = vals.Length - 1;
        //            mTimerIndex.Enqueue(i);
        //            mMarshalMemory.Write((vals[i].Value));
        //        }
        //    }
        //    mUsedTimerIndex = mTimerIndex;
        //    return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        //}

        private Memory<byte> SlopeCompressInt(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            int mLastValue = 0;

            if (count > 0)
            {
                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadInt();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadInt();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
            //mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        //private Memory<byte> SlopeCompress(Dictionary<ushort, uint> values, out Queue<int> mUsedTimerIndex)
        //{
        //    double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
        //    int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
        //    double slope = 0;

        //    mMarshalMemory.Position = 0;
        //    Queue<int> mTimerIndex = new Queue<int>();

        //    if (values.Count > 0)
        //    {
        //        var vals = values.ToArray();
        //        var pval = vals[0].Value;
        //        ushort ptim = vals[0].Key;

        //        mTimerIndex.Enqueue(0);

        //        mMarshalMemory.Write(pval);

        //        for (int i = 1; i < vals.Length; i++)
        //        {
        //            if (i == 1)
        //            {
        //                slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
        //            }
        //            else
        //            {
        //                if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
        //                {
        //                    mTimerIndex.Enqueue(i - 1);
        //                    mMarshalMemory.Write((vals[i - 1].Value));
        //                    slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
        //                }
        //            }
        //        }

        //        if (!mTimerIndex.Contains(vals.Length - 1))
        //        {
        //            int i = vals.Length - 1;
        //            mTimerIndex.Enqueue(i);
        //            mMarshalMemory.Write((vals[i].Value));
        //        }
        //    }
        //    mUsedTimerIndex = mTimerIndex;
        //    return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        //}

        private Memory<byte> SlopeCompressUInt(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            uint mLastValue = 0;

            if (count > 0)
            {
                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadUInt();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadUInt();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
            //mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        private Memory<byte> SlopeCompress(Dictionary<ushort, long> values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mUsedTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
           // mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }

        private Memory<byte> SlopeCompress(Dictionary<ushort, ulong> values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
          //  Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mUsedTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
           // mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }


        private Memory<byte> SlopeCompressLong(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
           // Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            long mLastValue = 0;

            if (count > 0)
            {
                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadLong();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadLong();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
           // mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompressULong(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            ulong mLastValue = 0;

            if (count > 0)
            {
                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadULong();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadULong();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
           // mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        //private Memory<byte> SlopeCompress(Dictionary<ushort, double> values, out Queue<int> mUsedTimerIndex)
        //{
        //    double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
        //    int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
        //    double slope = 0;

        //    mMarshalMemory.Position = 0;
        //    Queue<int> mTimerIndex = new Queue<int>();

        //    if (values.Count > 0)
        //    {
        //        var vals = values.ToArray();
        //        var pval = vals[0].Value;
        //        ushort ptim = vals[0].Key;

        //        mTimerIndex.Enqueue(0);

        //        mMarshalMemory.Write(pval);

        //        for (int i = 1; i < vals.Length; i++)
        //        {
        //            if (i == 1)
        //            {
        //                slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
        //            }
        //            else
        //            {
        //                if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
        //                {
        //                    mTimerIndex.Enqueue(i - 1);
        //                    mMarshalMemory.Write((vals[i - 1].Value));
        //                    slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
        //                }
        //            }
        //        }

        //        if (!mTimerIndex.Contains(vals.Length - 1))
        //        {
        //            int i = vals.Length - 1;
        //            mTimerIndex.Enqueue(i);
        //            mMarshalMemory.Write((vals[i].Value));
        //        }
        //    }
        //    mUsedTimerIndex = mTimerIndex;
        //    return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        //}

        private Memory<byte> SlopeCompressDouble(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            double mLastValue = 0;

            if (count > 0)
            {
                //var vals = values.ToArray();

                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadDouble();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadDouble();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
            //mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="values"></param>
        ///// <param name="mUsedTimerIndex"></param>
        ///// <returns></returns>
        //private Memory<byte> SlopeCompress(Dictionary<ushort, float> values, out Queue<int> mUsedTimerIndex)
        //{
        //    double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
        //    int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
        //    double slope = 0;

        //    mMarshalMemory.Position = 0;
        //    Queue<int> mTimerIndex = new Queue<int>();

        //    if (values.Count > 0)
        //    {
        //        var vals = values.ToArray();
        //        var pval = vals[0].Value;
        //        ushort ptim = vals[0].Key;

        //        mTimerIndex.Enqueue(0);

        //        mMarshalMemory.Write(pval);

        //        for (int i = 1; i < vals.Length; i++)
        //        {
        //            if (i == 1)
        //            {
        //                slope = CalSlope(pval, vals[i].Value, vals[i].Key - ptim);
        //            }
        //            else
        //            {
        //                if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
        //                {
        //                    mTimerIndex.Enqueue(i - 1);
        //                    mMarshalMemory.Write((vals[i - 1].Value));
        //                    slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
        //                }
        //            }
        //        }

        //        if (!mTimerIndex.Contains(vals.Length - 1))
        //        {
        //            int i = vals.Length - 1;
        //            mTimerIndex.Enqueue(i);
        //            mMarshalMemory.Write((vals[i].Value));
        //        }
        //    }
        //    mUsedTimerIndex = mTimerIndex;
        //    return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        //}


        private Memory<byte> SlopeCompressFloat(MemoryBlock values, ref CustomQueue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            //Queue<int> mTimerIndex = new Queue<int>();

            int count = values.ReadInt(0);

            ushort mLastTime = 0;
            float mLastValue = 0;

            if (count > 0)
            {
                //var vals = values.ToArray();

                ushort mStartTime = values.ReadUShort();
                var mStartValue = values.ReadFloat();

                mLastTime = mStartTime;
                mLastValue = mStartValue;

                mUsedTimerIndex.Insert(0);

                mMarshalMemory.Write(mStartValue);

                for (int i = 1; i < count; i++)
                {
                    var vkey = values.ReadUShort();
                    var vval = values.ReadFloat();
                    if (i == 1)
                    {
                        slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(mStartValue, vval, vkey - mStartTime), slopeArea, slopeType))
                        {
                            mUsedTimerIndex.Insert(i - 1);
                            mMarshalMemory.Write((mLastValue));
                            mStartTime = mLastTime;
                            mStartValue = mLastValue;
                            slope = CalSlope(mStartValue, vval, vkey - mStartTime);
                        }
                    }
                    mLastTime = vkey;
                    mLastValue = vval;
                }

                if (!mUsedTimerIndex.Contains(count))
                {
                    int i = count;
                    mUsedTimerIndex.Insert(i);
                    mMarshalMemory.Write(mLastValue);
                }
            }
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mMarshalMemory.Position); ;
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
        protected  Memory<byte> CompressValues<T>(MarshalMemoryBlock source, long offset, int count, CustomQueue<int> emptyIds,List<ushort> mTimers,TagType type)
        {
            int ig = -1;
            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressByte(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressShort(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressUShort(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressInt(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressUInt(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressLong(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressULong(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressDouble(mAvaiableDatabuffer, ref usedIndex);
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
                            ig = emptys.ReadIndex < emptyIds.WriteIndex ? emptys.IncRead() : -1;
                        }
                    }
                    mAvaiableDatabuffer.WriteInt(0, ac);
                    return SlopeCompressFloat(mAvaiableDatabuffer, ref usedIndex);
                default:
                    usedIndex = null;
                    return null;
            }
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
            //Queue<int> usedIndex;

            long rsize = 0;

            switch (type)
            {
                case TagType.Byte:
                    var cval = CompressValues<byte>(source, count * 2 + sourceAddr, count, emptys, tims, type);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                    var cqus = CompressQulitys(source, count * 3 + sourceAddr, count, usedIndex);
                    var timeData = CompressTimers(tims, usedIndex);

                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;

                    target.Write(cval.Length);
                    target.Write(cval);
                    rsize += 4;
                    rsize += cval.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Short:
                    var res = CompressValues<short>(source, count * 2 + sourceAddr, count, emptys, tims, type);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 4 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);

                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;

                    target.Write(res.Length);
                    target.Write(res);
                    rsize += 4;
                    rsize += res.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UShort:
                    var ures = CompressValues<ushort>(source, count * 2 + sourceAddr, count, emptys, tims, type);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                     cqus = CompressQulitys(source, count * 4 + sourceAddr, count, usedIndex);
                     timeData = CompressTimers(tims, usedIndex);

                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;

                    target.Write(ures.Length);
                    target.Write(ures);
                    rsize += 4;
                    rsize += ures.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Int:
                    var ires = CompressValues<int>(source, count * 2 + sourceAddr, count, emptys, tims, type);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);

                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;

                    target.Write(ires.Length);
                    target.Write(ires);
                    rsize += 4;
                    rsize += ires.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.UInt:
                    var uires = CompressValues<uint>(source, count * 2 + sourceAddr, count, emptys, tims, type);
                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, usedIndex);
                    timeData = CompressTimers(tims, usedIndex);

                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;


                    target.Write(uires.Length);
                    target.Write(uires);
                    rsize += 4;
                    rsize += uires.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Long:
                    var lres = CompressValues<long>(source, count * 2 + sourceAddr, count, emptys, tims, type);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, usedIndex);

                    timeData = CompressTimers(tims, usedIndex);

                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;

                    target.Write(lres.Length);
                    target.Write(lres);
                    rsize += 4;
                    rsize += lres.Length;

                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.ULong:
                    var ulres = CompressValues<ulong>(source, count * 2 + sourceAddr, count, emptys, tims, type);
                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, usedIndex);

                    timeData = CompressTimers(tims, usedIndex);
                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;

                    target.Write(ulres.Length);
                    target.Write(ulres);
                    rsize += 4;
                    rsize += ulres.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Double:
                    var dres = CompressValues<double>(source, count * 2 + sourceAddr, count, emptys, tims, type);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, usedIndex);

                    timeData = CompressTimers(tims, usedIndex);
                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;


                    target.Write(dres.Length);
                    target.Write(dres);
                    rsize += 4;
                    rsize += dres.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
                    break;
                case TagType.Float:
                    var fres = CompressValues<float>(source, count * 2 + sourceAddr, count, emptys, tims, type);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.WriteIndex);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, usedIndex);

                    timeData = CompressTimers(tims, usedIndex);
                    target.Write((int)timeData.Length);
                    target.Write(timeData);
                    rsize += 4;
                    rsize += timeData.Length;


                    target.Write(fres.Length);
                    target.Write(fres);
                    rsize += 4;
                    rsize += fres.Length;


                    target.Write(cqus.Length);
                    target.Write(cqus);
                    rsize += 4;
                    rsize += cqus.Length;
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
