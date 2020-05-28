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
        protected new byte[] CompressTimers(List<ushort> timerVals, Queue<int> usedIndex)
        {
            int preids = timerVals[0];
            int ig = -1;
            usedIndex.TryDequeue(out ig);
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
                    if (usedIndex.Count > 0)
                        usedIndex.TryDequeue(out ig);
                }
            }
            return mVarintMemory.DataBuffer.AsSpan(0, (int)mVarintMemory.WritePosition).ToArray();
        }


        private Queue<int> GetEmpityTimers(List<ushort> timerVals)
        {
            Queue<int> emptyIds = new Queue<int>();
            int preids = timerVals[0];
            mVarintMemory.Reset();
            {
                for (int i = 1; i < timerVals.Count; i++)
                {
                    if (timerVals[i] <= 0)
                    {
                        emptyIds.Enqueue(i);
                    }
                }
                return emptyIds;
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
        protected new Memory<byte> CompressQulitys(MarshalMemoryBlock source, long offset, int totalcount, Queue<int> usedIndex)
        {
            int count = 1;
            byte qus = source.ReadByte(offset);
            //using (VarintCodeMemory memory = new VarintCodeMemory(qulitys.Length * 2))
            mVarintMemory.Reset();
            int ig = -1;
            usedIndex.TryDequeue(out ig);
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
                    if (usedIndex.Count > 0)
                        usedIndex.TryDequeue(out ig);
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
        private Memory<byte> SlopeCompress(Dictionary<ushort,byte> values,out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if(values.Count>0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }


        private Memory<byte> SlopeCompress(Dictionary<ushort, short> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }


        private Memory<byte> SlopeCompress(Dictionary<ushort, ushort> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }

        private Memory<byte> SlopeCompress(Dictionary<ushort, int> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }

        private Memory<byte> SlopeCompress(Dictionary<ushort, uint> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }

        private Memory<byte> SlopeCompress(Dictionary<ushort, long> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }

        private Memory<byte> SlopeCompress(Dictionary<ushort, ulong> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }

        private Memory<byte> SlopeCompress(Dictionary<ushort, double> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="mUsedTimerIndex"></param>
        /// <returns></returns>
        private Memory<byte> SlopeCompress(Dictionary<ushort, float> values, out Queue<int> mUsedTimerIndex)
        {
            double slopeArea = this.Parameters.ContainsKey("SlopeArea") ? this.Parameters["SlopeArea"] : 0;
            int slopeType = (int)(this.Parameters.ContainsKey("SlopeType") ? this.Parameters["SlopeType"] : 0);
            double slope = 0;

            mMarshalMemory.Position = 0;
            Queue<int> mTimerIndex = new Queue<int>();

            if (values.Count > 0)
            {
                var vals = values.ToArray();
                var pval = vals[0].Value;
                ushort ptim = vals[0].Key;

                mTimerIndex.Enqueue(0);

                mMarshalMemory.Write(pval);

                for (int i = 1; i < vals.Length; i++)
                {
                    if (i == 1)
                    {
                        slope = CalSlope(pval, vals[i], vals[i].Key - ptim);
                    }
                    else
                    {
                        if (CheckIsNeedRecord(slope, CalSlope(pval, vals[i].Value, vals[i].Key - ptim), slopeArea, slopeType))
                        {
                            mTimerIndex.Enqueue(i - 1);
                            mMarshalMemory.Write((vals[i - 1].Value));
                            slope = CalSlope(vals[i - 1].Value, vals[i].Value, vals[i].Key - ptim);
                        }
                    }
                }

                if (!mTimerIndex.Contains(vals.Length - 1))
                {
                    int i = vals.Length - 1;
                    mTimerIndex.Enqueue(i);
                    mMarshalMemory.Write((vals[i].Value));
                }
            }
            mUsedTimerIndex = mTimerIndex;
            return mMarshalMemory.StartMemory.AsMemory(0, (int)mVarintMemory.WritePosition); ;
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
        /// <param name="usedTimerIndex"></param>
        /// <returns></returns>
        protected  Memory<byte> CompressValues<T>(MarshalMemoryBlock source, long offset, int count, Queue<int> emptyIds,List<ushort> mTimers,out Queue<int> usedTimerIndex)
        {
            int ig = -1;
            emptyIds.TryDequeue(out ig);

            if (typeof(T) == typeof(byte))
            {
                Dictionary<ushort, byte> mavaibleValues = new Dictionary<ushort, byte>();

                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadByte(offset + i);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues,out usedTimerIndex);
            }
            else if (typeof(T) == typeof(short))
            {
                Dictionary<ushort, short> mavaibleValues = new Dictionary<ushort, short>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadShort(offset + i);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            else if (typeof(T) == typeof(ushort))
            {
                Dictionary<ushort, ushort> mavaibleValues = new Dictionary<ushort, ushort>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadUShort(offset + i);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            else if (typeof(T) == typeof(int))
            {
                Dictionary<ushort, int> mavaibleValues = new Dictionary<ushort, int>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadInt(offset + i);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            else if (typeof(T) == typeof(uint))
            {
                Dictionary<ushort, uint> mavaibleValues = new Dictionary<ushort, uint>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadUInt(offset + i);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            else if (typeof(T) == typeof(long))
            {
                Dictionary<ushort, long> mavaibleValues = new Dictionary<ushort, long>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadLong(offset + i * 8);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            else if (typeof(T) == typeof(ulong))
            {
                Dictionary<ushort, ulong> mavaibleValues = new Dictionary<ushort, ulong>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadULong(offset + i * 8);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            else if (typeof(T) == typeof(double))
            {
                Dictionary<ushort, double> mavaibleValues = new Dictionary<ushort, double>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadDouble(offset + i * 8);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            else if (typeof(T) == typeof(float))
            {
                Dictionary<ushort, float> mavaibleValues = new Dictionary<ushort, float>();
                for (int i = 0; i < count; i++)
                {
                    if (i != ig)
                    {
                        var id = source.ReadFloat(offset + i * 4);
                        mavaibleValues.Add(mTimers[i], id);
                    }
                    else
                    {
                        if (emptyIds.Count > 0)
                            emptyIds.TryDequeue(out ig);
                    }
                }
                return SlopeCompress(mavaibleValues, out usedTimerIndex);
            }
            usedTimerIndex = null;
            return null;

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

            Queue<int> emptys = GetEmpityTimers(tims);
            Queue<int> usedIndex;

            long rsize = 0;

            switch (TagType)
            {
                case TagType.Byte:
                    var cval = CompressValues<byte>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                    var cqus = CompressQulitys(source, count * 3 + sourceAddr, count, new Queue<int>(usedIndex));
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
                    var res = CompressValues<short>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 4 + sourceAddr, count, new Queue<int>(usedIndex));
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
                    var ures = CompressValues<ushort>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                     cqus = CompressQulitys(source, count * 4 + sourceAddr, count, new Queue<int>(usedIndex));
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
                    var ires = CompressValues<int>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, new Queue<int>(usedIndex));
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
                    var uires = CompressValues<uint>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);
                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, new Queue<int>(usedIndex));
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
                    var lres = CompressValues<long>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;
                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, new Queue<int>(usedIndex));

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
                    var ulres = CompressValues<ulong>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);
                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, new Queue<int>(usedIndex));

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
                    var dres = CompressValues<double>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 10 + sourceAddr, count, new Queue<int>(usedIndex));

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
                    var fres = CompressValues<float>(source, count * 2 + sourceAddr, count, emptys, tims, out usedIndex);

                    target.WriteUShort(targetAddr, (ushort)usedIndex.Count);
                    rsize += 2;

                    cqus = CompressQulitys(source, count * 6 + sourceAddr, count, new Queue<int>(usedIndex));

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
