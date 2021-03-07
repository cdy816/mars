//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/02/15 19:52:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public unsafe class NumberStatisticsQueryResult
    {

        #region ... Variables  ...

        private IntPtr handle;

        private int mLimite = 0;

        private int mSize = 0;

        private int mCount = 0;

        public const short ValueSize = 48;

        private int mPosition = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public NumberStatisticsQueryResult():this(100)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public NumberStatisticsQueryResult(int valueCount)
        {
            Init(valueCount);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int Count
        {
            get
            {
                return mCount;
            }
            set
            {
                mCount = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int Size
        {
            get
            {
                return mSize;
            }
            set
            {
                mSize = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int Position
        {
            get
            {
                return mPosition;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public IntPtr MemoryHandle
        {
            get
            {
                return handle;
            }
        }



        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        private void Init(int count)
        {
            int csize = count * ValueSize;

            int cc = csize / 1024;
            if (csize % 1024 != 0)
            {
                cc++;
            }
            csize = cc * 1024;
            handle = Marshal.AllocHGlobal(csize);
            mLimite = count;
            mSize = csize;
            mCount = 0;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Resize(int count)
        {
            var newsize = count * ValueSize;
            mLimite = count > mLimite ? count : mLimite;

            if (newsize <= mSize)
            {
                return;
            }

            IntPtr nhd = Marshal.AllocHGlobal(newsize);

            Buffer.MemoryCopy((void*)handle, (void*)nhd, newsize, mSize);
            Marshal.FreeHGlobal(handle);
            handle = nhd;
            mSize = newsize;

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="Hourtime"></param>
        /// <param name="avgValue"></param>
        /// <param name="maxvalue"></param>
        /// <param name="maxvalueTime"></param>
        /// <param name="minvalue"></param>
        /// <param name="minValueTime"></param>
        public void AddValue(DateTime time,double avgValue,double maxvalue,DateTime maxvalueTime,double minvalue,DateTime minValueTime)
        {
            MemoryHelper.WriteDateTime((void*)handle, mPosition, time);
            MemoryHelper.WriteDouble((void*)(handle), mPosition+8, avgValue);
            MemoryHelper.WriteDateTime((void*)(handle), mPosition+16, maxvalueTime);
            MemoryHelper.WriteDouble((void*)(handle), mPosition+24, maxvalue);
            MemoryHelper.WriteDateTime((void*)(handle ), mPosition+ 32, minValueTime);
            MemoryHelper.WriteDouble((void*)(handle ), mPosition+ 40, minvalue);
            mPosition += ValueSize;
            mCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="avgvalue"></param>
        /// <param name="maxvalue"></param>
        /// <param name="maxvalueTime"></param>
        /// <param name="minValue"></param>
        /// <param name="minValueTime"></param>
        /// <returns></returns>
        public bool ReadValue(int id,out DateTime time,out double avgvalue,out double maxvalue,out DateTime maxvalueTime,out double minValue,out DateTime minValueTime)
        {
            if (id < mCount)
            {
                int pos = id * ValueSize;
                time = MemoryHelper.ReadDateTime((void*)handle, pos);
                avgvalue = MemoryHelper.ReadDouble((void*)handle, pos + 8);
                maxvalueTime = MemoryHelper.ReadDateTime((void*)handle, pos + 16);
                maxvalue = MemoryHelper.ReadDouble((void*)handle, pos + 24);
                minValueTime = MemoryHelper.ReadDateTime((void*)handle, pos + 32);
                minValue = MemoryHelper.ReadDouble((void*)handle, pos + 40);
                return true;
            }
            else
            {
                time = maxvalueTime = minValueTime = DateTime.MinValue;
                avgvalue = maxvalue = minValue = double.MinValue;
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<NumberStatisticsValue> ListAllValue()
        {
            DateTime time, maxtime, mintime;
            double avgvalue, maxvalue, minvalue;
            for (int i = 0; i < mCount; i++)
            {
                if (ReadValue(i, out time, out avgvalue, out maxvalue, out maxtime, out minvalue, out mintime))
                {
                    yield return new NumberStatisticsValue() { Time = time, MaxTime = maxtime, MinTime = mintime, MaxValue = maxvalue, MinValue = minvalue, AvgValue = avgvalue };
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            mCount = 0;
            Unsafe.InitBlockUnaligned((void*)handle, 0, (uint)mSize);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        public void CloneTo(NumberStatisticsQueryResult target)
        {
            if (target.mLimite < this.mLimite) target.Resize(this.mLimite);

            Buffer.MemoryCopy((void*)handle, (void*)target.handle, mLimite, this.mSize);
            target.mCount = this.mCount;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            Marshal.FreeHGlobal(handle);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public NumberStatisticsQueryResult ConvertUTCTimeToLocal()
        {
            for (int i = 0; i < mCount;i++)
            {
                int pos = i * ValueSize;
                var time = MemoryHelper.ReadDateTime((void*)handle, pos);
                time = time.ToLocalTime();
                MemoryHelper.WriteDateTime((void*)handle, pos, time);

                pos = pos + 16;
                time = MemoryHelper.ReadDateTime((void*)handle, pos);
                time = time.ToLocalTime();
                MemoryHelper.WriteDateTime((void*)handle, pos, time);

                pos = pos + 16;
                time = MemoryHelper.ReadDateTime((void*)handle, pos);
                time = time.ToLocalTime();
                MemoryHelper.WriteDateTime((void*)handle, pos, time);

            }
            return this;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public struct NumberStatisticsValue
    {
        public DateTime Time { get; set; }
        public double AvgValue { get; set; }
        public DateTime MaxTime { get; set; }
        public double MaxValue { get; set; }
        public DateTime MinTime { get; set; }

        public double MinValue { get; set; }
    }

}
