//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/6/21 13:05:15 .
//  Version 1.0
//  CDYWORK
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBRuntime.His.Compress
{
    /// <summary>
    /// 
    /// </summary>
    public class DoubleCompressBuffer
    {

        #region ... Variables  ...

        private int index = 0;

        double[] mBuffer;

        private double mMaxValue;

        private int mPrecision;

        private bool mCanCompress = false;

        private double mLastValue;

        private long mPrecisionValue;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public DoubleCompressBuffer(int count)
        {
            mBuffer = new double[count];
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int Precision { get { return mPrecision; } set { mPrecision = value; if (value >= 0) { mPrecisionValue = (long)Math.Pow(10, value); mMaxValue = long.MaxValue / mPrecisionValue; } } }

        /// <summary>
        /// 
        /// </summary>
        public MemoryBlock MemoryBlock { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ProtoMemory VarintMemory  { get; set; }


        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public void CheckAndResizeTo(int count)
        {
            if (count > mBuffer.Length)
            {
                mBuffer = new double[count];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool CanDecompress(double value)
        {
            return value < mMaxValue;
        }


        /// <summary>
        /// 将浮点数转换成整数
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private long TranslateData(double value)
        {
            return (long)(value * mPrecisionValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Append(double value)
        {
            if (index == 0)
            {
                mBuffer[index] = value;
                mLastValue = value;
            }
            else
            {
                var val = Math.Round((value - mLastValue), mPrecision+1);
                if(!CanDecompress(val))
                {
                    mCanCompress = false;
                }
                mBuffer[index] = val;
                mLastValue = value;
            }
            index++;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            index = 0;
            mCanCompress = true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Compress()
        {
            if (mCanCompress && index > 2)
            {
                MemoryBlock.Write((byte)this.Precision);
                MemoryBlock.Write(mBuffer[0]);
                for (int i = 1; i < index; i++)
                {
                    VarintMemory.WriteSInt64(TranslateData(mBuffer[i]));
                }
                MemoryBlock.WriteBytes(9, VarintMemory.DataBuffer,0,(int)VarintMemory.WritePosition);
            }
            else
            {
                MemoryBlock.Write(byte.MaxValue);
                for (int i = 0; i < index; i++)
                {
                    if (i == 0)
                    {
                        mLastValue = mBuffer[i];
                    }
                    else
                    {
                        mLastValue += mBuffer[i];
                    }
                    MemoryBlock.Write(mLastValue);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public static List<double> Decompress(byte[] values)
        {
            var memory = new MemorySpan(values);
            var type = memory.ReadByte();
            List<double> re = new List<double>(300);
            var dval = Math.Pow(10, type);
            if(type == byte.MaxValue)
            {
                while(memory.Position<memory.Length)
                {
                    re.Add(memory.ReadDouble());
                }
            }
            else
            {
                var val = memory.ReadDouble();
                re.Add(val);
                using (ProtoMemory vmemory = new ProtoMemory(values,9))
                {
                    while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                    {
                        val += (vmemory.ReadSInt64() / dval);
                        re.Add(val);
                    }
                }
            }
            return re;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
