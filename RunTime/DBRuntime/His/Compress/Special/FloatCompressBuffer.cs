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
    public class FloatCompressBuffer
    {

        #region ... Variables  ...

        private int index = 0;

        float[] mBuffer;

        private float mMaxValue;

        private int mPrecision;

        private bool mCanCompress = false;

        private float mLastValue;

        private long mPrecisionValue;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public FloatCompressBuffer(int count)
        {
            mBuffer = new float[count];
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int Precision { get { return mPrecision; } set { mPrecision = value; if (value > 0) { mPrecisionValue = (long)Math.Pow(10, value); mMaxValue = Int32.MaxValue / mPrecisionValue; } } }

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
            if(count>mBuffer.Length)
            {
                mBuffer = new float[count];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private bool CanDecompress(float value)
        {
            return value < mMaxValue;
        }


        /// <summary>
        /// 将浮点数转换成整数
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        private int TranslateData(float value)
        {
            return (int)(value * mPrecisionValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Append(float value)
        {
            if (index == 0)
            {
                mBuffer[index] = value;
                mLastValue = value;
            }
            else
            {
                var val = (float)Math.Round((value - mLastValue),mPrecision);
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
            if (mCanCompress && index>2)
            {
                MemoryBlock.Write((byte)(this.Precision + 100));
                MemoryBlock.Write(mBuffer[0]);
                int pval = 0;
                for (int i = 1; i < index; i++)
                {
                    var vval = TranslateData(mBuffer[i]);
                    VarintMemory.WriteSInt32(vval - pval);
                    pval = vval;
                    //VarintMemory.WriteSInt32(TranslateData(mBuffer[i]));
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
        public static List<float> Decompress(byte[] values)
        {
            var memory = new MemorySpan(values);
            var type = memory.ReadByte();
            List<float> re = new List<float>(300);
           
            if(type == byte.MaxValue)
            {
                while(memory.Position<memory.Length)
                {
                    re.Add(memory.ReadFloat());
                }
            }
            else
            {
                if (type >= 100)
                {
                    var dval = (float)Math.Pow(10, type - 100);
                    var val = memory.ReadFloat();
                    re.Add(val);
                    int pval = 0;
                    using (ProtoMemory vmemory = new ProtoMemory(values, 9))
                    {
                        while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                        {
                            var vval = vmemory.ReadSInt32();
                            val += ((pval + vval) / dval);
                            re.Add(val);
                            pval += vval;

                            //val += (vmemory.ReadSInt32() / dval);
                            //re.Add(val);
                        }
                    }
                }
                else
                {
                    var dval = (float)Math.Pow(10, type);
                    var val = memory.ReadFloat();
                    re.Add(val);
                    using (ProtoMemory vmemory = new ProtoMemory(values, 9))
                    {
                        while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                        {
                            val += (vmemory.ReadSInt32() / dval);
                            re.Add(val);
                        }
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
