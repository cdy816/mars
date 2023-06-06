﻿//==============================================================
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
    public class IntCompressBuffer
    {

        #region ... Variables  ...

        private int index = 0;

        long[] mBuffer;

        private bool mCanCompress = true;

        private int mLastValue;

        private int mErroCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public IntCompressBuffer(int count)
        {
            mBuffer = new long[count];
        }

        #endregion ...Constructor...

        #region ... Properties ...

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
                mBuffer = new long[count];
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Append(int value)
        {
            if (index == 0)
            {
                mBuffer[index] = value;
                mLastValue = value;
            }
            else
            {
                var val = (long)value - mLastValue;
                if ((val > int.MaxValue) || (val < int.MinValue))
                {
                    mErroCount++;
                    if(mErroCount>=this.mBuffer.Length/2)
                    {
                        mCanCompress = false;
                    }
                }
                mBuffer[index] = val;
                mLastValue = value;
            }
            //mBuffer[index] = value;
            //mMinValue = Math.Min(mMinValue, value);
            index++;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            index = 0;
            mErroCount = 0;
            mCanCompress = true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Compress()
        {
            if (mCanCompress)
            {
                MemoryBlock.Write((byte)0);
                MemoryBlock.Write((int)mBuffer[0]);
                long pval = 0;
                for (int i = 1; i < index; i++)
                {
                    var vval = mBuffer[i];
                    VarintMemory.WriteSInt64(vval - pval);
                    pval = vval;
                }
                MemoryBlock.WriteBytes(5, VarintMemory.DataBuffer, 0, (int)VarintMemory.WritePosition);
            }
            else
            {
                MemoryBlock.Write((byte)1);
                MemoryBlock.Write((int)mBuffer[0]);
                //MemoryBlock.Write(index-1);
                long ltmp = mBuffer[0];
                for (int i=1;i<index;i++)
                {
                    ltmp += mBuffer[i];
                    MemoryBlock.Write((int)ltmp);
                }

                //MemoryBlock.WriteBytes(5, VarintMemory.DataBuffer, 0, (int)VarintMemory.WritePosition);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public static List<int> Decompress(byte[] values)
        {
            List<int> re = new List<int>(300);
            try
            {
                var memory = new MemorySpan(values);
                var typ  = memory.ReadByte();
                long val = memory.ReadInt();
                re.Add((int)val);

                if (typ == 0)
                {
                   
                    long pval = 0;
                    using (ProtoMemory vmemory = new ProtoMemory(values, 5))
                    {
                        while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                        {
                            var vval = vmemory.ReadSInt64();
                            val += ((pval + vval));
                            re.Add((int)val);
                            pval += vval;
                        }
                    }
                }
                else
                {
                    while (memory.Position < memory.Length)
                    {
                        re.Add(memory.ReadInt());
                    }

                    //var count = memory.ReadInt();
                    //for(int i=0;i<count;i++)
                    //{
                    //    re.Add(memory.ReadInt());
                    //}
                    //using (ProtoMemory vmemory = new ProtoMemory(values, 5))
                    //{
                    //    while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                    //    {
                    //        var vval = vmemory.ReadSInt32();
                    //        re.Add((int)vval);
                    //    }
                    //}
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("",$"{ex.Message} {ex.StackTrace}");
            }
            return re;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
