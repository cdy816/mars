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
using System.Reflection.PortableExecutable;
using System.Text;

namespace DBRuntime.His.Compress
{
    /// <summary>
    /// 
    /// </summary>
    public class Int64CompressBuffer
    {

        #region ... Variables  ...

        private int index = 0;

        long[] mBuffer;

        private bool mCanCompress = true;

        private long mLastValue;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public Int64CompressBuffer(int count)
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
        public void Append(long value)
        {
            mBuffer[index] = value;

            if ((mLastValue > 0 && value < 0) || (mLastValue < 0 && value > 0))
            {
                long ltmp = value - mLastValue;
                if (Math.Abs(ltmp/2) < Math.Abs(mLastValue/2) || Math.Abs(ltmp/2) < Math.Abs(value / 2 ))
                {
                    mCanCompress = false;
                }
            }

            mLastValue = value;

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
            if (mCanCompress)
            {
                MemoryBlock.Write((byte)0);
                MemoryBlock.Write((long)mBuffer[0]);
                long pval = 0;
                for (int i = 1; i < index; i++)
                {
                    var vval = mBuffer[i] - mBuffer[i-1];
                    VarintMemory.WriteSInt64(vval - pval);
                    pval = vval;
                }
                MemoryBlock.WriteBytes(9, VarintMemory.DataBuffer, 0, (int)VarintMemory.WritePosition);
            }
            else
            {
                MemoryBlock.Write((byte)1);
                MemoryBlock.Write((long)mBuffer[0]);
                //MemoryBlock.Write(index-1);
                for (int i=1;i<index;i++)
                {
                    MemoryBlock.Write(mBuffer[i]);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public static List<long> Decompress(byte[] values)
        {
            List<long> re = new List<long>(300);
            try
            {
                var memory = new MemorySpan(values);
                var typ  = memory.ReadByte();
                long val = memory.ReadLong();
                re.Add((long)val);

                if (typ == 0)
                {
                   
                    long pval = 0;
                    using (ProtoMemory vmemory = new ProtoMemory(values, 9))
                    {
                        while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                        {
                            var vval = vmemory.ReadSInt64();
                            val += ((pval + vval));
                            re.Add((long)val);
                            pval += vval;
                        }
                    }
                }
                else
                {
                    while (memory.Position < memory.Length)
                    {
                        re.Add(memory.ReadLong());
                    }
                    //var count = memory.ReadInt();
                    //for(int i=0;i<count;i++)
                    //{
                    //    re.Add(memory.ReadLong());
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
                LoggerService.Service.Warn("Int64CompressBuffer", $"Decompress {ex.Message} {ex.StackTrace}");
            }
            return re;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
