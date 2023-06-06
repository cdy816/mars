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
    public class UIntCompressBuffer
    {

        #region ... Variables  ...

        private int index = 0;

        long[] mBuffer;

        private bool mCanCompress = true;

        private uint mLastValue;

        private int mErroCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public UIntCompressBuffer(int count)
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
        public void Append(uint value)
        {
            if (index == 0)
            {
                mBuffer[index] = value;
                mLastValue = value;
            }
            else
            {
                var val = (long)value - mLastValue;
                if ((Math.Abs(val) >= int.MaxValue))
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
                MemoryBlock.Write((uint)mBuffer[0]);
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
                MemoryBlock.Write((uint)mBuffer[0]);
                //MemoryBlock.Write(index-1);
                long ltmp = mBuffer[0];
                for (int i=1;i<index;i++)
                {
                    ltmp += mBuffer[i];
                    MemoryBlock.Write((uint)ltmp);
                }

                //MemoryBlock.WriteBytes(5, VarintMemory.DataBuffer, 0, (int)VarintMemory.WritePosition);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public static List<uint> Decompress(byte[] values)
        {
            List<uint> re = new List<uint>(300);
            try
            {
                var memory = new MemorySpan(values);
                var typ  = memory.ReadByte();
                long val = memory.ReadUInt();
                re.Add((uint)val);

                if (typ == 0)
                {
                   
                    long pval = 0;
                    using (ProtoMemory vmemory = new ProtoMemory(values, 5))
                    {
                        while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                        {
                            var vval = vmemory.ReadSInt64();
                            val += ((pval + vval));
                            re.Add((uint)val);
                            pval += vval;
                        }
                    }
                }
                else
                {
                    while (memory.Position < memory.Length)
                    {
                        re.Add(memory.ReadUInt());
                    }

                    //var count = memory.ReadInt();
                    //for(int i=0;i<count;i++)
                    //{
                    //    re.Add(memory.ReadUInt());
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
                LoggerService.Service.Warn("UIntCompressBuffer", $"Decompress {ex.Message} {ex.StackTrace}");
            }
            return re;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
