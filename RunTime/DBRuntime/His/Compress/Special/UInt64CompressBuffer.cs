//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/6/21 13:05:15 .
//  Version 1.0
//  CDYWORK
//==============================================================

using Cdy.Tag;
using Google.Protobuf.WellKnownTypes;
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
    public class UInt64CompressBuffer
    {

        #region ... Variables  ...

        private int index = 0;

        ulong[] mBuffer;

        private bool mCanCompress = true;

        private ulong mLastValue;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public UInt64CompressBuffer(int count)
        {
            mBuffer = new ulong[count];
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
                mBuffer = new ulong[count];
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Append(ulong value)
        {
            mBuffer[index] = value;
            if(Math.Abs((long)(value/4 - mLastValue/4))>=long.MaxValue/2)
            {
                if(index>0)
                mCanCompress= false;
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
            mLastValue = 0;
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
                MemoryBlock.Write((ulong)mBuffer[0]);
                long pval = 0;
                for (int i = 1; i < index; i++)
                {
                    long vval = (long)(mBuffer[i] - mBuffer[i - 1]);
                    VarintMemory.WriteSInt64(vval - pval);
                    pval = vval;
                }
                MemoryBlock.WriteBytes(9, VarintMemory.DataBuffer, 0, (int)VarintMemory.WritePosition);
            }
            else
            {
                MemoryBlock.Write((byte)1);
                MemoryBlock.Write((ulong)mBuffer[0]);
                //MemoryBlock.Write(index - 1);
                for (int i = 1; i < index; i++)
                {
                    MemoryBlock.Write(mBuffer[i]);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public static List<ulong> Decompress(byte[] values)
        {
            List<ulong> re = new List<ulong>(300);
            try
            {
                var memory = new MemorySpan(values);
                var typ  = memory.ReadByte();
                long val = memory.ReadLong();
                re.Add((ulong)val);
                if (typ == 0)
                {
                    long pval = 0;
                    using (ProtoMemory vmemory = new ProtoMemory(values, 9))
                    {
                        while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                        {
                            var vval = vmemory.ReadSInt64();
                            val += ((pval + vval));
                            re.Add((ulong)val);
                            pval += vval;
                        }
                    }
                }
                else
                {
                    while (memory.Position < memory.Length)
                    {
                        re.Add(memory.ReadULong());
                    }
                    //var count = memory.ReadInt();
                    //for (int i = 0; i < count; i++)
                    //{
                    //    re.Add(memory.ReadULong());
                    //}
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("UInt64CompressBuffer", $" Decompress {ex.Message} {ex.StackTrace}");
            }
            return re;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
