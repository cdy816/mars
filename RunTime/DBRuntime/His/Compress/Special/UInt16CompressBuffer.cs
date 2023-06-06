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
    public class UInt16CompressBuffer
    {

        #region ... Variables  ...

        private int index = 0;

        int[] mBuffer;

        private bool mCanCompress = true;

        private UInt16 mLastValue;

        private int mErroCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="count"></param>
        public UInt16CompressBuffer(int count)
        {
            mBuffer = new int[count];
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
                mBuffer = new int[count];
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public void Append(ushort value)
        {
            if (index == 0)
            {
                mBuffer[index] = value;
                mLastValue = value;
            }
            else
            {
                var val = (int)value - mLastValue;
                if (Math.Abs(val) >= ushort.MaxValue)
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
                int pval = 0;
                for (int i = 1; i < index; i++)
                {
                    var vval = mBuffer[i];
                    VarintMemory.WriteSInt32(vval - pval);
                    pval = vval;
                }
                MemoryBlock.WriteBytes(3, VarintMemory.DataBuffer, 0, (int)VarintMemory.WritePosition);
            }
            else
            {
                MemoryBlock.Write((byte)1);
                MemoryBlock.Write((short)mBuffer[0]);
                MemoryBlock.Write(index-1);
                long ltmp = mBuffer[0];
                for (int i=1;i<index;i++)
                {
                    ltmp += mBuffer[i];
                    MemoryBlock.Write((short)ltmp);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public static List<ushort> Decompress(byte[] values)
        {
            List<ushort> re = new List<ushort>(300);
            try
            {
                var memory = new MemorySpan(values);
                var typ  = memory.ReadByte();
                int val = memory.ReadUShort();
                re.Add((ushort)val);

                if (typ == 0)
                {
                   
                    int pval = 0;
                    using (ProtoMemory vmemory = new ProtoMemory(values, 3))
                    {
                        while (vmemory.ReadPosition < vmemory.DataBuffer.Length)
                        {
                            var vval = vmemory.ReadSInt32();
                            val += ((pval + vval));
                            re.Add((ushort)val);
                            pval += vval;
                        }
                    }
                }
                else
                {
                    while(memory.Position<memory.Length)
                    {
                        re.Add(memory.ReadUShort());
                    }
                    //var count = memory.ReadInt();
                    //for(int i=0;i<count;i++)
                    //{
                    //    re.Add(memory.ReadUShort());
                    //}
                }
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("Int16CompressBuffer", $"Decompress {ex.Message} {ex.StackTrace}");
            }
            return re;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
