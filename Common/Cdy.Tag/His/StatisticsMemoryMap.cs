//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by chongdaoyang at 2021/2/7 14:34:19.
//  Version 1.0
//  CHONGDAOYANGPC
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class StatisticsMemoryMap:IDisposable
    {

        #region ... Variables  ...

        private MarshalMemoryBlock mHead;

        private MarshalMemoryBlock mData;

        private int mAvaiableDataLen = 48;

        public const int TagTotalCount = 100000;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public StatisticsMemoryMap()
        {
            mHead = new MarshalMemoryBlock(TagTotalCount * 8, TagTotalCount * 8);
            mData = new MarshalMemoryBlock(TagTotalCount * 48 * 24, TagTotalCount * 48 * 24);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int StartId { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...



        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        public void Load(System.IO.Stream stream)
        {
            mHead.Clear();
            mData.Clear();
            mAvaiableDataLen = 48;
            if (stream.Length < TagTotalCount * 8)
            {
                return;
            }

            byte[] bvals = System.Buffers.ArrayPool<byte>.Shared.Rent(4);

            //var br = new System.IO.BinaryReader(stream);
            stream.Read(bvals, 0, 4);
            var datasize = BitConverter.ToInt32(bvals, 0);
            mHead.ReadFromStream(stream, datasize);
            stream.Read(bvals, 0, 4);
            datasize = BitConverter.ToInt32(bvals, 0);

            mData.ReadFromStream(stream, datasize);
            mAvaiableDataLen = datasize;

            System.Buffers.ArrayPool<byte>.Shared.Return(bvals);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public  MarshalFixedMemoryBlock GetStatisticsData(int id)
        {
            MarshalFixedMemoryBlock re = null;
            if (id>=StartId && id<StartId+TagTotalCount)
            {
                var vid = mHead.ReadLong((id - StartId) * 8);
                if(vid==0)
                {
                    re = new MarshalFixedMemoryBlock((mData.Buffers[0]+mAvaiableDataLen), 48);
                    mHead.WriteLong((id - StartId) * 8, mAvaiableDataLen);
                    mAvaiableDataLen += 48;
                }
                else
                {
                    re = new MarshalFixedMemoryBlock((mData.Buffers[0] + (int)vid), 48);
                }
            }         
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="block"></param>
        public void GetStatisticsData(int id, MarshalFixedMemoryBlock block)
        {
            if (id >= StartId && id < StartId + TagTotalCount)
            {
                var vid = mHead.ReadLong((id - StartId) * 8);
                if (vid == 0)
                {
                    block.Reset((mData.Buffers[0] + mAvaiableDataLen), 48);
                    mHead.WriteLong((id - StartId) * 8, mAvaiableDataLen);
                    mAvaiableDataLen += 48;
                }
                else
                {
                    block.Reset((mData.Buffers[0] + (int)vid), 48);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Save(System.IO.Stream stream)
        {
            stream.Write(BitConverter.GetBytes((int)mHead.AllocSize));
            mHead.WriteToStream(stream,0,mHead.AllocSize);

            stream.Write(BitConverter.GetBytes(mAvaiableDataLen));
            mData.WriteToStream(stream, 0, mAvaiableDataLen);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            if (mHead != null)
                mHead.Dispose();
            if (mData != null)
                mData.Dispose();
        }



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
