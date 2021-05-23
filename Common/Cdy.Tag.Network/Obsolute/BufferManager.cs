//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/15 8:33:21.
//  Version 1.0
//  种道洋
//==============================================================

using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class BufferManager
    {

        #region ... Variables  ...

        private static PooledByteBufferAllocator pooledByteBufAllocator;

        /// <summary>
        /// 
        /// </summary>
        static BufferManager()
        {
            pooledByteBufAllocator = new PooledByteBufferAllocator(true);
        }

        public static BufferManager Manager = new BufferManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public IByteBuffer Allocate(int size)
        {
           return pooledByteBufAllocator.DirectBuffer(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="preValue"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public IByteBuffer Allocate(byte preValue,int size)
        {
            return Allocate(size + 1).WriteByte(preValue);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
