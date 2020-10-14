//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/9/23 8:39:17.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBRuntime.His
{
    public class MemoryBlockPool
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static MemoryBlockPool Pool = new MemoryBlockPool();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<long, Queue<MemoryBlock>> mFreePools = new Dictionary<long, Queue<MemoryBlock>>();

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
        public MemoryBlock Get(long size)
        {
            lock (mFreePools)
            {
                if (mFreePools.ContainsKey(size))
                {
                    var pp = mFreePools[size];
                    if (pp.Count > 0)
                    {
                        var bnb = pp.Dequeue();
                        return bnb;
                    }
                    else
                    {
                        var bnb = NewBlock(size);
                        return bnb;
                    }
                }
                else
                {
                    var bnb = NewBlock(size);
                    Queue<MemoryBlock> dd = new Queue<MemoryBlock>();
                    mFreePools.Add(size, dd);
                    return bnb;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        private MemoryBlock NewBlock(long size)
        {
            return new MemoryBlock(size,(int)size).Clear() as MemoryBlock;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>

        public void Release(MemoryBlock block)
        {
            lock (mFreePools)
            {
                var size = (int)block.AllocSize;
                if (mFreePools.ContainsKey(size))
                {
                    var vv = mFreePools[size];
                    vv.Enqueue(block);
                    block.Clear();
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
