//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/9/23 8:39:17.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace DBRuntime.His
{
    public class ManualHisDataMemoryBlockPool
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static ManualHisDataMemoryBlockPool Pool = new ManualHisDataMemoryBlockPool();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, Queue<ManualHisDataMemoryBlock>> mFreePools = new Dictionary<int, Queue<ManualHisDataMemoryBlock>>();

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
        public void PreAlloc(int size)
        {
            lock (mFreePools)
            {
                if (mFreePools.ContainsKey(size))
                {
                    var pp = mFreePools[size];
                    var bnb = NewBlock(size);
                    pp.Enqueue(bnb);
                }
                else
                {
                    var bnb = NewBlock(size);
                    Queue<ManualHisDataMemoryBlock> dd = new Queue<ManualHisDataMemoryBlock>();
                    dd.Enqueue(bnb);
                    mFreePools.Add(size,dd);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public ManualHisDataMemoryBlock Get(int size)
        {
            if (mFreePools.ContainsKey(size))
            {
                var pp = mFreePools[size];
                if (pp.Count > 0)
                {
                    lock (mFreePools)
                    {
                        var bnb = pp.Dequeue();
                        return bnb;
                    }
                }
                else
                {
                    Cdy.Tag.LoggerService.Service.Debug("ManualHisMemoryBlockPool", "New datablock 1 size:" + size);
                    var bnb = NewBlock(size);
                    return bnb;
                }
            }
            else
            {
                lock (mFreePools)
                {
                    Cdy.Tag.LoggerService.Service.Debug("ManualHisMemoryBlockPool", "New datablock 2 size:" + size);
                    var bnb = NewBlock(size);
                    if (mFreePools.ContainsKey(size))
                    {
                        return bnb;
                    }
                    else
                    {
                        Queue<ManualHisDataMemoryBlock> dd = new Queue<ManualHisDataMemoryBlock>();
                        mFreePools.Add(size, dd);
                    }
                    return bnb;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        private ManualHisDataMemoryBlock NewBlock(int size)
        {
            var re = new ManualHisDataMemoryBlock(size);
            re.Clear();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>

        public void Release(ManualHisDataMemoryBlock block)
        {
            var size = (int)block.AllocSize;
            if (mFreePools.ContainsKey(size))
            {
                var vv = mFreePools[size];
                lock (mFreePools)
                {
                    vv.Enqueue(block);
                }
                block.Reset();
                block.Clear();
            }
            else
            {
                block.Dispose();
            }
            
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
