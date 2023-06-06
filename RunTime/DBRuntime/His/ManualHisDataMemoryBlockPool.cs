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
using System.Threading;

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

        private int mCachCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int MaxCachCount { get; set; } = 0;

       

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        private int RoundSize(int size)
        {
            return (size / 2048 + 1) * 2048;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public void PreAlloc(int size)
        {
           int psize = RoundSize(size);
            lock (mFreePools)
            {
                if (mFreePools.ContainsKey(psize))
                {
                    var pp = mFreePools[psize];
                    var bnb = NewBlock(psize);
                    pp.Enqueue(bnb);
                }
                else
                {
                    var bnb = NewBlock(psize);
                    Queue<ManualHisDataMemoryBlock> dd = new Queue<ManualHisDataMemoryBlock>();
                    dd.Enqueue(bnb);
                    mFreePools.Add(psize, dd);
                }
                Interlocked.Increment(ref mCachCount);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public ManualHisDataMemoryBlock Get(int size)
        {
            int psize = RoundSize(size);
            if (mFreePools.ContainsKey(psize))
            {
                lock (mFreePools)
                {
                    var pp = mFreePools[psize];
                    if (pp.Count > 0)
                    {
                        var bnb = pp.Dequeue();
                        Interlocked.Decrement(ref mCachCount);
                        bnb.UsedSize= size;
                        return bnb;
                    }
                    else
                    {
                        //Cdy.Tag.LoggerService.Service.Debug("ManualHisMemoryBlockPool", "New datablock 1 size:" + size);
                        var bnb = NewBlock(psize);
                        bnb.UsedSize = size;
                        return bnb;
                    }
                }
            }
            else
            {
                lock (mFreePools)
                {
                    //Cdy.Tag.LoggerService.Service.Debug("ManualHisMemoryBlockPool", "New datablock 2 size:" + size);
                    var bnb = NewBlock(psize);
                    if (mFreePools.ContainsKey(psize))
                    {
                        return bnb;
                    }
                    else
                    {
                        Queue<ManualHisDataMemoryBlock> dd = new Queue<ManualHisDataMemoryBlock>();
                        mFreePools.Add(psize, dd);
                    }
                    bnb.UsedSize = size;
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
                block.Reset();
                block.Clear();
                lock (mFreePools)
                {
                    if (mCachCount >= MaxCachCount)
                    {
                        if(vv.Count>0)
                        {
                            var dis = vv.Dequeue();
                            dis.Dispose();
                            vv.Enqueue(block);
                        }
                        else
                        {
                            block.Dispose();
                        }
                    }
                    else
                    {
                        vv.Enqueue(block);
                        Interlocked.Increment(ref mCachCount);
                    }
                }
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
