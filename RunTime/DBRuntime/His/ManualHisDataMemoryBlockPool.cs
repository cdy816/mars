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
        private Dictionary<int, Dictionary<ManualHisDataMemoryBlock, bool>> mPools = new Dictionary<int, Dictionary<ManualHisDataMemoryBlock, bool>>();

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
        public ManualHisDataMemoryBlock Get(int size)
        {
            lock (mPools)
            {
                if (mPools.ContainsKey(size))
                {
                    var pp = mPools[size].Where(e => !e.Value);
                    if (pp.Count() > 0)
                    {
                        var bnb = pp.First().Key;
                        mPools[size][bnb] = true;
                        return bnb;
                    }
                    else
                    {
                        var bnb = NewBlock(size);
                        mPools[size].Add(bnb, true);

                        return bnb;
                    }
                }
                else
                {
                    var bnb = NewBlock(size);
                    Dictionary<ManualHisDataMemoryBlock, bool> dd = new Dictionary<ManualHisDataMemoryBlock, bool>();
                    dd.Add(bnb, true);
                    mPools.Add(size, dd);
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
            return new ManualHisDataMemoryBlock(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>

        public void Release(ManualHisDataMemoryBlock block)
        {
            var size = (int)block.AllocSize;
            if(mPools.ContainsKey(size))
            {
                var vv = mPools[size];
                if(vv.ContainsKey(block))
                {
                    vv[block] = false;
                    block.Clear();
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
