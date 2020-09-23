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
    public class MarshalMemoryBlockPool
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static MarshalMemoryBlockPool Pool = new MarshalMemoryBlockPool();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<MarshalMemoryBlock, bool> mPools = new Dictionary<MarshalMemoryBlock, bool>();

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
        public MarshalMemoryBlock Get(long size)
        {
            lock (mPools)
            {
                var mms = mPools.Where(e => !e.Value && e.Key.AllocSize == size);
                if (mms.Count() > 0)
                {
                    var vv = mms.First().Key;
                    mPools[vv] = true;
                    return vv;
                }
                else
                {
                    var bnb = NewBlock(size);
                    mPools.Add(bnb, true);
                    return bnb;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        private MarshalMemoryBlock NewBlock(long size)
        {
            return new MarshalMemoryBlock(size).Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>

        public void Release(MarshalMemoryBlock block)
        {
            if(mPools.ContainsKey(block))
            {
                mPools[block] = false;
            }
            block.Clear();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
