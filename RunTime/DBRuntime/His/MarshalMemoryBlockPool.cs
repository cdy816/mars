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
using System.Diagnostics;
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
        private Dictionary<long, Queue<MarshalMemoryBlock>> mFreePools = new Dictionary<long, Queue<MarshalMemoryBlock>>();

        private const int PageSize = 1024 * 4;

        public static int MaxSize = 1024;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        private int CalMemoryBlock()
        {
            int count = 0;
            foreach(var vv in mFreePools)
            {
                count += vv.Value.Count;
            }
           return count;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public MarshalMemoryBlock Get(long size)
        {
            lock (mFreePools)
            {
                long lsize = ((size / PageSize) + 1) * PageSize;
                if (mFreePools.ContainsKey(lsize))
                {

                    var pp = mFreePools[lsize];
                    if (pp.Count > 0)
                    {
                        var bnb = pp.Dequeue();
//#if DEBUG
//                        LoggerService.Service.Info("MarshalMemoryBlock", $"从内存池中获取内存块 {lsize} ,内存块总数:{CalMemoryBlock()} 限制内存块个数{MaxSize}");
//#endif
                        return bnb;
                    }
                    else
                    {
                        var bnb = NewBlock(lsize);
//#if DEBUG
//                        LoggerService.Service.Info("MarshalMemoryBlock", $"已有尺寸，没有足够的内存块 {lsize} ,内存块总数:{CalMemoryBlock()} 限制内存块个数{MaxSize} 内存总量(M):{Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0} ");
//#endif
                        return bnb;
                    }
                }
                else
                {
                    var bnb = NewBlock(lsize);
                    Queue<MarshalMemoryBlock> dd = new Queue<MarshalMemoryBlock>();
                    mFreePools.Add(lsize, dd);
//#if DEBUG
//                    LoggerService.Service.Info("MarshalMemoryBlock", $"新分配尺寸，新分配内存块 {lsize} ,内存块总数:{CalMemoryBlock()} 限制内存块个数{MaxSize} 内存总量(M):{Process.GetCurrentProcess().WorkingSet64 / 1024.0 / 1024.0}");
//#endif
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
            return new MarshalMemoryBlock(size,(int)size).Clear() as MarshalMemoryBlock;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="block"></param>

        public void Release(MarshalMemoryBlock block)
        {
            lock (mFreePools)
            {
                try
                {
                    var size = (int)block.AllocSize;
                    if (mFreePools.ContainsKey(size) && mFreePools[size].Count < MaxSize)
                    {
                        var vv = mFreePools[size];
                        vv.Enqueue(block);
                        block.Clear();
//#if DEBUG
//                        LoggerService.Service.Info("MarshalMemoryBlock", $"内存回收 {size} ,缓存池数据量:{mFreePools[size].Count} 限制内存块个数{MaxSize}");
//#endif
                    }
                    else
                    {
                        block.Dispose();
//#if DEBUG
//                        LoggerService.Service.Info("MarshalMemoryBlock", $"内存回收 Disposed ,缓存池数据量:{CalMemoryBlock()} 限制内存块个数{MaxSize}");
//#endif
                    }
                }
                catch
                {

                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
