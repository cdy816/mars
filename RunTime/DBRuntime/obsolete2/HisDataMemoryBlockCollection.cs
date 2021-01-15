//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/8 15:17:34.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;

namespace DBRuntime.His
{
    /// <summary>
    /// 
    /// </summary>
    [Obsolete]
    public class HisDataMemoryBlockCollection : IDisposable
    {

        #region ... Variables  ...

        private Dictionary<int, HisDataMemoryBlock> mTagAddress = new Dictionary<int, HisDataMemoryBlock>();

        private int mRefCount = 0;

        private object mUserSizeLock = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public byte Id { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 变量内存地址缓存
        /// Tuple 每项的含义：起始地址,值地址偏移,质量地址偏移,数据大小
        /// </summary>
        public Dictionary<int, HisDataMemoryBlock> TagAddress
        {
            get
            {
                return mTagAddress;
            }
            set
            {
                if (mTagAddress != value)
                {
                    mTagAddress = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public DateTime CurrentDatetime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndDateTime { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void IncRef()
        {
            lock (mUserSizeLock)
                mRefCount++;
        }

        /// <summary>
        /// 
        /// </summary>
        public void DecRef()
        {
            lock (mUserSizeLock)
                mRefCount = mRefCount > 0 ? mRefCount - 1 : mRefCount;
        }

        /// <summary>
        /// 是否繁忙
        /// </summary>
        /// <returns></returns>
        public bool IsBusy()
        {
            return mRefCount > 0;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name=""></param>
        public void AddTagAddress(int id,HisDataMemoryBlock block)
        {
            if(!mTagAddress.ContainsKey(id))
            {
                mTagAddress.Add(id, block);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void RemoveTagAdress(int id)
        {
            if(mTagAddress.ContainsKey(id))
            {
                mTagAddress.Remove(id);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            if (TagAddress == null) return;
            foreach(var vv in TagAddress)
            {
                vv.Value?.Clear();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            while (this.IsBusy()) Thread.Sleep(1);
            foreach (var vv in mTagAddress)
                vv.Value?.Dispose();
            mTagAddress.Clear();
            mTagAddress = null;
           
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public static class MergeMemoryBlock2Extends
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <param name="stream"></param>
        public static void RecordToLog(this HisDataMemoryBlockCollection memory,Stream stream)
        {
            foreach(var vv in memory.TagAddress)
            {
                vv.Value?.RecordToLog2(stream);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryBusy(this HisDataMemoryBlockCollection memory)
        {
            memory.IncRef();
            LoggerService.Service.Info("MemoryBlock", "make " + memory.Name + " is busy.....");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void MakeMemoryNoBusy(this HisDataMemoryBlockCollection memory)
        {
            memory.DecRef();
            LoggerService.Service.Info("MemoryBlock", "make " + memory.Name + " is ready !");
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        public static void Dump(this HisDataMemoryBlockCollection memory)
        {
            string fileName = memory.Name + "_" + DateTime.Now.ToString("yyyy_MM_dd_HH_mm_ss") + ".dmp";
            fileName = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(MarshalFixedMemoryBlock).Assembly.Location), fileName);
            using (var stream = System.IO.File.Open(fileName, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite))
            {
                foreach (var vv in memory.TagAddress)
                {
                    vv.Value?.Dump(stream);
                }
            }
        }
    }

}
