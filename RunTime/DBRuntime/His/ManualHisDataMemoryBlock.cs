//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/9/21 15:04:04.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DBRuntime.His
{
    /// <summary>
    /// 
    /// </summary>
    public class ManualHisDataMemoryBlock: HisDataMemoryBlock
    {

        #region ... Variables  ...
        private object mLockObj = new object();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public ManualHisDataMemoryBlock(int size):base(size)
        {

        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int CurrentCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int MaxCount { get; set; }

        /// <summary>
        /// 时间存储单位,ms
        /// </summary>
        public int TimeUnit { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...
        
        /// <summary>
        /// 
        /// </summary>
        public void Lock()
        {
            Monitor.Enter(mLockObj);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Relase()
        {
            Monitor.Exit(mLockObj);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
