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
        ///// <summary>
        ///// 
        ///// </summary>
        //private object mLockObj = new object();
        //public static int Count = 0;

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
            //Count++;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 被格对齐的开始时间
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 真实的开始时间
        /// </summary>
        public DateTime RealTime { get; set; }

        /// <summary>
        /// 结束时间
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
            Monitor.Enter(this);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Relase()
        {
            Monitor.Exit(this);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Reset()
        {
            Time = DateTime.MinValue;
            EndTime = DateTime.MinValue;
            CurrentCount = 0;
            MaxCount = 0;
            TimeUnit = 0;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
