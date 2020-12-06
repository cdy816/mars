//==============================================================
//  Copyright (C) 2020 Chongdaoyang Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/7/21 21:59:36 .
//  Version 1.0
//  CDYWORK
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBRuntime.His
{
    /// <summary>
    /// 
    /// </summary>
    //public class HisDataMemoryBlock: FixedMemoryBlock
    public class HisDataMemoryBlock : MarshalFixedMemoryBlock
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public HisDataMemoryBlock(int size):base(size)
        {

        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }


        /// <summary>
        /// 时间戳地址
        /// </summary>
        public long TimerAddress { get; set; } = 0;

        /// <summary>
        /// 数值起始地址
        /// </summary>
        public long ValueAddress { get; set; }

        /// <summary>
        /// 质量戳地址
        /// </summary>
        public long QualityAddress { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public byte TimeLen { get; set; } = 2;

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
