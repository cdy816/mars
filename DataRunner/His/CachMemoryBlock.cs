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
using System.Collections.Generic;
using System.Text;

namespace DBRuntime.His
{
    /// <summary>
    /// 
    /// </summary>
    public class CachMemoryBlock : MarshalFixedMemoryBlock
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
        public CachMemoryBlock(long size):base(size)
        {
            InitValue();
        }

        /// <summary>
        /// 
        /// </summary>
        public CachMemoryBlock()
        {

        }
        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public DateTime CurrentDatetime { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
