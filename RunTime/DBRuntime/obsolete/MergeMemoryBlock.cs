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
    public class MergeMemoryBlock : MarshalMemoryBlock
    {

        #region ... Variables  ...
        private Dictionary<int, Tuple<long, int, int, int>> mTagAddress;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public MergeMemoryBlock(long size):base(size)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        public MergeMemoryBlock()
        {

        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 变量内存地址缓存
        /// Tuple 每项的含义：起始地址,值地址偏移,质量地址偏移,数据大小
        /// </summary>
        public Dictionary<int, Tuple<long, int, int,int>> TagAddress
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

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            mTagAddress.Clear();
            mTagAddress = null;
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
