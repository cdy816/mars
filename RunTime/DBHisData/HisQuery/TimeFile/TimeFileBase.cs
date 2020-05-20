//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class TimeFileBase:Dictionary<int,TimeFileBase>
    {
        #region ... Variables  ...

        private int mTimeKey = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
            /// 
            /// </summary>
        public int TimeKey
        {
            get
            {
                return mTimeKey;
            }
            set
            {
                if (mTimeKey != value)
                {
                    mTimeKey = value;
                }
            }
        }



        #endregion ...Properties...





        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="value"></param>
        public TimeFileBase AddTimefile(int key, TimeFileBase value)
        {
            if (this.ContainsKey(key))
            {
                return this[key];
            }
            else
            {
                base.Add(key, value);
                return value;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
