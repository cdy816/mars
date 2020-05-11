//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/11 12:09:03.
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
    public class SocketBuffer:FixedMemoryBlock
    {

        #region ... Variables  ...
        private int mReceiveAddr = 0;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        public SocketBuffer(int size):base(size)
        {

        }
        #endregion ...Constructor...

        #region ... Properties ...
        
        /// <summary>
        /// 
        /// </summary>
        public int ReceiveAddr 
        {
            get { return mReceiveAddr; }
            set 
            {
                if (value > this.Length)
                {
                    mReceiveAddr = (int)(value - Length);
                }
                else
                {
                    mReceiveAddr = value;
                }
            } 
        }

        /// <summary>
        /// 
        /// </summary>
        public int SendAddr { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
