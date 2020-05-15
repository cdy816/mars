//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/14 11:00:38.
//  Version 1.0
//  种道洋
//==============================================================

using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace DBRuntime.Api
{
    public class HisDataServerProcess : ServerProcessBase
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePoint = 0;
        
        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimePoint = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimeSpan = 2;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        
        public override byte FunId => ApiFunConst.HisDataRequestFun;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="client"></param>
        /// <param name="data"></param>
        public override void ProcessData(string client, IByteBuffer data)
        {
            base.ProcessData(client, data);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
