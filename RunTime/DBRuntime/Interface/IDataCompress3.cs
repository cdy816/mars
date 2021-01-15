//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public interface IDataCompress3
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="dataMemory"></param>
        void RequestToCompress(HisDataMemoryBlockCollection3 dataMemory);

        /// <summary>
        /// 请求手动压缩数据
        /// </summary>
        /// <param name="data"></param>
        void RequestManualToCompress(ManualHisDataMemoryBlock data);

        /// <summary>
        /// 
        /// </summary>
        void SubmitManualToCompress();


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
