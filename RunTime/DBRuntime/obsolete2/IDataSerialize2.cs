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
    [Obsolete]
    public interface IDataSerialize2
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
        /// 请求序列化文件
        /// </summary>
        /// <param name="dataMemory"></param>
        /// <param name="date"></param>
        void RequestToSeriseFile(CompressMemory2 dataMemory);

        /// <summary>
        /// 手动更新历史数据
        /// </summary>
        /// <param name="id"></param>
        /// <param name="time"></param>
        /// <param name="data"></param>
        /// <param name="size"></param>
        void ManualRequestToSeriseFile(IMemoryBlock data);

        /// <summary>
        /// 
        /// </summary>
        void RequestToSave();

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
