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
    public interface IHisDataPatch
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
        void Take();

        /// <summary>
        /// 开始数据修改
        /// </summary>
        /// <param name="id"></param>
        /// <param name="type"></param>
        /// <param name="user"></param>
        /// <param name="msg"></param>
        /// <param name="valuecount"></param>
        void BeginPatch(int id, TagType type, string user, string msg, int valuecount);

        /// <summary>
        /// 附加修改数据
        /// </summary>
        /// <param name="time"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        void AppendPatchValue(DateTime time, object value, byte quality);

        /// <summary>
        /// 结束数据修改
        /// </summary>
        void EndPatch();

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
