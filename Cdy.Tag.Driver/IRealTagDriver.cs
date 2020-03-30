//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 21:02:59.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Driver
{
    /// <summary>
    /// 
    /// </summary>
    public interface IRealTagDriver
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
        /// <param name="id"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(int id, object value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        bool SetTagValue(List<int> ids, object value);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        List<int> GetTagByLinkAddress(string address);

        Dictionary<string, List<int>> GetTagsByLinkAddress(List<string> address);

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
