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
    public class HisDataQuery
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
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public List<HisQueryResult<T>> Query<T>(List<string> tag, DateTime startTime, DateTime endTime)
        {
            var lid = ServiceLocator.Locator.Resolve<ITagQuery>().GetTagIdByName(tag);
            if (lid != null)
            {
                return Query<T>(lid, startTime, endTime);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public HisQueryResult<T> Query<T>(string tag,DateTime startTime, DateTime endTime)
        {
            int? lid = ServiceLocator.Locator.Resolve<ITagQuery>().GetTagIdByName(tag);
            if(lid!=null)
            {
                return Query<T>(lid.Value, startTime, endTime);
            }
            return null;
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagId"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public HisQueryResult<T> Query<T>(int tagId, DateTime startTime, DateTime endTime)
        {
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagId"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public List<HisQueryResult<T>> Query<T>(List<int?> tagId, DateTime startTime, DateTime endTime)
        {
            return null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
