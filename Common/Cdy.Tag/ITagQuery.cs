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
    public interface ITagManager
    {
        string Name
        {
            get;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        int? GetTagIdByName(string name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        List<int?> GetTagIdByName(List<string> name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        Tagbase GetTagByName(string name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <returns></returns>
        List<Tagbase> GetTagsByGroup(string group);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagnames"></param>
        /// <returns></returns>
        IEnumerable<Tagbase> GetTagsByName(IEnumerable<string> tagnames);

        List<Tagbase> GetTagByArea(string area);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Tagbase GetTagById(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        int MaxTagId();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        int MinTagId();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<Tagbase> ListAllTags();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        IEnumerable<string> ListTagGroups();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        IEnumerable<string> GetTagGroup(string parent);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        byte[] SeriseToStream();

    }
}
