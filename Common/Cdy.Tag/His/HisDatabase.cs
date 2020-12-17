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
using System.Linq;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDatabase
    {

        /// <summary>
        /// 
        /// </summary>
        public HisDatabase()
        {
            Setting = new HisSettingDoc();
        }

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 版本
        /// </summary>
        public string Version { get; set; } = "0.0.1";

        /// <summary>
        /// 历史变量
        /// </summary>
        public SortedDictionary<long,HisTag> HisTags { get; set; } = new SortedDictionary<long, HisTag>();

        /// <summary>
        /// 设置
        /// </summary>
        public HisSettingDoc Setting { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsDirty { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void AddHisTags(HisTag tag)
        {
            if(!HisTags.ContainsKey(tag.Id))
            {
                HisTags.Add(tag.Id, tag);
                IsDirty = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void RemoveHisTag(long id)
        {
            if (HisTags.ContainsKey(id))
            {
                HisTags.Remove(id);
                IsDirty = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void AddOrUpdate(HisTag tag)
        {
            if (!HisTags.ContainsKey(tag.Id))
            {
                HisTags.Add(tag.Id, tag);
            }
            else
            {
                HisTags[tag.Id] = tag;
            }
            IsDirty = true;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Freedatabase()
        {
            foreach(var vv in HisTags)
            {
                vv.Value.Parameters = null;
            }
            this.HisTags.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<HisTag> ListAllTags()
        {
            return HisTags.Values;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public HisTag GetHisTagById(int id)
        {
            return HisTags.ContainsKey(id) ? HisTags[id] : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<HisTag> ListAllDriverRecordTags()
        {
            return HisTags.Values.Where(e=>e.Type == RecordType.Driver);
        }
    }
}
