using System;
using System.Collections.Generic;
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
        public Dictionary<long,HisTag> HisTags { get; set; }

        /// <summary>
        /// 设置
        /// </summary>
        public HisSettingDoc Setting { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void AddHisTags(HisTag tag)
        {
            if(!HisTags.ContainsKey(tag.Id))
            {
                HisTags.Add(tag.Id, tag);
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
        }
    }
}
