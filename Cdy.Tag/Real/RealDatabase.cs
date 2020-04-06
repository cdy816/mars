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
using System.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class RealDatabase: ITagManager
    {
        /// <summary>
        /// 
        /// </summary>
        public RealDatabase()
        {
            Tags = new Dictionary<int, Tagbase>();
            NamedTags = new Dictionary<string, Tagbase>();
            Groups = new Dictionary<string, TagGroup>();
        }

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; } = "local";

        /// <summary>
        /// 
        /// </summary>
        public string Version { get; set; } = "0.0.1";

        /// <summary>
        /// 当前最大ID
        /// </summary>
        public int MaxId { get; set; } = -1;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string,Tagbase> NamedTags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int,Tagbase> Tags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string,TagGroup> Groups { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public int? GetTagIdByName(string name)
        {
            if(NamedTags.ContainsKey(name))
            {
                return NamedTags[name].Id;
            }
            else
            {
                return null;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public List<int> GetTagIdByLinkAddress(string address)
        {
            return Tags.Values.Where(e => e.LinkAddress == address).Select(e => e.Id).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public Dictionary<string,List<int>> GetTagsIdByLinkAddress(List<string> address)
        {
            Dictionary<string, List<int>> re = new Dictionary<string, List<int>>();
            foreach(var vv in address)
            {
                re.Add(vv, GetTagIdByLinkAddress(vv));
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public Tagbase GetTagByName(string name)
        {
            return NamedTags.ContainsKey(name) ? NamedTags[name] : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public Tagbase GetTagById(int id)
        {
            return Tags.ContainsKey(id) ? Tags[id] : null;
        }


        /// <summary>
        /// 添加
        /// </summary>
        /// <param name="tag"></param>
        public bool Add(Tagbase tag)
        {
            if (!Tags.ContainsKey(tag.Id))
            {
                Tags.Add(tag.Id, tag);
                CheckAndAddGroup(tag.Group)?.Tags.Add(tag);
                MaxId = Math.Max(MaxId, tag.Id);

                if (!NamedTags.ContainsKey(tag.Name))
                {
                    NamedTags.Add(tag.Name, tag);
                }
                else
                {
                    NamedTags[tag.Name] = tag;
                }

                return true;
            }
            else
            {
                Tags[tag.Id] = tag;

                if (!NamedTags.ContainsKey(tag.Name))
                {
                    NamedTags.Add(tag.Name, tag);
                }
                else
                {
                    NamedTags[tag.Name] = tag;
                }
            }
            return false;
        }

        /// <summary>
        /// 添加或更新
        /// </summary>
        /// <param name="id"></param>
        public void AddOrUpdate(Tagbase tag)
        {
            if (!Tags.ContainsKey(tag.Id))
            {
                Tags.Add(tag.Id, tag);
                CheckAndAddGroup(tag.Group)?.Tags.Add(tag);

                if (!NamedTags.ContainsKey(tag.Name))
                {
                    NamedTags.Add(tag.Name, tag);
                }
                else
                {
                    NamedTags[tag.Name] = tag;
                }
            }
            else
            {
                Tags[tag.Id] = tag;

                if (!NamedTags.ContainsKey(tag.Name))
                {
                    NamedTags.Add(tag.Name, tag);
                }
                else
                {
                    NamedTags[tag.Name] = tag;
                }
            }
            MaxId = Math.Max(MaxId, tag.Id);
        }

        /// <summary>
        /// 更改变量所在的组
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="group"></param>
        public void ChangedTagGroup(Tagbase tag,string group)
        {
            if (string.IsNullOrEmpty(tag.Group))
            {
                tag.Group = group;
                CheckAndAddGroup(tag.Group)?.Tags.Add(tag);
            }
            else
            {
                if(Groups.ContainsKey(tag.Group))
                {
                    var vv = Groups[tag.Group].Tags;
                    if(vv.Contains(tag))
                    {
                        vv.Remove(tag);
                    }
                }
            }
        }

        /// <summary>
        /// 追加新的变量
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool Append(Tagbase tag)
        {
            tag.Id = ++MaxId;
            return Add(tag);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool Remove(int id)
        {
            if(Tags.ContainsKey(id))
            {
                var tag = Tags[id];
                Tags.Remove(id);
                if(Groups.ContainsKey(tag.Group))
                {
                    var tgs = Groups[tag.Group].Tags;
                    if(tgs.Contains(tag))
                    {
                        tgs.Remove(tag);
                    }
                }
            }
            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <returns></returns>
        public List<Tagbase> GetTagsByGroup(string group)
        {
            if(this.Groups.ContainsKey(group))
            {
                return this.Groups[group].Tags;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        public void RemoveByGroup(string group)
        {
            if (this.Groups.ContainsKey(group))
            {
                var vv = this.Groups[group].Tags;
                foreach (var vvv in vv)
                {
                    Tags.Remove(vvv.Id);
                }                
                vv.Clear();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        public void RemoveGroup(string group)
        {
            if (this.Groups.ContainsKey(group))
            {
                var vv = this.Groups[group].Tags;
                foreach (var vvv in vv)
                {
                    Tags.Remove(vvv.Id);
                }

                //获取改组的所有子组
                var gg = GetAllChildGroups(this.Groups[group]);
                var ggnames = gg.Select(e => e.FullName);
                foreach (var vvg in gg)
                {
                    foreach (var vvv in vvg.Tags)
                    {
                        Tags.Remove(vvv.Id);
                    }
                    vvg.Tags.Clear();
                }

                this.Groups.Remove(group);
                foreach (var vvn in ggnames)
                    this.Groups.Remove(vvn);

                vv.Clear();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        public List<TagGroup> GetGroups(TagGroup parent)
        {
            return Groups.Values.Where(e => e.Parent == parent).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        public List<TagGroup> GetAllChildGroups(TagGroup parent)
        {
            List<TagGroup> re = new List<TagGroup>();
            var grps = GetGroups(parent);
            re.AddRange(grps);
            foreach(var vv in grps)
            {
                re.AddRange(GetAllChildGroups(vv));
            }
            return re;
        }

        /// <summary>
        /// 改变变量组的父类
        /// </summary>
        /// <param name="group"></param>
        /// <param name="oldParentName"></param>
        /// <param name="newParentName"></param>
        public void ChangeGroupParent(string group,string oldParentName,string newParentName)
        {
            string oldgroupFullName = oldParentName + "." + group;
            if (Groups.ContainsKey(oldgroupFullName))
            {
                var grp = Groups[oldgroupFullName];
                //获取改组的所有子组
                var gg = GetAllChildGroups(grp);

                //从Groups删除目标组以及所有子组
                Groups.Remove(oldgroupFullName);

                foreach (var vv in gg.Select(e => e.FullName))
                {
                    if (Groups.ContainsKey(vv))
                    {
                        Groups.Remove(vv);
                    }
                }

                if(Groups.ContainsKey(newParentName))
                {
                    grp.Parent = Groups[newParentName];
                }

                //修改子组变量对变量组的引用
                string fullname = string.Empty;
                foreach (var vv in gg)
                {
                    fullname = vv.FullName;
                    foreach (var vvt in vv.Tags)
                    {
                        vvt.Group = fullname;
                    }
                }

                //修改本组变量对变量组的引用
                fullname = grp.FullName;
                if (grp.Tags != null)
                {
                    foreach (var vv in grp.Tags)
                    {
                        vv.Group = fullname;
                    }
                }
                //将目标组以及所有子组添加至Groups中
                Groups.Add(fullname, grp);
                foreach (var vv in gg)
                {
                    Groups.Add(vv.FullName, vv);
                }
            }
        }

        /// <summary>
        /// 变量组改名
        /// </summary>
        /// <param name="group"></param>
        /// <param name="newName"></param>
        public void ChangeGroupName(string oldgroupFullName,string newName)
        {
            if (Groups.ContainsKey(oldgroupFullName))
            {
                var grp = Groups[oldgroupFullName];
                //获取改组的所有子组
                var gg = GetAllChildGroups(grp);

                //从Groups删除目标组以及所有子组
                Groups.Remove(oldgroupFullName);

                foreach(var vv  in gg.Select(e=>e.FullName))
                {
                    if(Groups.ContainsKey(vv))
                    {
                        Groups.Remove(vv);
                    }
                }

                grp.Name = newName;

                //修改子组变量对变量组的引用
                string fullname = string.Empty;
                foreach (var vv in gg)
                {
                    fullname = vv.FullName;
                    foreach (var vvt in vv.Tags)
                    {
                        vvt.Group = fullname;
                    }
                }

                //修改本组变量对变量组的引用
                fullname = grp.FullName;
                if (grp.Tags != null)
                {
                    foreach (var vv in grp.Tags)
                    {
                        vv.Group = fullname;
                    }
                }
                //将目标组以及所有子组添加至Groups中
                Groups.Add(fullname,grp);
                foreach(var vv in gg)
                {
                    Groups.Add(vv.FullName, vv);
                }
            }
        }

        /// <summary>
        /// 将变量组移动另一个变量组下
        /// </summary>
        /// <param name="groupFullName"></param>
        /// <param name="newParentFullName"></param>
        public void ChangeTagGroupParent(string groupFullName, string newParentFullName)
        {
            if (Groups.ContainsKey(groupFullName))
            {
                var grp = Groups[groupFullName];
                var gg = GetAllChildGroups(grp);

                var parent = Groups.ContainsKey(newParentFullName) ? Groups[newParentFullName] : null;

                Groups.Remove(grp.FullName);
                foreach(var vv in gg)
                {
                    Groups.Remove(vv.FullName);
                }

                grp.Parent = parent;

                Groups.Add(grp.FullName,grp);
                foreach(var vv in gg)
                {
                    Groups.Add(vv.FullName, vv);
                }
            }
        }

        /// <summary>
        /// 检查并添加组
        /// </summary>
        /// <param name="groupName">组名称,多级组之间通过"."分割</param>
        public TagGroup CheckAndAddGroup(string groupName)
        {
            if (string.IsNullOrEmpty(groupName)) return null;
            if (!Groups.ContainsKey(groupName))
            {
                TagGroup parent = null;
                if (groupName.LastIndexOf(".") > 0)
                {
                    string sparentName = groupName.Substring(0, groupName.LastIndexOf("."));
                    parent = CheckAndAddGroup(sparentName);
                }
                TagGroup tg = new TagGroup() { Parent = parent,Name = groupName };
                Groups.Add(tg.FullName, tg);
                return tg;
            }
            else
            {
                return Groups[groupName];
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ICollection<Tagbase> ListAllTags()
        {
            return Tags.Values;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public List<int?> GetTagIdByName(List<string> name)
        {
            List<int?> re = new List<int?>();
            foreach(var vv in name)
            {
                re.Add(GetTagIdByName(vv));
            }
            return re;
        }
    }
}
