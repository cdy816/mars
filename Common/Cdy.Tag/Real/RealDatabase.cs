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
            Tags = new SortedDictionary<int, Tagbase>();
            NamedTags = new Dictionary<string, Tagbase>();
            Groups = new Dictionary<string, TagGroup>();
        }

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; } = "local";

        /// <summary>
        /// 最后生成时间
        /// </summary>
        public string UpdateTime { get; set; } = "";

        /// <summary>
        /// 
        /// </summary>
        public string Version { get; set; } = "0.0.1";

        /// <summary>
        /// 当前最大ID
        /// </summary>
        public int MaxId { get; set; } = 0;

        /// <summary>
        /// 最小ID
        /// </summary>
        public int MinId { get; set; } = int.MaxValue;

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string,Tagbase> NamedTags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public SortedDictionary<int,Tagbase> Tags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string,TagGroup> Groups { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsDirty { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        public Database Owner { get; set; }

        /// <summary>
        /// 构建名字映射
        /// </summary>
        public void BuildNameMap()
        {
            NamedTags.Clear();
            foreach(var vv in Tags)
            {
                vv.Value.UpdateFullName();
                if(!NamedTags.ContainsKey(vv.Value.FullName))
                {
                    NamedTags.Add(vv.Value.FullName, vv.Value);
                }
            }
        }

        /// <summary>
        /// 建立组间映射关系
        /// </summary>
        public void BuildGroupMap()
        {
            foreach(var vv in Groups)
            {
                vv.Value.Tags.AddRange(Tags.Where(e => e.Value.Group == vv.Value.FullName).Select(e=>e.Value));
            }
        }

        /// <summary>
        /// 将复杂类型变量的子类型注册到根目录里
        /// </summary>
        /// <param name="tag"></param>
        public void BuildComplexTagId(Tagbase tag)
        {
            if (tag is ComplexTag)
            {
                foreach (var vv in (tag as ComplexTag).Tags)
                {
                    if (!Tags.ContainsKey(vv.Key))
                        Tags.Add(vv.Key, vv.Value);
                    else
                    {
                        Tags[vv.Key] = vv.Value;
                    }
                    if (vv.Value is ComplexTag)
                    {
                        BuildComplexTagId(vv.Value);
                    }
                }
            }
        }

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
        public List<Tagbase> GetTagByLinkAddress(string address)
        {
            return Tags.Values.Where(e => e.LinkAddress == address).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public Dictionary<string, List<Tagbase>> GetTagsByLinkAddress(List<string> address)
        {
            Dictionary<string, List<Tagbase>> re = new Dictionary<string, List<Tagbase>>();
            foreach (var vv in address)
            {
                re.Add(vv, GetTagByLinkAddress(vv));
            }
            return re;
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public List<int> GetTagIdsByLinkAddress(string address)
        {
            return Tags.Values.Where(e => e.LinkAddress == address).Select(e => e.Id).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="address"></param>
        /// <returns></returns>
        public List<Tagbase> GetTagIdsByLinkAddressStartHeadString(string address)
        {
            return Tags.Values.Where(e => e.LinkAddress.StartsWith(address)).ToList();
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
                re.Add(vv, GetTagIdsByLinkAddress(vv));
            }
            return re;
        }

        #region ITagManager
        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public List<Tagbase> GetTagsById(List<int> ids)
        {
            List<Tagbase> re = new List<Tagbase>();
            foreach(var vv in ids)
            {
                if(Tags.ContainsKey(vv))
                {
                    re.Add(Tags[vv]);
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagnames"></param>
        /// <returns></returns>
        public IEnumerable<Tagbase> GetTagsByName(IEnumerable<string> tagnames)
        {
            return Tags.Values.Where(e => tagnames.Contains(e.FullName));
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

        #endregion


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

                tag.UpdateFullName();

                if (!NamedTags.ContainsKey(tag.FullName))
                {
                    NamedTags.Add(tag.FullName, tag);
                }
                else
                {
                    NamedTags[tag.FullName] = tag;
                }
               
                UpdateComplextTag(null, tag);
                IsDirty = true;
                return true;
            }
            else
            {

                var vtag = Tags[tag.Id];

                if(NamedTags.ContainsKey(vtag.FullName))
                {
                    NamedTags.Remove(vtag.FullName);
                }

                tag.UpdateFullName();

                Tags[tag.Id] = tag;

                if (!NamedTags.ContainsKey(tag.FullName))
                {
                    NamedTags.Add(tag.FullName, tag);
                }
                else
                {
                    NamedTags[tag.FullName] = tag;
                }
                
                UpdateComplextTag(vtag, tag);
            }
            IsDirty = true;

            MinId = Math.Min(MinId, tag.Id);
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="tag"></param>
        public void UpdateById(int id,Tagbase tag)
        {
            if(Tags.ContainsKey(id))
            {
                tag.UpdateFullName();
                var oldtag = Tags[id];

              
                string sname = oldtag.FullName;

                if (sname != tag.FullName)
                {
                    if (NamedTags.ContainsKey(sname))
                        NamedTags.Remove(sname);
                    NamedTags.Add(tag.FullName, tag);
                }
                else
                {
                    NamedTags[tag.FullName] = tag;
                }

                if(!string.IsNullOrEmpty(tag.Parent))
                {
                    int pid = int.Parse(tag.Parent);
                    if(Tags.ContainsKey(pid))
                    {
                        (Tags[pid] as ComplexTag).Update(tag);
                    }
                }

                Tags[id] = tag;
                if (oldtag.Type != tag.Type || ((oldtag is ComplexTag)&&(tag is ComplexTag) && (oldtag as ComplexTag).LinkComplexClass != (tag as ComplexTag).LinkComplexClass))
                {
                    UpdateComplextTag(oldtag, tag);
                }
                else if(tag is ComplexTag)
                {
                    (tag as ComplexTag).Tags.Clear();
                    foreach(var vv in (oldtag as ComplexTag).Tags)
                    {
                        (tag as ComplexTag).Tags.Add(vv.Key, vv.Value);
                    }
                }
                IsDirty = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        private void RemoveComplexTagSubTags(ComplexTag tag)
        {
            foreach (var vv in tag.Tags)
            {
                if (Tags.ContainsKey(vv.Key))
                {
                    Tags.Remove(vv.Key);
                }
                if(NamedTags.ContainsKey(vv.Value.FullName))
                {
                    NamedTags.Remove(vv.Value.FullName);
                }
                if(vv.Value is ComplexTag)
                {
                    RemoveComplexTagSubTags(vv.Value as ComplexTag);
                }
            }
            tag.Tags.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="cls"></param>
        private void AddComplexTagSubTags(ComplexTag tag,ComplexTagClass cls)
        {
            foreach(var vv in cls.Tags)
            {
                var vtag = vv.Value.Clone();
                vtag.Id = MaxId++;
                vtag.Parent = tag.Id.ToString();

                if (!Tags.ContainsKey(vtag.Id))
                {
                    Tags.Add(vtag.Id, vtag);
                }
                else
                {
                    Tags[vtag.Id] = vtag;
                }
                
                vtag.FullName = tag.FullName + "." + vtag.Name;

                if(!this.NamedTags.ContainsKey(vtag.FullName))
                {
                    this.NamedTags.Add(vtag.FullName, vtag);
                }
                else
                {
                    this.NamedTags[vtag.FullName] = vtag;
                }

                tag.Tags.Add(vtag.Id, vtag);

                var hstag = cls.HisTags.ContainsKey(vv.Key)?cls.HisTags[vv.Key].Clone():null;
                if(hstag != null)
                {
                    hstag.Id=vtag.Id;
                    Owner.HisDatabase.AddOrUpdate(hstag);
                }
               

                if(vtag is ComplexTag)
                {
                    var cvtag = vtag as ComplexTag;
                    if(cvtag!=null && !string.IsNullOrEmpty(cvtag.LinkComplexClass) && Owner!=null && Owner.ComplexTagClass.Class.ContainsKey(cvtag.LinkComplexClass))
                    {
                        AddComplexTagSubTags(cvtag, Owner.ComplexTagClass.Class[cvtag.LinkComplexClass]);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldtag"></param>
        /// <param name="newtag"></param>
        private void UpdateComplextTag(Tagbase oldtag, Tagbase newtag)
        {
            if ((oldtag is ComplexTag) && (newtag is ComplexTag))
            {
                if((oldtag as ComplexTag).LinkComplexClass != (newtag as ComplexTag).LinkComplexClass)
                {
                    RemoveComplexTagSubTags(oldtag as ComplexTag);
                    ComplexTag ct = newtag as ComplexTag;
                    if(ct!=null&&!string.IsNullOrEmpty(ct.LinkComplexClass))
                    {
                        if(Owner!=null&&Owner.ComplexTagClass.Class.ContainsKey(ct.LinkComplexClass))
                        {
                            AddComplexTagSubTags(newtag as ComplexTag, Owner.ComplexTagClass.Class[ct.LinkComplexClass]);
                        }
                    }
                }
            }
            else if(oldtag is ComplexTag)
            {
                RemoveComplexTagSubTags(oldtag as ComplexTag);
            }
            else if(newtag is ComplexTag)
            {
                ComplexTag ct = newtag as ComplexTag;
                if (ct != null && !string.IsNullOrEmpty(ct.LinkComplexClass))
                {
                    if (Owner != null && Owner.ComplexTagClass.Class.ContainsKey(ct.LinkComplexClass))
                    {
                        AddComplexTagSubTags(newtag as ComplexTag, Owner.ComplexTagClass.Class[ct.LinkComplexClass]);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sname"></param>
        /// <param name="tag"></param>
        public void Update(string sname,Tagbase tag)
        {
            if(NamedTags.ContainsKey(sname))
            {
                tag.UpdateFullName();

                var oldtag = NamedTags[sname];
                var vid = oldtag.Id;
                tag.Id = vid;
                NamedTags[sname] = tag;
                Tags[vid] = tag;
               
                //UpdateComplextTag(vtag, tag);

                if (!string.IsNullOrEmpty(tag.Parent))
                {
                    int pid = int.Parse(tag.Parent);
                    if (Tags.ContainsKey(pid))
                    {
                        (Tags[pid] as ComplexTag).Update(tag);
                    }
                }

                if (oldtag.Type != tag.Type || ((oldtag is ComplexTag) && (tag is ComplexTag) && (oldtag as ComplexTag).LinkComplexClass != (tag as ComplexTag).LinkComplexClass))
                {
                    UpdateComplextTag(oldtag, tag);
                }
                else if (tag is ComplexTag)
                {
                    (tag as ComplexTag).Tags.Clear();
                    foreach (var vv in (oldtag as ComplexTag).Tags)
                    {
                        (tag as ComplexTag).Tags.Add(vv.Key, vv.Value);
                    }
                }

                IsDirty = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void UpdateForRuntime(Tagbase tag)
        {
            if(Tags.ContainsKey(tag.Id))
            {
                tag.UpdateFullName();

                var oldtag = Tags[tag.Id];
                var oldname = oldtag.FullName;
                if(oldname!=tag.FullName)
                {
                    if(NamedTags.ContainsKey(oldname))
                    NamedTags.Remove(oldname);
                    NamedTags.Add(tag.FullName, tag);
                }
                else
                {
                    NamedTags[tag.FullName] = tag;
                }
                Tags[tag.Id] = tag;

                IsDirty = true;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="head"></param>
        /// <param name="names"></param>
        private void ListAllTagNames(ComplexTag tag, string head, Dictionary<string, Tuple<int,string>> names)
        {
            foreach (var vv in tag.Tags)
            {
                string sname = !string.IsNullOrEmpty(head) ? head + "." + vv.Value.Name : vv.Value.Name;
                names.Add(sname,new Tuple<int, string>(vv.Value.Id,vv.Value.LinkAddress));
                if (vv.Value is ComplexTag)
                {
                    ListAllTagNames(vv.Value as ComplexTag, sname, names);
                }
            }
        }

        /// <summary>
        /// 重新根据变量类生产变量
        /// </summary>
        /// <param name="tag"></param>
        public void ReCreatComplexTagChild(ComplexTag tag)
        {
            if(string.IsNullOrEmpty(tag.LinkComplexClass))
            {
                RemoveComplexTagSubTags(tag);
            }
            else
            {
                Dictionary<string, Tuple<int, string>> names = new Dictionary<string, Tuple<int, string>>();
                ListAllTagNames(tag,"",names);
                foreach(var vv in names)
                {
                    if(Tags.ContainsKey(vv.Value.Item1))
                    {
                        var vtag = Tags[vv.Value.Item1];
                        Tags.Remove(vv.Value.Item1);
                        if(NamedTags.ContainsKey(vtag.FullName))
                        {
                            NamedTags.Remove(vtag.FullName);
                        }
                    }

                    if(Owner.HisDatabase.HisTags.ContainsKey(vv.Value.Item1))
                    {
                        Owner.HisDatabase.HisTags.Remove(vv.Value.Item1);
                    }
                }
                tag.Tags.Clear();

                var cls = Owner.ComplexTagClass.Class[tag.LinkComplexClass];

                int id = 0;
                foreach(var vv in cls.NamedTags)
                {
                    var vtag = vv.Value.Clone();

                    if (names.ContainsKey(vv.Key))
                    {
                        id = names[vv.Key].Item1;
                        vtag.LinkAddress = names[vv.Key].Item2;
                    }
                    else
                    {
                        id = MaxId++;
                    }
                    
                    vtag.Id = id;
                    vtag.Parent = tag.Id.ToString();
                    tag.Tags.Add(id, vtag);
                    Tags.Add(id, vtag);

                    if(cls.HisTags.ContainsKey(vv.Value.Id))
                    {
                        var htag = cls.HisTags[vv.Value.Id].Clone();
                        htag.Id = id;
                        Owner.HisDatabase.HisTags.Add(id, htag);
                    }

                    if (vtag is ComplexTag)
                    {
                        ReCreatComplexTagChildInner(vtag as ComplexTag, vv.Key, names);
                    }
                }

            }
        }

        private void ReCreatComplexTagChildInner(ComplexTag tag,string head, Dictionary<string, Tuple<int, string>> names)
        {
            int id = 0;

            if (!Owner.ComplexTagClass.Class.ContainsKey(tag.LinkComplexClass)) return;

            var cls = Owner.ComplexTagClass.Class[tag.LinkComplexClass];
            tag.Tags.Clear();

            //foreach (var vv in tag.Tags)
            foreach (var vv in cls.Tags)
            {
                string stmp = head + "." + vv.Value.Name;

                var vtag = vv.Value.Clone();

                if (names.ContainsKey(stmp))
                {
                    id = names[stmp].Item1;
                    vtag.LinkAddress = names[stmp].Item2;
                }
                else
                {
                    id = MaxId++;
                }
              
                if (cls.HisTags.ContainsKey(vv.Value.Id))
                {
                    var htag = cls.HisTags[vv.Value.Id].Clone();
                    htag.Id = id;
                    Owner.HisDatabase.HisTags.Add(id, htag);
                }
                vtag.Id = id;
                vtag.Parent = tag.Id.ToString();
                tag.Tags.Add(id, vtag);

                if (!NamedTags.ContainsKey(stmp))
                {
                    NamedTags.Add(stmp, vtag);
                }
                else
                {
                    NamedTags[stmp] = vtag;
                }

                if (!Tags.ContainsKey(id))
                {
                    Tags.Add(id, vtag);
                }
                else
                {
                    Tags[id] = vtag;
                }

                if(vtag is ComplexTag)
                {
                    ReCreatComplexTagChildInner(vtag as ComplexTag, stmp, names);
                }

            }
        }



        /// <summary>
        /// 添加或更新
        /// </summary>
        /// <param name="id"></param>
        public void AddOrUpdate(Tagbase tag)
        {
            if (!NamedTags.ContainsKey(tag.FullName))
            {
                Append(tag);
            }
            else
            {
                Update(tag.FullName, tag);
            }
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
                    tag.Group = group;
                    CheckAndAddGroup(tag.Group)?.Tags.Add(tag);
                }
            }
            IsDirty = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldname"></param>
        /// <param name="newname"></param>
        public void ChangedComplexLinkClass(string oldname,string newname)
        {
            foreach(var vv in this.Tags.Where(e=>e.Value is ComplexTag && (e.Value as ComplexTag).LinkComplexClass==oldname))
            {
                (vv.Value as ComplexTag).LinkComplexClass = newname;
            }
        }

        /// <summary>
        /// 追加新的变量
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool Append(Tagbase tag)
        {
            tag.Id = MaxId++;
            MinId = Math.Min(MinId, tag.Id);
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

                if(NamedTags.ContainsKey(tag.FullName))
                {
                    NamedTags.Remove(tag.FullName);
                }
                if(Groups.ContainsKey(tag.Group))
                {
                    var tgs = Groups[tag.Group].Tags;
                    if(tgs.Contains(tag))
                    {
                        tgs.Remove(tag);
                    }
                }

                if (MinId == id)
                    MinId = Tags.Keys.Count > 0 ? Tags.Keys.Min() : 0;

                if(tag is ComplexTag)
                {
                    RemoveComplexTagSubTags(tag as ComplexTag);
                }

                IsDirty = true;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool RemoveWithoutGroupProcess(Tagbase tag)
        {
            Tags.Remove(tag.Id);
            NamedTags.Remove(tag.FullName);
            IsDirty = true;

            MinId = Tags.Keys.Count > 0 ? Tags.Keys.Min() : 0;

            return true;
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
            else if(string.IsNullOrEmpty(group))
            {
                return this.Tags.Values.Where(e => string.IsNullOrEmpty(e.Group)&&string.IsNullOrEmpty(e.Parent)).ToList();
            }
            return new List<Tagbase>();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        public List<int> RemoveByGroup(string group)
        {
            List<int> re = new List<int>();
            if (this.Groups.ContainsKey(group))
            {
                var vv = this.Groups[group].Tags;
                foreach (var vvv in vv)
                {
                    Tags.Remove(vvv.Id);

                    re.Add(vvv.Id);
                    if(vvv is ComplexTag)
                    {
                        RemoveComplexTagSubTags(vvv as ComplexTag);
                    }
                }                
                vv.Clear();
                MinId = Tags.Keys.Count > 0 ? Tags.Keys.Min() : 0;
                IsDirty = true;
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        public List<int> RemoveGroup(string group)
        {
            List<int> re = new List<int>();
            if (this.Groups.ContainsKey(group))
            {
                var vv = this.Groups[group].Tags;
                foreach (var vvv in vv)
                {
                    Tags.Remove(vvv.Id);

                    re.Add(vvv.Id);
                    if (vvv is ComplexTag)
                    {
                        RemoveComplexTagSubTags(vvv as ComplexTag);
                    }
                }

                //获取改组的所有子组
                var gg = GetAllChildGroups(this.Groups[group]);
                var ggnames = gg.Select(e => e.FullName);
                foreach (var vvg in gg)
                {
                    foreach (var vvv in vvg.Tags)
                    {
                        Tags.Remove(vvv.Id);
                        re.Add(vvv.Id);
                        if (vvv is ComplexTag)
                        {
                            RemoveComplexTagSubTags(vvv as ComplexTag);
                        }
                    }
                    vvg.Tags.Clear();
                }

                this.Groups.Remove(group);
                foreach (var vvn in ggnames)
                    this.Groups.Remove(vvn);

                vv.Clear();

                MinId = Tags.Keys.Count>0?Tags.Keys.Min():0;

                IsDirty = true;
            }
            return re;
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
        /// <param name="groupName"></param>
        /// <returns></returns>
        public TagGroup GetGroup(string groupName)
        {
            return Groups.ContainsKey(groupName) ? Groups[groupName] : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="chileGroupName"></param>
        /// <returns></returns>
        public bool HasChildGroup(TagGroup parent, string childGroupName)
        {
            var vss = Groups.Values.Where(e => e.Parent == parent).Select(e => e.Name);
            if (vss.Count() > 0 && vss.Contains(childGroupName))
            {
                return true;
            }
            return false;
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
                var vnames = gg.Select(e => e.FullName).ToList();
                Groups.Remove(oldgroupFullName);

                foreach (var vv in vnames)
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
                IsDirty = true;
            }
        }

        /// <summary>
        /// 变量组改名
        /// </summary>
        /// <param name="group"></param>
        /// <param name="newName"></param>
        public bool ChangeGroupName(string oldgroupFullName,string newName)
        {
            if (Groups.ContainsKey(oldgroupFullName))
            {
                var grp = Groups[oldgroupFullName];

                var ss = grp.Parent != null ? grp.Parent.FullName + "." + newName : newName;

                if (Groups.ContainsKey(ss))
                {
                    return false;
                }

                //获取改组的所有子组
                var gg = GetAllChildGroups(grp);

                //从Groups删除目标组以及所有子组

                var vnames = gg.Select(e => e.FullName).ToList();

                Groups.Remove(oldgroupFullName);

                foreach(var vv  in vnames)
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

                IsDirty = true;
                return true;
            }
            return false;
        }

        ///// <summary>
        ///// 将变量组移动另一个变量组下
        ///// </summary>
        ///// <param name="groupFullName"></param>
        ///// <param name="newParentFullName"></param>
        //public void ChangeTagGroupParent(string groupFullName, string newParentFullName)
        //{
        //    if (Groups.ContainsKey(groupFullName))
        //    {
        //        var grp = Groups[groupFullName];
        //        var gg = GetAllChildGroups(grp);

        //        var parent = Groups.ContainsKey(newParentFullName) ? Groups[newParentFullName] : null;

        //        Groups.Remove(grp.FullName);
        //        foreach(var vv in gg)
        //        {
        //            Groups.Remove(vv.FullName);
        //        }

        //        grp.Parent = parent;

        //        Groups.Add(grp.FullName,grp);
        //        foreach(var vv in gg)
        //        {
        //            Groups.Add(vv.FullName, vv);
        //        }
        //        IsDirty = true;
        //    }
        //}

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

                string sname = groupName;
                if (parent != null)
                {
                    sname = groupName.Substring(parent.FullName.Length + 1);
                }

                TagGroup tg = new TagGroup() { Parent = parent,Name = sname };
                Groups.Add(tg.FullName, tg);
                IsDirty = true;
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
        public IEnumerable<Tagbase> ListAllTags()
        {
            return Tags.Values;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Tagbase> ListAllRootTags()
        {
            return Tags.Values.Where(e=>e.Parent=="");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<string> ListTagGroups()
        {
            var re = Groups.Keys.ToList();
            re.Add("");
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        public IEnumerable<string> GetTagGroup(string parent)
        {
            
            if (string.IsNullOrEmpty(parent))
            {
                return Groups.Where(e => e.Value.Parent == null).Select(e => e.Value.FullName);
            }
            else
            {
                return Groups.Where(e => e.Value.Parent!=null && e.Value.Parent.FullName==(parent)).Select(e => e.Value.FullName);
            }
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int MaxTagId()
        {
            return MaxId;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public int MinTagId()
        {
            return MinId;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public byte[] SeriseToStream()
        {
            using (var ms = new System.IO.MemoryStream())
            {
                using (System.IO.Compression.GZipStream gs = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionLevel.Optimal))
                {
                    new RealDatabaseSerise() { Database = this }.Save(gs);
                    return ms.GetBuffer();
                }
            }
        }
    }
}
