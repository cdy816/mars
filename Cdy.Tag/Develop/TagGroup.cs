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
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class TagGroup
    {
        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public TagGroup Parent { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string FullName { get { return Parent != null ? Name : Parent.FullName + "." + Name; } }

        /// <summary>
        /// 用作加载时，建立父子关系
        /// </summary>
        internal string ParentName { get; set; }

        /// <summary>
        /// 用作加载时，表示一个全名称
        /// </summary>
        internal string FullNameString { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<Tagbase> Tags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            if (obj == null||!(obj is TagGroup)) return false;
            return this.FullName == (obj as TagGroup).FullName;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return this.Name.GetHashCode();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public static class TagGroupExtend
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <returns></returns>
        public static XElement SaveToXML(this TagGroup group)
        {
            XElement xe = new XElement("TagGroup");
            xe.SetAttributeValue("Name", group.Name);
            xe.SetAttributeValue("Parent", group.Parent.FullName);
            xe.SetAttributeValue("FullName", group.FullNameString);
            return xe;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        public static TagGroup LoadTagGroupFromXML(this XElement xe)
        {
            TagGroup group = new TagGroup();
            group.Name = xe.Attribute("Name").Value;
            group.ParentName = xe.Attribute("Parent").Value;
            group.FullNameString = xe.Attribute("FullName").Value;
            return group;
        }
    }

}
