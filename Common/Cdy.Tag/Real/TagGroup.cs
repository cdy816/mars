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
        /// 描述
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string FullName { get { return Parent == null ? Name : Parent.FullName + "." + Name; } }

        /// <summary>
        /// 
        /// </summary>
        public List<Tagbase> Tags { get; set; } = new List<Tagbase>();

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
            if(!string.IsNullOrEmpty(group.Description))
            {
                xe.SetAttributeValue("Description", group.Description);
            }
            if(group.Parent!=null)
            xe.SetAttributeValue("Parent", group.Parent.FullName);
            xe.SetAttributeValue("FullName", group.FullName);
            return xe;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="xe"></param>
        ///// <returns></returns>
        //public static TagGroup LoadTagGroupFromXML(this XElement xe)
        //{
        //    TagGroup group = new TagGroup();
        //    group.Name = xe.Attribute("Name").Value;
        //    if (xe.Attribute("Parent") != null)
        //        group.ParentName = xe.Attribute("Parent").Value;
        //    group.FullNameString = xe.Attribute("FullName").Value;
        //    return group;
        //}
    }

}
