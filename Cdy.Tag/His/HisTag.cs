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
    /// 历史记录
    /// </summary>
    public class HisTag
    {
        /// <summary>
        /// Tag Id
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// 记录类型
        /// </summary>
        public RecordType Type { get; set; }

        /// <summary>
        /// 变量类型
        /// </summary>
        public TagType TagType { get; set; }

        /// <summary>
        /// 压缩算法
        /// </summary>
        public int CompressType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, double> Parameters { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public float CompressParameter1 { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //public float CompressParameter2 { get; set; }


        ///// <summary>
        ///// 
        ///// </summary>
        //public float CompressParameter3 { get; set; }

        /// <summary>
        /// 定时记录周期,ms
        /// </summary>
        public long Circle { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public static class HisTagExtends
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static XElement SaveToXML(this HisTag tag)
        {
            XElement xe = new XElement("HisTag");
            xe.SetAttributeValue("Id", tag.Id);
            xe.SetAttributeValue("Type", (int)tag.Type);
            xe.SetAttributeValue("TagType", (int)tag.TagType);
            xe.SetAttributeValue("Circle", tag.Circle);
            xe.SetAttributeValue("CompressType", tag.CompressType);

            if (tag.Parameters != null && tag.Parameters.Count > 0)
            {
                XElement para = new XElement("Parameters");
                foreach(var vv in tag.Parameters)
                {
                    var vpp = new XElement("ParameterItem");
                    vpp.SetAttributeValue("Name", vv.Key.ToString());
                    vpp.SetAttributeValue("Value", vv.Value.ToString());
                    para.Add(vpp);
                }
                xe.Add(para);
            }
            //xe.SetAttributeValue("CompressParameter1", tag.CompressParameter1);
            //xe.SetAttributeValue("CompressParameter2", tag.CompressParameter2);
            //xe.SetAttributeValue("CompressParameter3", tag.CompressParameter3);
            return xe;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        public static HisTag LoadHisTagFromXML(this XElement xe)
        {
            HisTag hisTag = new HisTag();
            hisTag.Id = int.Parse(xe.Attribute("Id").Value);
            hisTag.Circle = long.Parse(xe.Attribute("Circle").Value);
            hisTag.Type = (RecordType)(int.Parse(xe.Attribute("Type").Value));
            hisTag.TagType = (TagType)(int.Parse(xe.Attribute("TagType").Value));
            hisTag.CompressType = int.Parse(xe.Attribute("CompressType").Value);

            if(xe.Element("Parameters") !=null)
            {
                Dictionary<string, double> dvals = new Dictionary<string, double>();
                foreach(var vv in xe.Element("Parameters").Elements())
                {
                    string skey = vv.Attribute("Name").Value;
                    double dval = Convert.ToDouble(vv.Attribute("Value").Value);
                    if(!dvals.ContainsKey(skey))
                    {
                        dvals.Add(skey, dval);
                    }
                }
                hisTag.Parameters = dvals;
            }

            //hisTag.CompressParameter1 = float.Parse(xe.Attribute("CompressParameter1").Value);
            //hisTag.CompressParameter2 = float.Parse(xe.Attribute("CompressParameter2").Value);
            //hisTag.CompressParameter3 = float.Parse(xe.Attribute("CompressParameter3").Value);
            return hisTag;
        }
    }


}
