//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 读写模式
    /// </summary>
    public enum ReadWriteMode
    {
        ReadWrite,
        Write,
        Read
    }

    /// <summary>
    /// 
    /// </summary>
    public abstract class Tagbase
    {
        private string mFullName;
        private string mGroup="";
        /// <summary>
        /// 编号
        /// </summary>
        public int Id { get; set; } = -1;

        /// <summary>
        /// 类型
        /// </summary>
        public abstract TagType Type { get; }

        /// <summary>
        /// 名称
        /// </summary>
        public string Name { get; set; } = "";




        /// <summary>
        /// 
        /// </summary>
        public string FullName
        {
            get { return mFullName; }
            set { mFullName = value; }
        }

        /// <summary>
        /// 组
        /// </summary>
        public string Group { get { return mGroup; } set { mGroup = value; UpdateFullName(); } }

        /// <summary>
        /// 
        /// </summary>
        public string Parent { get; set; } = "";

        /// <summary>
        /// 描述
        /// </summary>
        public string Desc { get; set; } = "";

        /// <summary>
        /// 区域，同一个区域内的变量具有相同的特性
        /// </summary>
        public string Area { get; set; } = "";

        /// <summary>
        /// 单位
        /// </summary>
        public string Unit { get; set; } = "";

        /// <summary>
        /// 运行状态，仅用作运行时
        /// </summary>
        public short State { get; set; }

        /// <summary>
        /// 扩展字段1
        /// </summary>
        public string ExtendField1 { get; set; } = "";

        /// <summary>
        /// 数字扩展字段2，仅用作运行时
        /// </summary>
        public long ExtendField2 { get; set; }

        /// <summary>
        /// 外部管理IO的地址
        /// </summary>
        public string LinkAddress { get; set; } = "";

        /// <summary>
        /// 
        /// </summary>
        public long ValueAddress { get; set; }

        /// <summary>
        /// 指的大小
        /// </summary>
        public abstract  int ValueSize { get; }

        /// <summary>
        /// 值转换函数
        /// </summary>
        public IValueConvert Conveter { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ReadWriteMode ReadWriteType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public virtual void UpdateFullName()
        {
            if (string.IsNullOrEmpty(Parent))
            {
                if (string.IsNullOrEmpty(Group))
                {
                    mFullName = Name;
                }
                else
                {
                    mFullName = Group + "." + Name;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            var target = obj as Tagbase;
            if (target == null) return false;
            return this.Name == target.Name && target.Area == target.Area && target.FullName == target.FullName && target.Group == this.Group && target.Desc == this.Desc && target.LinkAddress == this.LinkAddress && target.Conveter == this.Conveter && target.ReadWriteType == this.ReadWriteType && this.Id == target.Id && this.Type == target.Type && this.Unit==target.Unit && this.ExtendField1==target.ExtendField1&&this.ExtendField2==target.ExtendField2&&this.State == target.State;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public virtual void CloneTo(Tagbase tag)
        {
            tag.Name = this.Name;
            tag.Id=this.Id;
            tag.Conveter = this.Conveter;
            tag.ReadWriteType = this.ReadWriteType;
            tag.ValueAddress = this.ValueAddress;
            tag.Desc = this.Desc;
            tag.Group=this.Group;
            tag.LinkAddress = this.LinkAddress;
            tag.Unit = this.Unit;
            tag.ExtendField1 = this.ExtendField1;
            tag.ExtendField2 = this.ExtendField2;
            tag.State = this.State;
            tag.Area=this.Area;
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public abstract class NumberTagBase : Tagbase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public NumberTagBase()
        {
            MaxValue = AllowMaxValue;
            MinValue = AllowMinValue;
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public abstract double AllowMaxValue { get; }

        /// <summary>
        /// 
        /// </summary>
        public abstract double AllowMinValue { get; }

        /// <summary>
        /// 最大值
        /// </summary>
        public double MaxValue { get; set; }

        /// <summary>
        /// 最小值
        /// </summary>
        public double MinValue { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public override void CloneTo(Tagbase tag)
        {
            base.CloneTo(tag);
            (tag as NumberTagBase).MaxValue = this.MaxValue;
            (tag as NumberTagBase).MinValue = this.MinValue;
        }


        public override bool Equals(object obj)
        {
            var target = obj as NumberTagBase;
            if (target == null) return false;
            return base.Equals(obj) &&  this.MaxValue == target.MaxValue && this.MinValue == target.MinValue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public abstract class FloatingTagBase : NumberTagBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 小数位数
        /// </summary>
        public byte Precision { get; set; } = 2;
        #endregion ...Properties...

        #region ... Methods    ...

        public override void CloneTo(Tagbase tag)
        {
            base.CloneTo(tag);
            (tag as FloatingTagBase).Precision = this.Precision;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public override bool Equals(object obj)
        {
            FloatingTagBase target = obj as FloatingTagBase;
            if (target == null) return false;
            return base.Equals(obj) && target.Precision == this.Precision;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override int GetHashCode()
        {
            return base.GetHashCode();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 
    /// </summary>
    public static class TagExtends
    {
        public static XElement SaveToXML(this Tagbase tag)
        {
            XElement xe = new XElement("Tag");
            xe.SetAttributeValue("Id", tag.Id);
            xe.SetAttributeValue("Name", tag.Name);
            xe.SetAttributeValue("Type", (int)tag.Type);
            xe.SetAttributeValue("Group", tag.Group);
            xe.SetAttributeValue("Desc", tag.Desc);
            xe.SetAttributeValue("Parent", tag.Parent);
            xe.SetAttributeValue("Area", tag.Area);

            if (!string.IsNullOrEmpty(tag.FullName))
            xe.SetAttributeValue("FullName", tag.FullName);

            if(!tag.LinkAddress.Contains(":"))
            {
                tag.LinkAddress=tag.LinkAddress+":";
            }

            if(!string.IsNullOrEmpty(tag.ExtendField1))
            {
                xe.SetAttributeValue("ExtendField1", tag.ExtendField1);
            }

            if (!string.IsNullOrEmpty(tag.Unit))
            {
                xe.SetAttributeValue("Unit", tag.Unit);
            }


            //xe.SetAttributeValue("ExtendField2", tag.ExtendField2);

            xe.SetAttributeValue("LinkAddress", tag.LinkAddress);
            xe.SetAttributeValue("ReadWriteType", (int)tag.ReadWriteType);
            if(tag.Conveter!=null)
            {
                xe.SetAttributeValue("Conveter", tag.Conveter.Name+":"+ tag.Conveter.SaveToString());
            }
            if(tag is NumberTagBase)
            {
                xe.SetAttributeValue("MaxValue", (tag as NumberTagBase).MaxValue);
                xe.SetAttributeValue("MinValue", (tag as NumberTagBase).MinValue);
            }
            if(tag is FloatingTagBase)
            {
                xe.SetAttributeValue("Precision", (tag as FloatingTagBase).Precision);
            }

            if(tag is ComplexTag)
            {
                foreach(var vv in (tag as ComplexTag).Tags)
                {
                    xe.Add(vv.Value.SaveToXML());
                }
                xe.SetAttributeValue("LinkComplexClass", (tag as ComplexTag).LinkComplexClass);
            }

            //if (tag is ComplexClassTag)
            //{
            //    XElement real = new XElement("Real");
            //    foreach (var vv in (tag as ComplexClassTag).Tags)
            //    {
            //        real.Add(vv.Value.SaveToXML());
            //    }
            //    xe.Add(real);

            //    XElement his = new XElement("His");
            //    foreach (var vv in (tag as ComplexClassTag).HisTags)
            //    {
            //        real.Add(vv.Value.SaveToXML());
            //    }
            //    xe.Add(his);

            //    xe.SetAttributeValue("LinkComplexClass", (tag as ComplexTag).LinkComplexClass);
            //}

            return xe;
        }

        public static Tagbase LoadTagFromXML(this XElement xe)
        {
            TagType tp = (TagType)int.Parse(xe.Attribute("Type").Value);
            Tagbase re = null;

            switch (tp)
            {
                case TagType.Bool:
                    re = new BoolTag();
                    break;
                case TagType.Byte:
                    re = new ByteTag();
                    break;
                case TagType.Short:
                    re = new ShortTag();
                    break;
                case TagType.UShort:
                    re = new UShortTag();
                    break;
                case TagType.Int:
                    re = new IntTag();
                    break;
                case TagType.UInt:
                    re = new UIntTag();
                    break;
                case TagType.Long:
                    re = new LongTag();
                    break;
                case TagType.ULong:
                    re = new ULongTag();
                    break;
                case TagType.Float:
                    re = new FloatTag();
                    break;
                case TagType.Double:
                    re = new DoubleTag();
                    break;
                case TagType.String:
                    re = new StringTag();
                    break;
                case TagType.DateTime:
                    re = new DateTimeTag();
                    break;
                case TagType.IntPoint:
                    re = new IntPointTag();
                    break;
                case TagType.UIntPoint:
                    re = new UIntPointTag();
                    break;
                case TagType.LongPoint:
                    re = new LongPointTag();
                    break;
                case TagType.ULongPoint:
                    re = new ULongPointTag();
                    break;
                case TagType.IntPoint3:
                    re = new IntPoint3Tag();
                    break;
                case TagType.UIntPoint3:
                    re = new UIntPoint3Tag();
                    break;
                case TagType.LongPoint3:
                    re = new LongPoint3Tag();
                    break;
                case TagType.ULongPoint3:
                    re = new ULongPoint3Tag();
                    break;
                case TagType.Complex:
                    re = new ComplexTag();
                    break;
                //case TagType.ClassComplex:
                //    re = new ComplexClassTag();
                //    break;
            }

            re.Id = int.Parse(xe.Attribute("Id").Value);
            re.Name = xe.Attribute("Name").Value;
            re.Parent = xe.Attribute("Parent") != null ? xe.Attribute("Parent").Value : "";
            re.Group = xe.Attribute("Group")!=null? xe.Attribute("Group").Value:"";
            re.Desc = xe.Attribute("Desc") != null ? xe.Attribute("Desc").Value : "";
            re.LinkAddress = xe.Attribute("LinkAddress") != null ? xe.Attribute("LinkAddress").Value : "";
            re.Area = xe.Attribute("Area") != null ? xe.Attribute("Area").Value : "";

            re.FullName = xe.Attribute("FullName") != null ? xe.Attribute("FullName").Value : re.FullName;

            re.Unit = xe.Attribute("Unit") != null ? xe.Attribute("Unit").Value : "";
            re.ExtendField1 = xe.Attribute("ExtendField1") != null ? xe.Attribute("ExtendField1").Value : "";

            if (xe.Attribute("Conveter") !=null)
            {
                var vres= xe.Attribute("Conveter").Value;
                string[] sval = vres.Split(new char[] { ':' });
                var vtmp = ValueConvertManager.manager.GetConvert(sval[0]);
                if(vtmp!=null)
                {
                    re.Conveter = vtmp.LoadFromString(vres.Replace(sval[0]+":",""));
                }
            }

            if (xe.Attribute("ReadWriteType") != null)
            {
                re.ReadWriteType = (ReadWriteMode) int.Parse(xe.Attribute("ReadWriteType").Value);
            }

            if (re is NumberTagBase)
            {
                if (xe.Attribute("MaxValue") != null)
                {
                    (re as NumberTagBase).MaxValue = double.Parse(xe.Attribute("MaxValue").Value);
                }
                if (xe.Attribute("MinValue") != null)
                {
                    (re as NumberTagBase).MinValue = double.Parse(xe.Attribute("MinValue").Value);
                }
            }
            if (re is FloatingTagBase)
            {
                if (xe.Attribute("Precision") != null)
                {
                    (re as FloatingTagBase).Precision = byte.Parse(xe.Attribute("Precision").Value);
                }
            }

            if (re is ComplexTag)
            {
                var ctag = re as ComplexTag;
                foreach (var vv in xe.Elements())
                {
                    var vtag = vv.LoadTagFromXML();
                    if (!ctag.Tags.ContainsKey(vtag.Id))
                        ctag.Tags.Add(vtag.Id, vtag);
                }

                if(xe.Attribute("LinkComplexClass")!=null)
                {
                    ctag.LinkComplexClass = xe.Attribute("LinkComplexClass").Value;
                }
            }

            //if(re is ComplexClassTag)
            //{
            //    var ctag = re as ComplexClassTag;
            //    if (xe.Attribute("LinkComplexClass") != null)
            //    {
            //        ctag.LinkComplexClass = xe.Attribute("LinkComplexClass").Value;
            //    }
            //    if(xe.Element("Real") !=null)
            //    {
            //        foreach (var vv in xe.Element("Real").Elements())
            //        {
            //            var vtag = vv.LoadTagFromXML();
            //            if (!ctag.Tags.ContainsKey(vtag.Id))
            //                ctag.Tags.Add(vtag.Id, vtag);
            //        }

            //        foreach (var vv in xe.Element("His").Elements())
            //        {
            //            var vtag = vv.LoadHisTagFromXML();
            //            if (!ctag.HisTags.ContainsKey(vtag.Id))
            //                ctag.HisTags.Add(vtag.Id, vtag);
            //        }
            //    }
            //}

            return re;
        }

        public static Tagbase Clone(this Tagbase tag)
        {
            return tag.SaveToXML().LoadTagFromXML();
        }
    }

}
