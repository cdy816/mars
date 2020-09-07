using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService.Controllers
{
    public class Database
    {
        public string Name { get; set; }
        public string Desc { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class TagGroup
    {
        public string Name { get; set; }

        public string Parent { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiTag
    {
        /// <summary>
        /// 
        /// </summary>
        public WebApiRealTag RealTag { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Cdy.Tag.HisTag HisTag { get; set; }
    }

    public class WebApiRealTag
    {
        public int Id { get; set; }
        public string Name { get; set; }
        public int Type { get; set; }
        public string Group { get; set; }
        public string Desc { get; set; }
        public string LinkAddress { get; set; }
        public int ReadWriteType { get; set; }
        public string Convert { get; set; }
        public double MaxValue { get; set; }
        public double MinValue { get; set; }
        public byte Precision { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="realtag"></param>
        /// <returns></returns>
        public static WebApiRealTag CreatFromTagbase(Tagbase realtag)
        {
            WebApiRealTag tag = new WebApiRealTag();
            tag.CloneFromRealTag(realtag);
            return tag;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="realtag"></param>
        public void CloneFromRealTag(Tagbase realtag)
        {
            this.Id = realtag.Id;
            this.Name = realtag.Name;
            this.Type = (byte)realtag.Type;
            this.Group = realtag.Group;
            this.Desc = realtag.Desc;
            this.LinkAddress = realtag.LinkAddress;
            this.ReadWriteType = (int)realtag.ReadWriteType;
            this.Convert = realtag.Conveter != null ? realtag.Conveter.SeriseToString() : "";
            if(realtag is NumberTagBase)
            {
                this.MaxValue = (realtag as NumberTagBase).MaxValue;
                this.MinValue = (realtag as NumberTagBase).MinValue;
            }
            if(realtag is FloatingTagBase)
            {
                this.Precision = (realtag as FloatingTagBase).Precision;
            }
        }

        public Tagbase ConvertToTagbase()
        {
            Cdy.Tag.Tagbase re = null;
            switch (this.Type)
            {
                case (int)(Cdy.Tag.TagType.Bool):
                    re = new Cdy.Tag.BoolTag();
                    break;
                case (int)(Cdy.Tag.TagType.Byte):
                    re = new Cdy.Tag.ByteTag();
                    break;
                case (int)(Cdy.Tag.TagType.DateTime):
                    re = new Cdy.Tag.DateTimeTag();
                    break;
                case (int)(Cdy.Tag.TagType.Double):
                    re = new Cdy.Tag.DoubleTag();
                    break;
                case (int)(Cdy.Tag.TagType.Float):
                    re = new Cdy.Tag.FloatTag();
                    break;
                case (int)(Cdy.Tag.TagType.Int):
                    re = new Cdy.Tag.IntTag();
                    break;
                case (int)(Cdy.Tag.TagType.UInt):
                    re = new Cdy.Tag.UIntTag();
                    break;
                case (int)(Cdy.Tag.TagType.ULong):
                    re = new Cdy.Tag.ULongTag();
                    break;
                case (int)(Cdy.Tag.TagType.UShort):
                    re = new Cdy.Tag.UShortTag();
                    break;
                case (int)(Cdy.Tag.TagType.Long):
                    re = new Cdy.Tag.LongTag();
                    break;
                case (int)(Cdy.Tag.TagType.Short):
                    re = new Cdy.Tag.ShortTag();
                    break;
                case (int)(Cdy.Tag.TagType.String):
                    re = new Cdy.Tag.StringTag();
                    break;
            }
            if (re != null)
            {
                re.Name = this.Name;
                re.LinkAddress = this.LinkAddress;
                re.Group = this.Group;
                re.Desc = this.Desc;
                re.Id = (int)this.Id;
                re.ReadWriteType = (Cdy.Tag.ReadWriteMode)this.ReadWriteType;
                if (!string.IsNullOrEmpty(this.Convert))
                {
                    re.Conveter = this.Convert.DeSeriseToValueConvert();
                }
                if (re is Cdy.Tag.NumberTagBase)
                {
                    (re as Cdy.Tag.NumberTagBase).MaxValue = this.MaxValue;
                    (re as Cdy.Tag.NumberTagBase).MinValue = this.MinValue;
                }

                if (re is Cdy.Tag.FloatingTagBase)
                {
                    (re as Cdy.Tag.FloatingTagBase).Precision = (byte)this.Precision;
                }
            }

            return re;
        }


    }



}
