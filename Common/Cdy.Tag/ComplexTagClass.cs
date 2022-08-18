using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ComplexTagClass
    {
        private string mName = "";
        private string mDescript = "";

        private int mMaxId = 0;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, Tagbase> mTags = new Dictionary<int, Tagbase>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, Tagbase> mNamedTags = new Dictionary<string, Tagbase>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, HisTag> mHisTags = new Dictionary<int, HisTag>();

        /// <summary>
        /// 
        /// </summary>
        public ComplexTagClass()
        {
            Id=Guid.NewGuid().ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        public ComplexTagClassDocument Owner { get; set; }

        public string Id { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public string Name
        {
            get
            {
                return mName;
            }
            set
            {
                if (mName != value)
                {
                    mName = value;
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string Descript
        {
            get
            {
                return mDescript;
            }
            set
            {
                if (mDescript != value)
                {
                    mDescript = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int, Tagbase> Tags
        {
            get
            {
                return mTags;
            }
            set
            {
                mTags = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, Tagbase> NamedTags
        {
            get
            {
                return mNamedTags;
            }
            set
            {
                mNamedTags = value;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public Dictionary<int, HisTag> HisTags
        {
            get
            {
                return mHisTags;
            }
            set
            {
                mHisTags = value;
            }
        }

        private int GetMaxId()
        {
            for(int i=0;i<int.MaxValue;i++)
            {
                if(!mTags.ContainsKey(i))
                {
                    return i;
                }
            }
            return int.MaxValue;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void AppendTag(Tagbase tag)
        {
            tag.Id = GetMaxId();
            AddRealTag(tag);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void AddRealTag(Tagbase tag)
        {
            if(Tags.ContainsKey(tag.Id))
            {
                Tags[tag.Id] = tag;
            }
            else
            {
                Tags.Add(tag.Id, tag);
            }

            if(!mNamedTags.ContainsKey(tag.Name))
            {
                mNamedTags.Add(tag.Name, tag);
            }
            else
            {
                mNamedTags[tag.Name]=tag;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void AddHisTag(HisTag tag)
        {
            if (HisTags.ContainsKey(tag.Id))
            {
                HisTags[tag.Id] = tag;
            }
            else
            {
                HisTags.Add(tag.Id, tag);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public void UpdateRealTag(Tagbase tag)
        {
            if (Tags.ContainsKey(tag.Id))
            {
                var vtag = Tags[tag.Id];

                Tags[tag.Id] = tag;

                if(vtag.Name!=tag.Name)
                {
                    mNamedTags.Remove(vtag.Name);
                    mNamedTags.Add(tag.Name, tag);
                }
                else
                {
                    if(mNamedTags.ContainsKey(tag.Name))
                    {
                        mNamedTags[tag.Name] = tag;
                    }
                }
                //if(tag is ComplexTag)
                //{
                //    FillComplexTag(tag as ComplexTag);
                //}
            }
        }

        private int GetAvaiableId()
        {
            for(int i= mMaxId;i<int.MaxValue;i++)
            {
                if(!Tags.ContainsKey(i))
                {
                    mMaxId = i;
                    return i;
                }
            }
            return 0;
        }

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="tag"></param>
        //private void FillComplexTag(ComplexTag tag)
        //{
        //    string stmp = tag.LinkComplexClass;
        //    if(this.Owner.Class.ContainsKey(stmp))
        //    {
        //        foreach(var vtag in this.Owner.Class[stmp].Tags)
        //        {
        //            var ttag = vtag.Value.Clone();
        //            tag.Tags.Add(ttag.Id, ttag);
        //            if(ttag is ComplexTag)
        //            {
        //                FillComplexTag(vtag.Value as ComplexTag);
        //            }
        //        }
        //    }
        //}


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void RemoveRealTag(int id)
        {
            if (Tags.ContainsKey(id))
            {
                var vtag = Tags[id];
                Tags.Remove(id);
                if(NamedTags.ContainsKey(vtag.Name))
                {
                    NamedTags.Remove(vtag.Name);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void RemoveHisTag(int id)
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
        public void UpdateHisTag(HisTag tag)
        {
            if (HisTags.ContainsKey(tag.Id))
            {
                HisTags[tag.Id] = tag;
            }
            else
            {
                HisTags.Add(tag.Id, tag);
            }
        }
    }

    public static class ComplexTagClassExtend
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static XElement SaveToXML(this ComplexTagClass tag)
        {
            XElement xe = new XElement("ComplexTagClass");
            xe.SetAttributeValue("Name", tag.Name);
            xe.SetAttributeValue("Id", tag.Id);

            XElement xx = new XElement("Real");
            
            foreach(var vv in tag.Tags.Values)
            {
                xx.Add(vv.SaveToXML());
            }
            xe.Add(xx);

            xx = new XElement("His");
            
            foreach(var vv in tag.HisTags.Values)
            {
                xx.Add(vv.SaveToXML());
            }

            xe.Add(xx);
            return xe;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        public static ComplexTagClass LoadComplexTagClassFromXML(this XElement xe)
        {
            ComplexTagClass re = new ComplexTagClass();

            if(xe.Attribute("Name") !=null)
            {
                re.Name = xe.Attribute("Name").Value;
            }

            if (xe.Attribute("Id") != null)
            {
                re.Id = xe.Attribute("Id").Value;
            }

            if (xe.Element("Real") !=null)
            {
                foreach(var vv in xe.Element("Real").Elements())
                {
                    re.AddRealTag(vv.LoadTagFromXML());
                }
            }

            if (xe.Element("His") != null)
            {
                foreach (var vv in xe.Element("His").Elements())
                {
                    re.AddHisTag(vv.LoadHisTagFromXML());
                }
            }

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public static ComplexTagClass Clone(this ComplexTagClass tag)
        {
            return tag.SaveToXML().LoadComplexTagClassFromXML();
        }
    }
}
