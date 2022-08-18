using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    public class ComplexClassTag : Tagbase
    {
        /// <summary>
        /// 
        /// </summary>
        private SortedDictionary<int, Tagbase> mTags=new SortedDictionary<int, Tagbase>();

        private SortedDictionary<int, HisTag> mHisTags = new SortedDictionary<int, HisTag>();

        /// <summary>
        /// 
        /// </summary>
        public override TagType Type => TagType.ClassComplex;

        /// <summary>
        /// 
        /// </summary>
        public override int ValueSize => CalValueSize();

        /// <summary>
        /// 关联的复杂类型变量
        /// </summary>
        public string LinkComplexClass { get; set; } = "";


        /// <summary>
        /// 
        /// </summary>
        public SortedDictionary<int,Tagbase> Tags
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
        public SortedDictionary<int, HisTag> HisTags
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

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int CalValueSize()
        {
            int isize = 0;
            foreach(var vv in mTags)
            {
                isize += vv.Value.ValueSize;
            }
            return isize;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public override void CloneTo(Tagbase tag)
        {
            base.CloneTo(tag);
            (tag as ComplexClassTag).LinkComplexClass = this.LinkComplexClass;
        }

        /// <summary>
        /// 
        /// </summary>
        public override void UpdateFullName()
        {
            base.UpdateFullName();
            foreach(var vv in this.Tags)
            {
                UpdateFullNameInner(vv.Value,this.FullName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="parentname"></param>
        private void UpdateFullNameInner(Tagbase tag,string parentname)
        {
            tag.FullName = parentname + "." + tag.Name;

            if (tag is ComplexClassTag)
            {
                foreach (var vv in (tag as ComplexClassTag).Tags)
                {
                    UpdateFullNameInner(vv.Value, this.FullName);
                }
            }
        }

    }
}
