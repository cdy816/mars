using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    public class ComplexTag : Tagbase
    {
        /// <summary>
        /// 
        /// </summary>
        private SortedDictionary<int, Tagbase> mTags=new SortedDictionary<int, Tagbase>();
        /// <summary>
        /// 
        /// </summary>
        public override TagType Type => TagType.Complex;

        /// <summary>
        /// 
        /// </summary>
        public override int ValueSize => CalValueSize();

        /// <summary>
        /// 关联的复杂类型变量
        /// </summary>
        public string LinkComplexClass { get; set; } = "";

        private int mLastCount = 0;

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
            (tag as ComplexTag).LinkComplexClass = this.LinkComplexClass;
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

        public void Update(Tagbase tag)
        {
            if(Tags.ContainsKey(tag.Id))
            {
                Tags[tag.Id] = tag;
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

            if(tag.Group!=null && tag.FullName.StartsWith(tag.Group+"."))
            {
                tag.FullName=tag.FullName.Substring(tag.Group.Length+1);
            }

            if (tag is ComplexTag)
            {
                foreach (var vv in (tag as ComplexTag).Tags)
                {
                    UpdateFullNameInner(vv.Value, this.FullName);
                }
            }
        }

    }
}
