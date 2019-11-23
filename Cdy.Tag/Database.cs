using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class Database
    {
        /// <summary>
        /// 
        /// </summary>
        public Database()
        {
            Tags = new Dictionary<long, Tagbase>();
        }

        /// <summary>
        /// 当前最大ID
        /// </summary>
        public long MaxId { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<long,Tagbase> Tags { get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        public bool Add(Tagbase tag)
        {
            if(!Tags.ContainsKey(tag.Id))
            {
                Tags.Add(tag.Id, tag);
                return true;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void AddOrUpdate(Tagbase tag)
        {
            if (!Tags.ContainsKey(tag.Id))
            {
                Tags.Add(tag.Id, tag);
            }
            else
            {
                Tags[tag.Id] = tag;
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
        public bool Remove(long id)
        {
            if(Tags.ContainsKey(id))
            {
                Tags.Remove(id);
            }
            return false;
        }

    }
}
