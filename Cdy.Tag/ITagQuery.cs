using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public interface ITagQuery
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        int? GetTagIdByName(string name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        List<int?> GetTagIdByName(List<string> name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        Tagbase GetTagByName(string name);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        Tagbase GetTagById(int id);

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        ICollection<Tagbase> ListAllTags();

    }
}
