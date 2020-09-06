using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopClientWebApi
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
        public Cdy.Tag.Tagbase RealTag { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Cdy.Tag.HisTag HisTag { get; set; }
    }
}
