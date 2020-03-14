using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    public class Database
    {

        /// <summary>
        /// 
        /// </summary>
        public Database()
        {
            Setting = new SettingDoc();
            Security = new SecurityDocument();
        }

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Version { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public RealDatabase RealDatabase { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public HisDatabase HisDatabase { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public SettingDoc Setting { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public SecurityDocument Security { get; set; }

    }
}
