using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBInStudio.Desktop
{
    public class ServerHelper
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static ServerHelper Helper = new ServerHelper();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        public string Server { get; set; }

        public string UserName { get; set; }

        public string Password { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Database { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
