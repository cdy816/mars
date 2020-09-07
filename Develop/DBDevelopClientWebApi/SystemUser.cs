using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopClientWebApi
{
    public class WebApiNewSystemUserRequest: RequestBase
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        public string UserName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Password { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class WebApiReNameSystemUserRequest : RequestBase
    {
        public string OldName { get; set; }
        public string NewName { get; set; }
    }

    public class WebApiModifySystemUserPasswordRequest : RequestBase
    {
        public string UserName { get; set; }
        public string Password { get; set; }
        public string NewPassword { get; set; }
    }


    public class WebApiUpdateSystemUserRequest : RequestBase
    {
        public string UserName { get; set; }
        public bool IsAdmin { get; set; }
        public bool NewDatabasePermission { get; set; }
        public List<string> Databases { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiSystemUserItem
    {
        public string UserName { get; set; }
        public bool IsAdmin { get; set; }
        public bool NewDatabase { get; set; }
        public List<string> Databases { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiRemoveSystemUserRequest : RequestBase
    {
        public string UserName { get; set; }
    }

}
