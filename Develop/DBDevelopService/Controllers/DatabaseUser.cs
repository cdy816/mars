using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService.Controllers
{
    /// <summary>
    /// 
    /// </summary>
    public class WebApiUserGroup
    {
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Parent { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiUserGroupRequest : WebApiDatabaseRequest
    {
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Parent { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiMoveUserGroupRequest : WebApiDatabaseRequest
    {
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string OldParentName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string NewParentName { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiRenameUserGroupRequest : WebApiDatabaseRequest
    {
        public string NewName { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string OldFullName { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiRequestByUserGroup:WebApiDatabaseRequest
    {
        public string GroupFullName { get; set; }
    }


    /// <summary>
    /// 
    /// </summary>
    public class WebApiNewDatabasePermissionRequest : WebApiDatabaseRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public string Desc { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool EnableWrite { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool SuperPermission { get; set; }

        /// <summary>
        /// 访问的变量
        /// </summary>
        public List<string> Group { get; set; }

    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiRemoveDatabasePermissionRequest : WebApiDatabaseRequest
    {
        public string Permission { get; set; }
    }

    public class WebApiUserRequest : WebApiDatabaseRequest
    {
        public string UserName { get; set; }
    }


    public class WebApiUserAndPassword: WebApiUserRequest
    {
        

        public string Password { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class WebApiUserInfo : WebApiUserAndPassword
    {
        /// <summary>
        /// 
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Permissions { get; set; } = new List<string>();

    }

    public class WebApiUserInfoWithoutPassword : WebApiUserRequest
    {
        /// <summary>
        /// 
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Permissions { get; set; } = new List<string>();

    }

}
