using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService
{
    /// <summary>
    /// 
    /// </summary>
    public class PermissionDocument
    {

        #region ... Variables  ...

        public const string NewPermission = "new";
        public const string DeletePermission = "delete";
        public const string ModifyPermission = "modify";
        public const string AdminPermission = "admin";

        private Dictionary<string, Permission> mPermissions = new Dictionary<string, Permission>();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public PermissionDocument()
        {
            InitInnerPermission();
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, Permission> Permissions
        {
            get
            {
                return mPermissions;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void InitInnerPermission()
        {
            mPermissions.Add(NewPermission, new Permission() { Name = NewPermission });
            mPermissions.Add(DeletePermission, new Permission() { Name = DeletePermission });
            //mPermissions.Add(ModifyPermission, new Permission() { Name = ModifyPermission });
            mPermissions.Add(AdminPermission, new Permission() { Name = AdminPermission, IsAdmin = true });
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
