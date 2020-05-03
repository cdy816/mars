//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/4 18:23:59.
//  Version 1.0
//  种道洋
//==============================================================

using DBDevelopClientApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;

namespace DBInStudio.Desktop
{
    /// <summary>
    /// 
    /// </summary>
    public class SecurityTreeItemViewModel:HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...
        private UserGroupRootViewModel mUserViewModel = new UserGroupRootViewModel();
        private PermissionTreeItemViewModel mPermissionViewModel = new PermissionTreeItemViewModel();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public SecurityTreeItemViewModel()
        {
            Name = Res.Get("Security");
            
        }
        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>

        public void Init()
        {
            Children.Clear();

            Children.Add(mUserViewModel);
            Children.Add(mPermissionViewModel);
            mUserViewModel.Database = this.Database;
            mPermissionViewModel.Database = this.Database;
            QueryGroups();

        }

        private void QueryGroups()
        {
            Application.Current.Dispatcher.Invoke(() => {
                this.mUserViewModel.Children.Clear();
            });

            var vv = DevelopServiceHelper.Helper.QueryDatabaseUserGroups(this.Database);
            if (vv != null)
            {
                foreach (var vvv in vv.Where(e => string.IsNullOrEmpty(e.Value)))
                {
                    Application.Current.Dispatcher.Invoke(() => {
                        UserTreeItemViewModel groupViewModel = new UserTreeItemViewModel() { mName = vvv.Key, Database = Database };
                        mUserViewModel.Children.Add(groupViewModel);
                        groupViewModel.InitData(vv);
                    });
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class UserGroupRootViewModel : UserTreeItemViewModel
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        public UserGroupRootViewModel()
        {
            Name = Res.Get("User");
        }
        #endregion ...Constructor...

        #region ... Properties ...

        public override string FullName => "";

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    /// <summary>
    /// 
    /// </summary>
    public class UserTreeItemViewModel : HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="groups"></param>
        public void InitData(Dictionary<string, string> groups)
        {
            foreach (var vv in groups.Where(e => e.Value == this.FullName))
            {
                UserTreeItemViewModel groupViewModel = new UserTreeItemViewModel() { Name = vv.Key, Database = Database };
                groupViewModel.Parent = this;
                this.Children.Add(groupViewModel);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    public class PermissionTreeItemViewModel : HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public PermissionTreeItemViewModel()
        {
            Name = Res.Get("Permission");
        }
        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


}
