//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/16 11:01:34.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class PermissionDocument:INotifyPropertyChanged
    {

        #region ... Variables  ...
        private bool mIsDirty = false;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...


        public PermissionDocument()
        {
            var superPermission = new UserPermission() { Name = "Super", SuperPermission = true, EnableWrite = true,Desc="Super Permission" };
            Permissions.Add(superPermission.Name, superPermission);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, UserPermission> Permissions { get; set; } = new Dictionary<string, UserPermission>();

        public bool IsDirty { get { return mIsDirty; } set { mIsDirty = value; OnPropertyChanged("IsDirty"); } }

        public event PropertyChangedEventHandler PropertyChanged;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>

        protected void OnPropertyChanged(string name)
        {
            if (PropertyChanged != null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(name));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="permission"></param>
        public void Add(UserPermission permission)
        {
            if(!Permissions.ContainsKey(permission.Name))
            {
                Permissions.Add(permission.Name, permission);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void Remove(string name)
        {
            if(!Permissions.ContainsKey(name))
            {
                Permissions.Remove(name);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
