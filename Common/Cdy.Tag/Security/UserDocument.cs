//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/16 10:56:46.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Cdy.Tag
{
    public class UserDocument:INotifyPropertyChanged
    {

        #region ... Variables  ...

        private bool mIsDirty = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        public UserDocument()
        {
            var pp = new UserItem() { Name = "Admin", Password = "Admin", SuperUser = true };
            pp.Permissions.Add("Super");
            Users.Add("Admin", pp);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, UserItem> Users { get; set; } = new Dictionary<string, UserItem>();

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string,UserGroup> Groups { get; set; } = new Dictionary<string, UserGroup>();

        /// <summary>
        /// 
        /// </summary>
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
            if(PropertyChanged!=null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(name));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public UserItem GetUser(string name)
        {
            if (Users.ContainsKey(name))
            {
                return Users[name];
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        public void AddUser(UserItem user)
        {
            if (!Users.ContainsKey(user.Name))
            {
                Users.Add(user.Name, user);
                IsDirty = true;
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void RemoveUser(string name)
        {
            if (Users.ContainsKey(name))
            {
                Users.Remove(name);
                IsDirty = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        public void ModifyUser(UserItem user)
        {
            if (Users.ContainsKey(user.Name))
            {
                Users[user.Name] = user;
                IsDirty = true;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public UserGroup GetUserGroup(string name)
        {
            if (Groups.ContainsKey(name))
            {
                return Groups[name];
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        public void AddUserGroup(UserGroup user)
        {
            if (!Groups.ContainsKey(user.FullName))
            {
                Groups.Add(user.FullName, user);
                IsDirty = true;
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void RemoveUserGroup(string name)
        {
            if (Groups.ContainsKey(name))
            {
                Groups.Remove(name);
                IsDirty = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        public void ModifyUserGroup(UserGroup user)
        {
            if (Groups.ContainsKey(user.FullName))
            {
                Groups[user.FullName] = user;
                IsDirty = true;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
