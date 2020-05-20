//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/1 22:07:12.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Windows.Input;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class UserGroupDetailViewModel : ViewModelBase
    {

        #region ... Variables  ...
        
        private UserTreeItemViewModel mModel;

        private ObservableCollection<UserItemViewModel> mUsers = new ObservableCollection<UserItemViewModel>();

        private UserItemViewModel mCurrentSelectedUser;

        private ICommand mAddCommand;

        private ICommand mRemoveCommand;

        private List<string> mPermissionCach = null;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public UserGroupDetailViewModel()
        {

        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public ICommand AddCommand
        {
            get
            {
                if(mAddCommand==null)
                {
                    mAddCommand = new RelayCommand(() => {
                        AddUser();
                    });
                }
                return mAddCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand RemoveCommand
        {
            get
            {
                if(mRemoveCommand==null)
                {
                    mRemoveCommand = new RelayCommand(() => {
                        RemoveUser();
                    },()=> { return mCurrentSelectedUser != null; });
                }
                return mRemoveCommand;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public UserItemViewModel CurrentSelectedUser
        {
            get
            {
                return mCurrentSelectedUser;
            }
            set
            {
                if (mCurrentSelectedUser != value)
                {
                    if (mCurrentSelectedUser != null)
                    {
                        mCurrentSelectedUser.CheckPermission();
                        mCurrentSelectedUser.Update();
                    }
                    mCurrentSelectedUser = value;
                    OnPropertyChanged("CurrentSelectedUser");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public ObservableCollection<UserItemViewModel> Users
        {
            get
            {
                return mUsers;
            }
            set
            {
                mUsers = value;
                OnPropertyChanged("Users");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public UserTreeItemViewModel Model
        {
            get
            {
                return mModel;
            }
            set
            {
                if (mModel != value)
                {
                    mModel = value;
                    OnPropertyChanged("Model");
                    mPermissionCach = null;
                    System.Threading.Tasks.Task.Run(() => {
                        QueryUsers();
                    });
                }
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public string GetAvailableName(string name)
        {
            var users = DBDevelopClientApi.DevelopServiceHelper.Helper.GetAllUserNames(this.Model.Database);
            string sname = name;
            for(int i=1;i<int.MaxValue;i++)
            {
                if (!users.Contains(sname)) return sname;
                else
                {
                    sname = name + i;
                }
            }
            return name;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool CheckNameAvaiable(string name)
        {
            var users = DBDevelopClientApi.DevelopServiceHelper.Helper.GetAllUserNames(this.Model.Database);
            return !users.Contains(name);
        }

        /// <summary>
        /// 
        /// </summary>
        private void AddUser()
        {
            string newUserName = GetAvailableName("user");
            Cdy.Tag.UserItem user = new Cdy.Tag.UserItem() { Name = newUserName, Group = this.Model.FullName };
            var umode = new UserItemViewModel() { Model = user,IsNew=true,IsEdit=true,ParentModel=this };
            
            umode.IntPermission(mPermissionCach);
            Users.Add(umode);

            CurrentSelectedUser = umode;
        }

        /// <summary>
        /// 
        /// </summary>
        private void RemoveUser()
        {
            mCurrentSelectedUser.ParentModel = null;

            int id = Users.IndexOf(mCurrentSelectedUser);
            if(!mCurrentSelectedUser.IsNew)
            {
                if(DBDevelopClientApi.DevelopServiceHelper.Helper.RemoveDatabaseUser(Model.Database,mCurrentSelectedUser.Name))
                {
                    Users.Remove(mCurrentSelectedUser);
                }
            }
            else
            {
                Users.Remove(mCurrentSelectedUser);
            }

           
            mCurrentSelectedUser = null;

            if (Users.Count > id) CurrentSelectedUser = Users[id];

            else CurrentSelectedUser = Users[Users.Count - 1];


        }

        /// <summary>
        /// 
        /// </summary>
        public void QueryUsers()
        {
            ObservableCollection<UserItemViewModel> utmp = new ObservableCollection<UserItemViewModel>();
            var users = DBDevelopClientApi.DevelopServiceHelper.Helper.GetUsersByGroup(this.Model.Database, this.Model.FullName);
            if (users != null)
            {
                foreach (var vv in users)
                    utmp.Add(new UserItemViewModel() { Model = vv,ParentModel=this });
            }
            Users = utmp;

            if (mPermissionCach == null)
                mPermissionCach = DBDevelopClientApi.DevelopServiceHelper.Helper.GetAllDatabasePermission(this.Model.Database).Select(e => e.Name).ToList();

            foreach (var vv in Users)
            {
                vv.IntPermission(mPermissionCach);
            }

            if (Users.Count > 0) CurrentSelectedUser = Users[0];
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class UserItemViewModel : ViewModelBase
    {

        #region ... Variables  ...

        private List<PermissionItemModel> mPermissionModel;

        private bool mIsNew = false;

        private bool mIsEdit = false;


        private bool mIsPasswordChanged = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
            /// 
            /// </summary>
        public bool IsEdit
        {
            get
            {
                return mIsEdit;
            }
            set
            {
                if (mIsEdit != value)
                {
                    mIsEdit = value;
                    OnPropertyChanged("IsEdit");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool IsNew
        {
            get
            {
                return mIsNew;
            }
            set
            {
                if (mIsNew != value)
                {
                    mIsNew = value;
                    OnPropertyChanged("IsNew");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public List<PermissionItemModel> PermissionModel
        {
            get
            {
                return mPermissionModel;
            }
            set
            {
                if (mPermissionModel != value)
                {
                    mPermissionModel = value;
                    OnPropertyChanged("PermissionModel");
                }
            }
        }



        public Cdy.Tag.UserItem Model { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Name
        {
            get
            {
                return Model.Name;
            }
            set
            {
                if (Model.Name!=value && ParentModel.CheckNameAvaiable(value))
                {
                    Model.Name = value;
                }
                if(IsEdit)
                {
                    IsEdit = false;
                    Update();
                }
                
                OnPropertyChanged("Name");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Password
        {
            get
            {
                return Model.Password;
            }
            set
            {
                mIsPasswordChanged = true;
                Model.Password = value;
                OnPropertyChanged("Password");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Permissions
        {
            get
            {
                return Model.Permissions;
            }
            set
            {
                if(Model.Permissions!=value)
                {
                    Model.Permissions = value;
                }
                OnPropertyChanged("Permissions");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public UserGroupDetailViewModel ParentModel { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void IntPermission(List<string> allpermission)
        {
            List<PermissionItemModel> ptmp = new List<PermissionItemModel>();
            foreach (var vv in allpermission)
            {
                PermissionItemModel pm = new PermissionItemModel() { Name = vv };
                pm.IsSelected = this.Permissions.Contains(vv);
                ptmp.Add(pm);
            }
            PermissionModel = ptmp;
        }

        /// <summary>
        /// 
        /// </summary>
        public void CheckPermission()
        {
           if(this.PermissionModel!=null)
            this.Permissions = this.PermissionModel.Where(e => e.IsSelected).Select(e => e.Name).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Update()
        {
            if (this.ParentModel == null) return;

            DBDevelopClientApi.DevelopServiceHelper.Helper.UpdateDatabaseUser(this.ParentModel.Model.Database, this.Model);
            IsNew = false;
            if (mIsPasswordChanged)
            {
                mIsPasswordChanged = false;
                DBDevelopClientApi.DevelopServiceHelper.Helper.UpdateDatabaseUserPassword(this.ParentModel.Model.Database, this.Name, this.Model.Password);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class PermissionItemModel:ViewModelBase
    {

        #region ... Variables  ...
        private bool mIsSelected = false;
        private string mName = "";
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public bool IsSelected
        {
            get
            {
                return mIsSelected;
            }
            set
            {
                if (mIsSelected != value)
                {
                    mIsSelected = value;
                    OnPropertyChanged("IsSelected");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string Name
        {
            get
            {
                return mName;
            }
            set
            {
                if (mName != value)
                {
                    mName = value;
                    OnPropertyChanged("Name");
                }
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


}
