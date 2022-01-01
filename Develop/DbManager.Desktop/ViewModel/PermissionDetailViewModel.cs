//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/3 19:29:26.
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
    public class PermissionDetailViewModel : ViewModelBase, IModeSwitch
    {

        #region ... Variables  ...
        private System.Collections.ObjectModel.ObservableCollection<PermissionItemViewModel> mPermissions = new System.Collections.ObjectModel.ObservableCollection<PermissionItemViewModel>();

        private ICommand mAddCommand;
        private ICommand mRemoveCommand;

        private PermissionItemViewModel mCurrentSelected;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public ICommand AddCommand
        {
            get
            {
                if(mAddCommand==null)
                {
                    mAddCommand = new RelayCommand(() => {
                        Add();
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
                        Remove();
                    },()=> { return CurrentSelected != null && !CurrentSelected.IsSuper; });
                }
                return mRemoveCommand;
            }
        }



        /// <summary>
            /// 
            /// </summary>
        public PermissionItemViewModel CurrentSelected
        {
            get
            {
                return mCurrentSelected;
            }
            set
            {
                if (mCurrentSelected != value)
                {
                    Update();
                    mCurrentSelected = value;
                    OnPropertyChanged("CurrentSelected");
                }
            }
        }


        public ObservableCollection<PermissionItemViewModel> Permissions
        {
            get
            {
                return mPermissions;
            }
            set
            {
                mPermissions = value;
                OnPropertyChanged("Permissions");
            }
        }

        public string Database { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Query()
        {
            var pps = DBDevelopClientApi.DevelopServiceHelper.Helper.GetAllDatabasePermission(Database);
            mPermissions.Clear();
            foreach(var vv in pps)
            {
                var pp = new PermissionItemViewModel(vv) { Parent = this };
                mPermissions.Add(pp);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private string GetAvaiabelName(string name)
        {
            var names = this.Permissions.Select(e => e.Name).ToArray();
            string ss = name;
            for(int i=1;i<int.MaxValue;i++)
            {
                if(!names.Contains(ss))
                {
                    return ss;
                }
                else
                {
                    ss = name + i;
                }
            }
            return name;
        }

        /// <summary>
        /// 
        /// </summary>
        private void Add()
        {
            string sname = GetAvaiabelName("Permission");
            Cdy.Tag.UserPermission pitem = new Cdy.Tag.UserPermission() { Name = sname };
            if (DBDevelopClientApi.DevelopServiceHelper.Helper.UpdateDatabasePermission(this.Database, pitem))
            {
                this.Permissions.Add(new PermissionItemViewModel(pitem) { IsNew = true,Parent=this });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Remove()
        {
            if(DBDevelopClientApi.DevelopServiceHelper.Helper.RemoveDatabasePermission(this.Database,CurrentSelected.Name))
            {
                CurrentSelected.Parent = null;
                mPermissions.Remove(CurrentSelected);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Update()
        {
            if (mCurrentSelected != null && (mCurrentSelected.IsChanged || mCurrentSelected.IsNew))
            {
                mCurrentSelected.IsNew = false;
                mCurrentSelected.IsChanged = false;
                DBDevelopClientApi.DevelopServiceHelper.Helper.UpdateDatabasePermission(this.Database, mCurrentSelected.Model);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Active()
        {
            Query();
        }

        /// <summary>
        /// 
        /// </summary>
        public void DeActive()
        {
            Update();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class PermissionItemViewModel:ViewModelBase
    {

        #region ... Variables  ...

        private Cdy.Tag.UserPermission mModel;

        private string mGroupString;

        private ICommand mGroupEditCommand;

        private bool mIsNew = false;

        private bool mIsSelected = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public PermissionItemViewModel()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mode"></param>
        public PermissionItemViewModel(Cdy.Tag.UserPermission mode)
        {
            mModel = mode;
            MakeGroupString();
        }

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
        public bool IsChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public PermissionDetailViewModel Parent { get; set; }

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
                    OnPropertyChanged("CanNameEdit");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsNameReadOnly
        {
            get
            {
                return !mIsNew || mModel.SuperPermission;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsEnableEdit
        {
            get
            {
                return !mModel.SuperPermission;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public ICommand GroupEditCommand
        {
            get
            {
                if(mGroupEditCommand == null)
                {
                    mGroupEditCommand = new RelayCommand(() => {
                        PermissionGroupManagerViewModel mm = new PermissionGroupManagerViewModel() { Database = Parent.Database };
                        mm.Groups = Group;
                        if(mm.ShowDialog().Value)
                        {
                            Group = mm.Groups;
                        }

                    });
                   
                }
                return mGroupEditCommand;
            }
        }

        

        /// <summary>
        /// 
        /// </summary>
        public Cdy.Tag.UserPermission Model
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
                return mModel.Name;
            }
            set
            {
                mModel.Name = value;
                IsChanged = true;
                OnPropertyChanged("Name");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Desc
        {
            get
            {
                return mModel.Desc;
            }
            set
            {
                mModel.Desc = value;
                OnPropertyChanged("Desc");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsSuper
        {
            get
            {
                return mModel.SuperPermission;
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool EnableWrite
        {
            get
            {
                return mModel.EnableWrite;
            }
            set
            {
                if (mModel.EnableWrite != value)
                {
                    mModel.EnableWrite = value;
                    IsChanged = true;
                    OnPropertyChanged("EnableWrite");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string GroupString
        {
            get
            {
                return mGroupString;
            }
            set
            {
                if (mGroupString != value)
                {
                    mGroupString = value;
                    ParserGroupString();
                    OnPropertyChanged("GroupString");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public List<string> Group
        {
            get
            {
                return Model.Group;
            }
            set
            {
                if (Model.Group != value)
                {
                    Model.Group = value;
                }
                MakeGroupString();
                IsChanged = true;
                OnPropertyChanged("GroupString");
                OnPropertyChanged("Group");
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

       

        private void ParserGroupString()
        {
            Group = mGroupString.Split(new char[] { ',' }).ToList();
        }

        private void MakeGroupString()
        {
            StringBuilder sb = new StringBuilder();
            foreach(var vv in Group)
            {
                sb.Append(vv + ",");
            }
            sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
            mGroupString = sb.ToString();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

}
