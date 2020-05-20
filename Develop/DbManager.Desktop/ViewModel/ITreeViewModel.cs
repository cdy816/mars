//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/4 17:34:05.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace DBInStudio.Desktop
{
    public class TreeItemViewModel: ViewModelBase,ITreeViewModel
    {

        #region ... Variables  ...
        internal string mName="";
        private bool mIsSelected = false;
        private bool mIsExpanded = false;
        private bool mIsEdit;
        private ICommand mRenameCommand;
        private ICommand mRemoveCommand;
        private TreeItemViewModel mParent;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
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
                if (mName != value && !string.IsNullOrEmpty(value))
                {
                    string oldName = mName;
                    if(OnRename(oldName, value))
                    {
                        mName = value;
                    }
                    OnPropertyChanged("Name");
                }
                IsEdit = false;
            }
        }

        public string Database { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public ICommand ReNameCommand
        {
            get
            {
                if (mRenameCommand == null)
                {
                    mRenameCommand = new RelayCommand(() => {
                        IsEdit = true;
                    });
                }
                return mRenameCommand;
            }
        }

        public ICommand ReMoveCommand
        {
            get
            {
                if (mRemoveCommand == null)
                {
                    mRemoveCommand = new RelayCommand(() => {
                        IsEdit = true;
                    });
                }
                return mRemoveCommand;
            }
        }

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
        /// 被选中
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
        /// 展开
        /// </summary>
        public bool IsExpanded
        {
            get
            {
                return mIsExpanded;
            }
            set
            {
                if (mIsExpanded != value)
                {
                    mIsExpanded = value;
                    OnPropertyChanged("IsExpanded");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public TreeItemViewModel Parent
        {
            get
            {
                return mParent;
            }
            set
            {
                if (mParent != value)
                {
                    mParent = value;
                    OnPropertyChanged("Parent");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public virtual string FullName { get { return Parent != null ? Parent.FullName + "." + this.Name : this.Name; } }

        #endregion ...Properties...

        #region ... Methods    ...

        public virtual bool CanAddChild()
        {
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Remove()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public virtual bool CanRemove()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        public virtual bool OnRename(string oldName, string newName)
        {
            return true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    public class HasChildrenTreeItemViewModel:TreeItemViewModel
    {

        #region ... Variables  ...
        private System.Collections.ObjectModel.ObservableCollection<TreeItemViewModel> mChildren = new System.Collections.ObjectModel.ObservableCollection<TreeItemViewModel>();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<TreeItemViewModel> Children
        {
            get
            {
                return mChildren;
            }
        }
        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

         
    public interface ITreeViewModel
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        string Name { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
