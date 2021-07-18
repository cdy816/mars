//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/6/11 16:18:22.
//  Version 1.0
//  种道洋
//==============================================================

using DBDevelopClientApi;
using DBInStudio.Desktop.ViewModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Input;

namespace DBInStudio.Desktop
{
    /// <summary>
    /// 
    /// </summary>
    public class TagGroupViewModel : HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...

        internal string mDescription;

        private ICommand mSetDescriptionCommand;

        private bool mIsDescriptionEdit = false;

        public static TagGroupViewModel CopyTarget { get; set; }
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        public TagGroupViewModel()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="description"></param>
        public TagGroupViewModel(string name, string description)
        {
            mDescription = description;
            Name = name;
        }
        #endregion ...Constructor...

        #region ... Properties ...

        public ICommand SetDescriptionCommand
        {
            get
            {
                if(mSetDescriptionCommand==null)
                {
                    mSetDescriptionCommand = new RelayCommand(() => {
                        IsDescriptionEdit = true;
                    });
                }
                return mSetDescriptionCommand;
            }
        }

        public override string FullName => (Parent!=null && !(Parent is RootTagGroupViewModel)) ? (Parent as TagGroupViewModel).FullName + "." + Name : Name;

        /// <summary>
        /// 
        /// </summary>
        public string Description
        {
            get
            {
                return mDescription;
            }
            set
            {
                if (mDescription != value)
                {
                    if (DevelopServiceHelper.Helper.UpdateGroupDescription(this.Database, this.FullName, value))
                    {
                        mDescription = value;
                        IsDescriptionEdit = false;
                    }
                    OnPropertyChanged("Description");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool IsDescriptionEdit
        {
            get
            {
                return mIsDescriptionEdit;
            }
            set
            {
                if (mIsDescriptionEdit != value)
                {

                    mIsDescriptionEdit = value;
                    OnPropertyChanged("IsDescriptionEdit");
                }
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override ViewModelBase GetModel(ViewModelBase mode)
        {
            if(mode is TagGroupDetailViewModel)
            {
                (mode as TagGroupDetailViewModel).GroupModel = this;
                return mode;
            }
            else
            {
                return new TagGroupDetailViewModel() { GroupModel = this };
            }
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanPaste()
        {
            return CopyTarget != null && !(this.FullName.Contains(CopyTarget.FullName) || (CopyTarget.FullName.Contains(this.FullName) && !string.IsNullOrEmpty(this.FullName)));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanCopy()
        {
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanAddChild()
        {
            return true;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanRemove()
        {
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Remove()
        {
            string sname = this.FullName;
            if (MessageBox.Show(string.Format(Res.Get("RemoveConfirmMsg"), sname), "", MessageBoxButton.YesNo) == MessageBoxResult.Yes)
            {
                if (DevelopServiceHelper.Helper.RemoveTagGroup(Database, sname))
                {
                    if (Parent != null && Parent is HasChildrenTreeItemViewModel)
                    {
                        (Parent as HasChildrenTreeItemViewModel).Children.Remove(this);
                        Parent = null;
                    }
                    if (CopyTarget == this)
                        CopyTarget = null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetNewGroupName()
        {
            List<string> vtmps = Children.Select(e => e.Name).ToList();
            string tagName = "group";
            for (int i = 1; i < int.MaxValue; i++)
            {
                tagName = "group" + i;
                if (!vtmps.Contains(tagName))
                {
                    return tagName;
                }
            }
            return tagName;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        /// <returns></returns>
        public bool AddGroup(string parent)
        {
            string chileName = GetNewGroupName();
            chileName = DevelopServiceHelper.Helper.AddTagGroup(this.Database, chileName, parent);
            if (!string.IsNullOrEmpty(chileName))
            {
                var vmm = new TagGroupViewModel() { mName = chileName, Database = this.Database, Parent = this };
                this.Children.Add(vmm);
                
                if (!this.IsExpanded) this.IsExpanded = true;
                vmm.IsSelected = true;
                vmm.IsEdit = true;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Add()
        {
            AddGroup(this.FullName);
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Copy()
        {
            CopyTarget = this;
        }


        /// <summary>
        /// 
        /// </summary>
        public override void Paste()
        {
            string sgroup = DevelopServiceHelper.Helper.PasteTagGroup(Database, CopyTarget.FullName, FullName);
            if (!string.IsNullOrEmpty(sgroup))
            {
                this.Children.Add(new TagGroupViewModel() { Database = this.Database, mName = sgroup,mDescription="" });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public override bool OnRename(string oldName, string newName)
        {
            return DevelopServiceHelper.Helper.ReNameTagGroup(Database, this.FullName, newName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="groups"></param>
        public void InitData(List<Tuple<string,string, string>> groups)
        {
            foreach (var vv in groups.Where(e => e.Item3 == this.FullName))
            {
                string sname = vv.Item1;
                if (!string.IsNullOrEmpty(vv.Item3))
                {
                    sname = sname.Substring(sname.IndexOf(vv.Item3) + vv.Item3.Length + 1);
                }
                TagGroupViewModel groupViewModel = new TagGroupViewModel() { mName = sname, Database = Database,mDescription=vv.Item2 };
                groupViewModel.Parent = this;
                this.Children.Add(groupViewModel);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class RootTagGroupViewModel : TagGroupViewModel
    {

        #region ... Variables  ...
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public RootTagGroupViewModel()
        {
            Name = Res.Get("Tag");
        }
        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public override string FullName => string.Empty;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanRemove()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanCopy()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public override bool OnRename(string oldName, string newName)
        {
            return true;
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
