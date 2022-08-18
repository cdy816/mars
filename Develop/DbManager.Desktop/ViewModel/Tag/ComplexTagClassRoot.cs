using DBDevelopClientApi;
using DBInStudio.Desktop.ViewModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DBInStudio.Desktop
{
    public class ComplexTagClassRoot : HasChildrenTreeItemViewModel
    {

       

        public ComplexTagClassRoot()
        {
            Name = Res.Get("ComplexTagClass");
        }

        /// <summary>
        /// 
        /// </summary>
        public override string FullName => string.Empty;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetNewGroupName()
        {
            List<string> vtmps = Children.Select(e => e.Name).ToList();
            string tagName = "cls";
            for (int i = 1; i < int.MaxValue; i++)
            {
                tagName = "cls" + i;
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
        /// <returns></returns>
        public override bool CanPaste()
        {
            return ComplextTagClass.CopyTarget != null && !(this.FullName.Contains(ComplextTagClass.CopyTarget.FullName) || (ComplextTagClass.CopyTarget.FullName.Contains(this.FullName) && !string.IsNullOrEmpty(this.FullName)));
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Paste()
        {
            string sgroup = DevelopServiceHelper.Helper.PasteTagClass(Database, ComplextTagClass.CopyTarget.Name);
            if (!string.IsNullOrEmpty(sgroup))
            {
                this.Children.Add(new ComplextTagClass() { Database = this.Database, mName = sgroup, mDescription = "" });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Add()
        {
            string chileName = GetNewGroupName();
            chileName = DevelopServiceHelper.Helper.AddTagClass(this.Database, chileName);
            if (!string.IsNullOrEmpty(chileName))
            {
                var vmm = new ComplextTagClass() { mName = chileName, Database = this.Database, Parent = this };
                this.Children.Add(vmm);

                if (!this.IsExpanded) this.IsExpanded = true;
                vmm.IsSelected = true;
                vmm.IsEdit = true;
            }
            UpdateTagType();
        }

        public override ViewModelBase GetModel(ViewModelBase mode)
        {
            return new ComplexTagInfoViewModel();
        }

        /// <summary>
        /// 
        /// </summary>
        public void UpdateTagType()
        {
            var vnames = this.Children.Select(e => e.Name).ToList();
            TagViewModel.UpdateTagType(vnames);
        }

    }

    /// <summary>
    /// 
    /// </summary>
    public class ComplextTagClass : TreeItemViewModel
    {
        internal string mDescription;

        private ICommand mSetDescriptionCommand;

        private bool mIsDescriptionEdit = false;

        public static ComplextTagClass CopyTarget { get; set; }

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
                    if (DevelopServiceHelper.Helper.UpdateClassDescription(this.Database, this.FullName, value))
                    {
                        mDescription = value;
                    }
                    OnPropertyChanged("Description");
                }
                IsDescriptionEdit = false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand SetDescriptionCommand
        {
            get
            {
                if (mSetDescriptionCommand == null)
                {
                    mSetDescriptionCommand = new RelayCommand(() => {
                        IsDescriptionEdit = true;
                    });
                }
                return mSetDescriptionCommand;
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mode"></param>
        /// <returns></returns>
        public override ViewModelBase GetModel(ViewModelBase mode)
        {
            if (mode is TagClassDetailViewModel)
            {
                (mode as TagClassDetailViewModel).GroupModel = this;
                return mode;
            }
            else
            {
                return new TagClassDetailViewModel() { GroupModel = this };
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
            return DevelopServiceHelper.Helper.ReNameTagClass(Database, this.FullName, newName);
        }

        /// <summary>
        /// 
        /// </summary>
        public override void OnRenamed()
        {
            (Parent as ComplexTagClassRoot).UpdateTagType();
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Remove()
        {
            string sname = this.FullName;
            if (MessageBox.Show(string.Format(Res.Get("RemoveConfirmMsg"), sname), "", MessageBoxButton.YesNo) == MessageBoxResult.Yes)
            {
                if (DevelopServiceHelper.Helper.RemoveTagClass(Database, sname))
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
            (Parent as ComplexTagClassRoot).UpdateTagType();
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
        public override bool CanPaste()
        {
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Paste()
        {
            (Parent as ComplexTagClassRoot).Paste();
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Add()
        {
            Parent?.Add();
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Copy()
        {
            CopyTarget = this;
        }
    }

}
