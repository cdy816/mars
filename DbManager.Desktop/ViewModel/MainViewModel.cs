//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 10:54:26.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;
using DBInStudio.Desktop.ViewModel;
using System.Linq;
using System.Threading.Tasks;
using System.Threading;
using System.Windows;
using DBDevelopClientApi;
using Microsoft.Win32;
using System.IO;

namespace DBInStudio.Desktop
{
    public class MainViewModel : ViewModelBase
    {

        #region ... Variables  ...

        private ICommand mLoginCommand;

        private string mDatabase = string.Empty;

        private ICommand mSaveCommand;



        private ICommand mAddGroupCommand;
        private ICommand mRemoveGroupCommand;

        private ICommand mExportCommand;

        private ICommand mImportCommand;

        private TreeItemViewModel mCurrentSelectTreeItem;

   

        private System.Collections.ObjectModel.ObservableCollection<ITreeViewModel> mTagGroup = new System.Collections.ObjectModel.ObservableCollection<ITreeViewModel>();


        private RootTagGroupViewModel mRootTagGroupModel = new RootTagGroupViewModel();

        private SecurityTreeItemViewModel securityModel = new SecurityTreeItemViewModel();

        private ViewModelBase mContentViewModel;

        private bool mIsCanOperate = true;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool IsCanOperate
        {
            get
            {
                return mIsCanOperate;
            }
            set
            {
                if (mIsCanOperate != value)
                {
                    mIsCanOperate = value;
                    OnPropertyChanged("IsCanOperate");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand ImportCommand
        {
            get
            {
                if(mImportCommand==null)
                {
                    mImportCommand = new RelayCommand(() => {
                        ImportFromFile();
                    });
                }
                return mImportCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand ExportCommand
        {
            get
            {
                if (mExportCommand == null)
                {
                    mExportCommand = new RelayCommand(() => {
                        ExportToFile();
                    });
                }
                return mExportCommand;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public ViewModelBase ContentViewModel
        {
            get
            {
                return mContentViewModel;
            }
            set
            {
                if(mContentViewModel!=value)
                {
                    mContentViewModel = value;
                    OnPropertyChanged("ContentViewModel");
                }
            }
        }




        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<ITreeViewModel> TagGroup
        {
            get
            {
                return mTagGroup;
            }
        }





        /// <summary>
        /// 
        /// </summary>
        public TreeItemViewModel CurrentSelectGroup
        {
            get
            {
                return mCurrentSelectTreeItem;
            }
            set
            {
                if (mCurrentSelectTreeItem != value)
                {
                    mCurrentSelectTreeItem = value;
                    SelectContentModel();
                    OnPropertyChanged("CurrentSelectGroup");
                }
            }
        }
    
        /// <summary>
        /// 
        /// </summary>
        public ICommand LoginCommand
        {
            get
            {
                if (mLoginCommand == null)
                {
                    mLoginCommand = new RelayCommand(() => {
                        Login();
                    });
                }
                return mLoginCommand;
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public ICommand AddGroupCommand
        {
            get
            {
                if (mAddGroupCommand == null)
                {
                    mAddGroupCommand = new RelayCommand(() => {
                        NewGroup();
                    },()=> { return mCurrentSelectTreeItem != null && mCurrentSelectTreeItem.CanAddChild(); });
                }
                return mAddGroupCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand RemoveGroupCommand
        {
            get
            {
                if (mRemoveGroupCommand == null)
                {
                    mRemoveGroupCommand = new RelayCommand(() => {
                        (CurrentSelectGroup).ReMoveCommand.Execute(null);
                    },()=> { return CurrentSelectGroup != null && CurrentSelectGroup.CanRemove() ; });
                }
                return mRemoveGroupCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand SaveCommand
        {
            get
            {
                if(mSaveCommand==null)
                {
                    mSaveCommand = new RelayCommand(() => {

                        if (ContentViewModel is TagGroupDetailViewModel)
                        {
                            (ContentViewModel as TagGroupDetailViewModel).UpdateAll();
                        }

                        if (DevelopServiceHelper.Helper.Save(mDatabase))
                        {
                            MessageBox.Show(Res.Get("SaveSucessful"));
                        }
                        else
                        {
                            MessageBox.Show(Res.Get("Savefailed"));
                        }
                    });
                }
                return mSaveCommand;
            }
        }



        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        private void ImportFromFile()
        {
            IsCanOperate = false;

            OpenFileDialog ofd = new OpenFileDialog();
            ofd.Filter = "csv|*.csv";
            List<TagViewModel> ltmp = new List<TagViewModel>();

            if (ofd.ShowDialog().Value)
            {
                var stream = new StreamReader(File.Open(ofd.FileName, FileMode.OpenOrCreate, FileAccess.ReadWrite));
                while (!stream.EndOfStream)
                {
                    string sval = stream.ReadLine();
                    if (!string.IsNullOrEmpty(sval))
                    {
                        TagViewModel tm = TagViewModel.LoadFromCSVString(sval);
                        ltmp.Add(tm);
                    }
                }
                stream.Close();
            }

            Task.Run(() => {

                int id;
                foreach (var vv in ltmp)
                {
                    if (!DevelopServiceHelper.Helper.AddTag(this.mDatabase, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(vv.RealTagMode, vv.HisTagMode), out id))
                    {
                        MessageBox.Show(string.Format(Res.Get("UpdateTagFail"), vv.RealTagMode.Name), Res.Get("erro"), MessageBoxButton.OK, MessageBoxImage.Error);
                        break;
                    }
                }
                Application.Current.Dispatcher.BeginInvoke(new Action(() => {
                    IsCanOperate = true;
                    SelectContentModel();
                }));
            });

            

           
        }

        /// <summary>
        /// 
        /// </summary>
        private void ExportToFile()
        {
            IsCanOperate = false;
            SaveFileDialog ofd = new SaveFileDialog();
            ofd.Filter = "csv|*.csv";
            if (ofd.ShowDialog().Value)
            {

                var stream = new StreamWriter(File.Open(ofd.FileName, FileMode.OpenOrCreate, FileAccess.ReadWrite));
                var res = DevelopServiceHelper.Helper.QueryAllTag(mDatabase);
                foreach (var vv in res.Select(e=>new TagViewModel(e.Value.Item1,e.Value.Item2)))
                {
                    stream.WriteLine(vv.SaveToCSVString());
                }
                stream.Close();
            }

            IsCanOperate = true;
        }

        

        /// <summary>
        /// 
        /// </summary>
        private void Login()
        {
            LoginViewModel login = new LoginViewModel();
            if (login.ShowDialog().Value)
            {
                ListDatabaseViewModel ldm = new ListDatabaseViewModel();
                if (ldm.ShowDialog().Value)
                {
                    this.TagGroup.Clear();

                    mDatabase = ldm.SelectDatabase.Name;
                    var dbitem = new DatabaseViewModel() { Name = mDatabase,IsSelected=true,IsExpanded=true };
                    this.TagGroup.Add(dbitem);
                    dbitem.Children.Add(mRootTagGroupModel);
                    mRootTagGroupModel.Database = mDatabase;
                    dbitem.Children.Add(securityModel);
                    securityModel.Database = mDatabase;
                    securityModel.Init();
                    Task.Run(() => {
                        TagViewModel.Drivers = DevelopServiceHelper.Helper.GetRegistorDrivers(mDatabase);
                        QueryGroups();
                    });
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        private void NewGroup()
        {
            string sparent = mCurrentSelectTreeItem!=null && mCurrentSelectTreeItem is TagGroupViewModel? (mCurrentSelectTreeItem as TagGroupViewModel).FullName:string.Empty;
            string name = GetNewGroupName();
            if(DevelopServiceHelper.Helper.AddTagGroup(mDatabase,name,sparent))
            {
                if (mCurrentSelectTreeItem != null && mCurrentSelectTreeItem is TagGroupViewModel)
                {
                    (mCurrentSelectTreeItem as TagGroupViewModel).Children.Add(new TagGroupViewModel() { Name = name, Parent = (mCurrentSelectTreeItem as TagGroupViewModel) });
                }
                else
                {
                    this.TagGroup.Add(new TagGroupViewModel() { Name = name });
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetNewGroupName()
        {
            List<string> vtmps = mCurrentSelectTreeItem!=null && mCurrentSelectTreeItem is TagGroupViewModel? (mCurrentSelectTreeItem as TagGroupViewModel).Children.Select(e => e.Name).ToList():TagGroup.Select(e=>e.Name).ToList();
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

        ///// <summary>
        ///// 
        ///// </summary>
        //private void RemoveGroup()
        //{
        //    string sname = (mCurrentSelectTreeItem as TagGroupViewModel).FullName;
        //    if(DevelopServiceHelper.Helper.RemoveGroup(mDatabase,sname))
        //    {
        //        if((mCurrentSelectTreeItem as TagGroupViewModel).Parent!=null)
        //        {
        //            (mCurrentSelectTreeItem as TagGroupViewModel).Parent.Children.Remove((mCurrentSelectTreeItem as TagGroupViewModel));
        //            (mCurrentSelectTreeItem as TagGroupViewModel).Parent = null;
        //        }
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        private void QueryGroups()
        {
            Application.Current.Dispatcher.Invoke(() => {
                this.mRootTagGroupModel.Children.Clear();
            });
            
            var vv = DevelopServiceHelper.Helper.QueryTagGroups(this.mDatabase);
            if(vv!=null)
            {
                foreach(var vvv in vv.Where(e=>string.IsNullOrEmpty(e.Value)))
                {
                    Application.Current.Dispatcher.Invoke(() => {
                        TagGroupViewModel groupViewModel = new TagGroupViewModel() { mName = vvv.Key,Database=mDatabase };
                        mRootTagGroupModel.Children.Add(groupViewModel);
                        groupViewModel.InitData(vv);
                    });
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void SelectContentModel()
        {
            if(ContentViewModel is TagGroupDetailViewModel)
            {
                (ContentViewModel as TagGroupDetailViewModel).UpdateAll();
            }
            else if(ContentViewModel is PermissionTreeItemViewModel)
            {
                (ContentViewModel as PermissionDetailViewModel).Update();
            }

            if((mCurrentSelectTreeItem is TagGroupViewModel)||(mCurrentSelectTreeItem is RootTagGroupViewModel))
            {
                if(ContentViewModel==null || !(ContentViewModel is TagGroupDetailViewModel))
                {
                    ContentViewModel = new TagGroupDetailViewModel();
                }
                (ContentViewModel as TagGroupDetailViewModel).GroupModel = mCurrentSelectTreeItem as TagGroupViewModel;
            }
            else if(mCurrentSelectTreeItem is UserTreeItemViewModel)
            {
                if(ContentViewModel == null || !(ContentViewModel is UserGroupDetailViewModel))
                {
                    ContentViewModel = new UserGroupDetailViewModel();
                }
                (ContentViewModel as UserGroupDetailViewModel).Model = mCurrentSelectTreeItem as UserTreeItemViewModel;
            }
            else if(mCurrentSelectTreeItem is PermissionTreeItemViewModel)
            {
                if (ContentViewModel == null || !(ContentViewModel is PermissionTreeItemViewModel))
                {
                    ContentViewModel = new PermissionDetailViewModel() { Database = this.mDatabase };
                }
                 (ContentViewModel as PermissionDetailViewModel).Query();
            }
            else
            {
                ContentViewModel = null;
            }
            
        }

        ///// <summary>
        ///// 
        ///// </summary>
        //private void QueryTags(string group)
        //{
        //    Application.Current.Dispatcher.Invoke(() => {
        //        mSelectGroupTags.Clear();
        //    });
            
        //    var vv = DevelopServiceHelper.Helper.QueryTagByGroup(mDatabase, SelectGroup);
        //    if (vv != null)
        //    {
        //        foreach (var vvv in vv)
        //        {
        //            Application.Current.Dispatcher.BeginInvoke(new Action(() => {
        //                TagViewModel model = new TagViewModel(vvv.Value.Item1, vvv.Value.Item2);
        //                mSelectGroupTags.Add(model);
        //            }));
                    
        //        }
        //    }
        //}

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
