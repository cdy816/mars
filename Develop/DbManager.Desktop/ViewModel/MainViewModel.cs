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
using Cdy.Tag;
using System.Diagnostics;

namespace DBInStudio.Desktop
{
    public class MainViewModel : ViewModelBase, IProcessNotify
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

        private double mProcessNotify;

        private Visibility mNotifyVisiblity = Visibility.Hidden;

        private string mUserName;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public MainViewModel()
        {
            ServiceLocator.Locator.Registor<IProcessNotify>(this);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
            /// 
            /// </summary>
        public string MainwindowTitle
        {
            get
            {
                return string.IsNullOrEmpty(Database) ? Res.Get("MainwindowTitle"): Res.Get("MainwindowTitle")+"--"+this.Database;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string Database
        {
            get
            {
                return mDatabase;
            }
            set
            {
                if (mDatabase != value)
                {
                    mDatabase = value;
                    OnPropertyChanged("Database");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public string UserName
        {
            get
            {
                return mUserName;
            }
            set
            {
                if (mUserName != value)
                {
                    mUserName = value;
                    OnPropertyChanged("UserName");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public Visibility NotifyVisiblity
        {
            get
            {
                return mNotifyVisiblity;
            }
            set
            {
                if (mNotifyVisiblity != value)
                {
                    mNotifyVisiblity = value;
                    OnPropertyChanged("NotifyVisiblity");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double ProcessNotify
        {
            get
            {
                return mProcessNotify;
            }
            set
            {
                if (mProcessNotify != value)
                {
                    mProcessNotify = value;
                    OnPropertyChanged("ProcessNotify");
                }
            }
        }


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
                        (CurrentSelectGroup).AddCommand.Execute(null);
                        //  NewGroup();
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
                        (CurrentSelectGroup).RemoveCommand.Execute(null);
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

            int mode = 0;
            var mm = new ImportModeSelectViewModel();
            if (mm.ShowDialog().Value)
            {
                mode = mm.Mode;
            }
            else
            {
                return;
            }

            StringBuilder sb = new StringBuilder();

            Task.Run(() => {
                BeginShowNotify();
                int id;
                int icount = 0;
                int tcount = ltmp.Count;

                //删除所有，重新添加
                if (mode == 1)
                {
                    DevelopServiceHelper.Helper.ClearTagAll(this.mDatabase);
                }

                bool haserro = false;
                foreach (var vv in ltmp)
                {
                    //更新数据
                    if (!DevelopServiceHelper.Helper.Import(this.mDatabase, new Tuple<Tagbase, HisTag>(vv.RealTagMode, vv.HisTagMode), mode, out id))
                    {
                        sb.AppendLine(string.Format(Res.Get("AddTagFail"), vv.RealTagMode.Name));
                        haserro = true;
                    }
                    else
                    {
                        vv.IsNew = false;
                        vv.IsChanged = false;
                    }

                    icount++;
                    ServiceLocator.Locator.Resolve<IProcessNotify>().ShowNotifyValue(((icount * 1.0 / tcount) * 100));
                }

                if (haserro)
                {
                    string errofile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(ofd.FileName), "erro.txt");
                    System.IO.File.WriteAllText(errofile, sb.ToString());
                    if (MessageBox.Show(Res.Get("ImportErroMsg"), "", MessageBoxButton.OKCancel) == MessageBoxResult.OK)
                    {
                        try
                        {
                            Process.Start(Path.GetDirectoryName(errofile));
                        }
                        catch
                        {

                        }
                    }
                }

                Application.Current.Dispatcher.BeginInvoke(new Action(() => {
                    IsCanOperate = true;
                    QueryGroups();
                    EndShowNotify();
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

                Task.Run(() => {
                    BeginShowNotify();
                    DevelopServiceHelper.Helper.QueryAllTag(mDatabase,new Action<int, int, Dictionary<int, Tuple<Tagbase, HisTag>>>((idx,total,res)=> {
                        foreach (var vv in res.Select(e => new TagViewModel(e.Value.Item1, e.Value.Item2)))
                        {
                            stream.WriteLine(vv.SaveToCSVString());
                           
                            ServiceLocator.Locator.Resolve<IProcessNotify>().ShowNotifyValue(((idx * 1.0 / total) * 100));
                        }

                    }));
                    stream.Close();
                    EndShowNotify();
                    IsCanOperate = true;
                    MessageBox.Show(Res.Get("TagExportComplete"));
                });

                
            }

          
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
                    UserName = login.UserName;
                    Database = ldm.SelectDatabase.Name;
                    OnPropertyChanged("MainwindowTitle");
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

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="parent"></param>
        ///// <returns></returns>
        //public bool AddGroup(string parent)
        //{
        //    string chileName = GetNewGroupName();
        //    chileName = DevelopServiceHelper.Helper.AddTagGroup(mDatabase, chileName, parent);
        //    if (!string.IsNullOrEmpty(chileName))
        //    {
        //        if (mCurrentSelectTreeItem != null && mCurrentSelectTreeItem is TagGroupViewModel)
        //        {
        //            (mCurrentSelectTreeItem as TagGroupViewModel).Children.Add(new TagGroupViewModel() { mName = chileName, Parent = (mCurrentSelectTreeItem as TagGroupViewModel), Database = this.mDatabase });
        //        }
        //        else
        //        {
        //            this.TagGroup.Add(new TagGroupViewModel() { mName = chileName, Database = this.mDatabase });
        //        }
        //    }
        //    return false;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        //private void NewGroup()
        //{
        //    string sparent = mCurrentSelectTreeItem!=null && mCurrentSelectTreeItem is TagGroupViewModel? (mCurrentSelectTreeItem as TagGroupViewModel).FullName:string.Empty;
        //    AddGroup(sparent);
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <returns></returns>
        //private string GetNewGroupName()
        //{
        //    List<string> vtmps = mCurrentSelectTreeItem!=null && mCurrentSelectTreeItem is TagGroupViewModel? (mCurrentSelectTreeItem as TagGroupViewModel).Children.Select(e => e.Name).ToList():TagGroup.Select(e=>e.Name).ToList();
        //    string tagName = "group";
        //    for (int i = 1; i < int.MaxValue; i++)
        //    {
        //        tagName = "group" + i;
        //        if (!vtmps.Contains(tagName))
        //        {
        //            return tagName;
        //        }
        //    }
        //    return tagName;
        //}

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

        /// <summary>
        /// 
        /// </summary>
        public void BeginShowNotify()
        {
            Application.Current.Dispatcher.BeginInvoke(new Action(() => {

                NotifyVisiblity = Visibility.Visible;
            }));
            
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        public void ShowNotifyValue(double val)
        {
            Application.Current.Dispatcher.BeginInvoke(new Action(() => {
                if (val > 100)
                {
                    ProcessNotify = 100;
                }
                else
                {
                    ProcessNotify = val;
                }
            }));
            
        }

        /// <summary>
        /// 
        /// </summary>
        public void EndShowNotify()
        {
            Application.Current.Dispatcher.BeginInvoke(new Action(() => {
                NotifyVisiblity = Visibility.Hidden;
                ProcessNotify = 0;
            }));
           
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

    ///// <summary>
    ///// 
    ///// </summary>
    //public interface ITagGroupAdd
    //{
    //    bool AddGroup(string parent);
    //}

    /// <summary>
    /// 
    /// </summary>
    public interface IProcessNotify
    {
        /// <summary>
        /// 
        /// </summary>
        void BeginShowNotify();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        void ShowNotifyValue(double val);
        /// <summary>
        /// 
        /// </summary>
        void EndShowNotify();
    }


}
