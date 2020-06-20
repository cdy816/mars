//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/4 18:51:48.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using DBDevelopClientApi;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class TagGroupDetailViewModel:ViewModelBase, IModeSwitch
    {

        #region ... Variables  ...
        private TagGroupViewModel mGroupModel;
        private System.Collections.ObjectModel.ObservableCollection<TagViewModel> mSelectGroupTags = new System.Collections.ObjectModel.ObservableCollection<TagViewModel>();

        private ICommand mAddCommand;
        private ICommand mRemoveCommand;
        private ICommand mImportCommand;
        private ICommand mExportCommand;

        private ICommand mCopyCommand;
        private ICommand mPasteCommand;

        private TagViewModel mCurrentSelectTag;

        private int mTotalPageNumber = 0;
        private int mCurrentPageIndex = 0;

        private bool mIsLoading = false;

        private static List<TagViewModel> mCopyTags = new List<TagViewModel>();

        private int mTagCount = 0;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool IsLoading
        {
            get
            {
                return mIsLoading;
            }
            set
            {
                if (mIsLoading != value)
                {
                    mIsLoading = value;
                    OnPropertyChanged("IsLoading");
                }
            }
        }

        public ICommand CopyCommand
        {
            get
            {
                if(mCopyCommand==null)
                {
                    mCopyCommand = new RelayCommand(() => {
                        CopyTag();
                    });
                }
                return mCopyCommand;
            }
         }

        /// <summary>
        /// 
        /// </summary>
        public ICommand PasteCommand
        {
            get
            {
                if (mPasteCommand == null)
                {
                    mPasteCommand = new RelayCommand(() => {
                        PasteTag();
                    },()=> { return mCopyTags.Count > 0; });
                }
                return mPasteCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand AddCommand
        {
            get
            {
                if (mAddCommand == null)
                {
                    mAddCommand = new RelayCommand(() => {
                        NewTag();
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
                if (mRemoveCommand == null)
                {
                    mRemoveCommand = new RelayCommand(() => {
                        RemoveTag();
                    }, () => { return CurrentSelectTag != null; });
                }
                return mRemoveCommand;
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
        public ICommand ImportCommand
        {
            get
            {
                if (mImportCommand == null)
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
        public TagViewModel CurrentSelectTag
        {
            get
            {
                return mCurrentSelectTag;
            }
            set
            {
                if (mCurrentSelectTag != value)
                {
                    if (mCurrentSelectTag != null && (mCurrentSelectTag.IsChanged || mCurrentSelectTag.IsNew))
                    {
                        UpdateTag(mCurrentSelectTag);
                    }
                    mCurrentSelectTag = value;

                    OnPropertyChanged("CurrentSelectTag");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public TagGroupViewModel GroupModel
        {
            get
            {
                return mGroupModel;
            }
            set
            {
                if (mGroupModel != value)
                {
                    mGroupModel = value;
                    mTotalPageNumber = -1;
                    
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<TagViewModel> SelectGroupTags
        {
            get
            {
                return mSelectGroupTags;
            }
            set
            {
                mSelectGroupTags = value;
                OnPropertyChanged("SelectGroupTags");
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public int TagCount
        {
            get
            {
                return mTagCount;
            }
            set
            {
                if (mTagCount != value)
                {
                    mTagCount = value;
                    OnPropertyChanged("TagCount");
                }
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void ExportToFile()
        {
            SaveFileDialog ofd = new SaveFileDialog();
            ofd.Filter = "csv|*.csv";
            if(ofd.ShowDialog().Value)
            {
                var stream = new StreamWriter(File.Open(ofd.FileName, FileMode.OpenOrCreate, FileAccess.ReadWrite));

                Task.Run(() => {
                    ServiceLocator.Locator.Resolve<IProcessNotify>().BeginShowNotify();
                    DevelopServiceHelper.Helper.QueryTagByGroup(GroupModel.Database, GroupModel.FullName,new Action<int, int, Dictionary<int, Tuple<Tagbase, HisTag>>>((idx, total, res) => {
                        foreach (var vv in res.Select(e => new TagViewModel(e.Value.Item1, e.Value.Item2)))
                        {
                            stream.WriteLine(vv.SaveToCSVString());

                            ServiceLocator.Locator.Resolve<IProcessNotify>().ShowNotifyValue(((idx * 1.0 / total) * 100));
                        }

                    }));
                    stream.Close();
                    ServiceLocator.Locator.Resolve<IProcessNotify>().EndShowNotify();

                    MessageBox.Show(Res.Get("TagExportComplete"));
                });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void ImportFromFile()
        {
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

            Task.Run(() =>
            {
                ServiceLocator.Locator.Resolve<IProcessNotify>().BeginShowNotify();

                //删除所有，重新添加
                if (mode == 1)
                {
                    DevelopServiceHelper.Helper.ClearTagByGroup(GroupModel.Database, GroupModel.FullName);
                }

                //var tags = mSelectGroupTags.ToDictionary(e => e.RealTagMode.Name);
                int tcount = ltmp.Count;
                int icount = 0;
                bool haserro = false;
                int id = 0;
                foreach (var vv in ltmp)
                {
                    vv.RealTagMode.Group = this.GroupModel.FullName;

                    //更新数据
                    if (!DevelopServiceHelper.Helper.Import(GroupModel.Database, new Tuple<Tagbase, HisTag>(vv.RealTagMode, vv.HisTagMode), mode, out id))
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
                    if(MessageBox.Show(Res.Get("ImportErroMsg"),"",MessageBoxButton.OKCancel) == MessageBoxResult.OK)
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

                mCurrentPageIndex = -1;
                mTotalPageNumber = -1;
                ContinueQueryTags();
                Application.Current.Dispatcher.BeginInvoke(new Action(() =>
                {
                    ServiceLocator.Locator.Resolve<IProcessNotify>().EndShowNotify();
                }));

            });

        }

        public void ContinueLoadData()
        {
            if (!IsLoading)
            {
                IsLoading = true;
                System.Threading.Tasks.Task.Run(() => { ContinueQueryTags(); IsLoading = false; });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void ContinueQueryTags()
        {
            int count = 0;
            if (mTotalPageNumber <0)
            {
                Application.Current.Dispatcher.Invoke(new Action(() => {
                    SelectGroupTags.Clear();
                }));
                
              //  var vtags = new System.Collections.ObjectModel.ObservableCollection<TagViewModel>();
                mCurrentPageIndex = 0;
                
                var vv = DevelopServiceHelper.Helper.QueryTagByGroup(this.GroupModel.Database, this.GroupModel.FullName,mCurrentPageIndex, out mTotalPageNumber,out count);
                if (vv != null)
                {
                    foreach (var vvv in vv)
                    {
                        TagViewModel model = new TagViewModel(vvv.Value.Item1, vvv.Value.Item2);
                        Application.Current.Dispatcher.Invoke(new Action(() => {
                            SelectGroupTags.Add(model);
                        }));
                       
                    }
                }
            }
            else
            {
                if (mTotalPageNumber > mCurrentPageIndex+1)
                {
                    mCurrentPageIndex++;
                    int totalcount = 0;
                    
                    var vv = DevelopServiceHelper.Helper.QueryTagByGroup(this.GroupModel.Database, this.GroupModel.FullName, mCurrentPageIndex, out totalcount,out count);
                    if (vv != null)
                    {
                        foreach (var vvv in vv)
                        {
                            Application.Current.Dispatcher.BeginInvoke(new Action(() =>
                            {
                                TagViewModel model = new TagViewModel(vvv.Value.Item1, vvv.Value.Item2);
                                SelectGroupTags.Add(model);
                            }));
                        }
                    }
                }
            }
            TagCount = count;
        }

        /// <summary>
        /// 
        /// </summary>
        private void QueryTags()
        {
            var vtags = new System.Collections.ObjectModel.ObservableCollection<TagViewModel>();
            
            
            var vv = DevelopServiceHelper.Helper.QueryTagByGroup(this.GroupModel.Database, this.GroupModel.FullName);
            if (vv != null)
            {
                foreach (var vvv in vv)
                {
                    Application.Current.Dispatcher.BeginInvoke(new Action(() => {
                        TagViewModel model = new TagViewModel(vvv.Value.Item1, vvv.Value.Item2);
                        vtags.Add(model);
                    }));
                }
            }

            SelectGroupTags = vtags;

        }


        /// <summary>
        /// 
        /// </summary>
        private void RemoveTag()
        {
            if (CurrentSelectTag != null)
            {
                if (DevelopServiceHelper.Helper.Remove(GroupModel.Database, CurrentSelectTag.RealTagMode.Id))
                {
                    SelectGroupTags.Remove(CurrentSelectTag);
                }
            }
        }

        private void CopyTag()
        {
            mCopyTags.Clear();
            foreach(var vv in mSelectGroupTags.Where(e=>e.IsSelected))
            {
                mCopyTags.Add(vv);
            }
        }

        private void PasteTag()
        {
            if(mCopyTags.Count>0)
            {
                TagViewModel tm = null;
                foreach(var vv in mCopyTags)
                {
                    var vtag = vv.Clone();
                    vtag.RealTagMode.Id = -1;
                    vtag.Name = GetNewName(vv.Name);
                    vtag.IsNew = true;
                    if (UpdateTag(vtag))
                    {
                        this.SelectGroupTags.Add(vtag);
                        tm = vtag;
                    }
                }
                if (tm != null)
                    CurrentSelectTag = tm;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void NewTag()
        {
            if (CurrentSelectTag != null)
            {
                var vtag = CurrentSelectTag.Clone();
                vtag.Name = GetNewName();
                vtag.IsNew = true;
                if (UpdateTag(vtag))
                {
                    this.SelectGroupTags.Add(vtag);
                    CurrentSelectTag = vtag;
                }
            }
            else
            {
                var tag = new Cdy.Tag.DoubleTag() { Name = GetNewName() };
                if (this.GroupModel != null && GroupModel is TagGroupViewModel)
                {
                    tag.Group = (GroupModel as TagGroupViewModel).FullName;
                }
                var vtag = new TagViewModel(tag, null) { IsNew = true };
                if (UpdateTag(vtag))
                {
                    this.SelectGroupTags.Add(vtag);
                    CurrentSelectTag = vtag;
                }
            }
        }

        /// <summary>
        /// 获取字符串中的数字
        /// </summary>
        /// <param name="str">字符串</param>
        /// <returns>数字</returns>
        public static int GetNumberInt(string str)
        {
            int result = -1;
            if (str != null && str != string.Empty)
            {
                // 正则表达式剔除非数字字符（不包含小数点.）
                str = Regex.Replace(str, @"[^\d.\d]", "");
                // 如果是数字，则转换为decimal类型
                if (Regex.IsMatch(str, @"^[+-]?\d*[.]?\d*$"))
                {
                    try
                    {
                        if (!string.IsNullOrEmpty(str))
                            result = int.Parse(str);
                    }
                    catch
                    {

                    }
                }
            }
            return result;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetNewName(string baseName="tag")
        {
            var vtmps = mSelectGroupTags.Select(e => e.Name).ToList();
            string tagName = baseName;

            int number = GetNumberInt(baseName);
            if(number>=0)
            {
                if(tagName.EndsWith(number.ToString()))
                {
                    tagName = tagName.Substring(0, tagName.IndexOf(number.ToString()));
                }
            }
            string sname = tagName;
            for (int i = 1; i < int.MaxValue; i++)
            {
                tagName = sname + i;
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
        /// <param name="tagmodel"></param>
        private bool UpdateTag(TagViewModel tagmodel)
        {
            if (tagmodel.IsNew)
            {
                int id;
                var re = DevelopServiceHelper.Helper.AddTag(GroupModel.Database, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(tagmodel.RealTagMode, tagmodel.HisTagMode), out id);
                if (re)
                {
                    tagmodel.RealTagMode.Id = id;
                    if (tagmodel.HisTagMode != null) tagmodel.HisTagMode.Id = id;
                    tagmodel.IsChanged = false;
                    tagmodel.IsNew = false;
                }
                return re;
            }
            else
            {
                if (tagmodel.IsChanged)
                {
                    var re = DevelopServiceHelper.Helper.UpdateTag(GroupModel.Database, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(tagmodel.RealTagMode, tagmodel.HisTagMode));
                    if (re)
                    {
                        tagmodel.IsChanged = false;
                    }
                    return re;
                }
            }
            return false;
        }


        /// <summary>
        /// 
        /// </summary>
        public void UpdateAll()
        {
            foreach(var vv in this.mSelectGroupTags)
            {
                UpdateTag(vv);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Active()
        {
            mTotalPageNumber = -1;
            ContinueQueryTags();
        }

        /// <summary>
        /// 
        /// </summary>
        public void DeActive()
        {
            UpdateAll();
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
