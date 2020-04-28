//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/4 18:51:48.
//  Version 1.0
//  种道洋
//==============================================================

using DBDevelopClientApi;
using Microsoft.Win32;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Input;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class TagGroupDetailViewModel:ViewModelBase
    {

        #region ... Variables  ...
        private TagGroupViewModel mGroupModel;
        private System.Collections.ObjectModel.ObservableCollection<TagViewModel> mSelectGroupTags = new System.Collections.ObjectModel.ObservableCollection<TagViewModel>();

        private ICommand mAddCommand;
        private ICommand mRemoveCommand;
        private ICommand mImportCommand;
        private ICommand mExportCommand;

        private TagViewModel mCurrentSelectTag;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

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
                    UpdateAll();
                    mGroupModel = value;
                    QueryTags();
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
                foreach(var vv in mSelectGroupTags)
                {
                    stream.WriteLine(vv.SaveToCSVString());
                }
                stream.Close();
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
                while(!stream.EndOfStream)
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

            var tags = mSelectGroupTags.ToDictionary(e => e.RealTagMode.Id);
            foreach(var vv in ltmp)
            {
                if(tags.ContainsKey(vv.RealTagMode.Id))
                {
                    if(!DevelopServiceHelper.Helper.UpdateTag(GroupModel.Database, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(vv.RealTagMode, vv.HisTagMode)))
                    {
                        MessageBox.Show(string.Format(Res.Get("UpdateTagFail"), vv.RealTagMode.Name),Res.Get("erro"),MessageBoxButton.OK,MessageBoxImage.Error);
                        break;
                    }
                }
                else
                {
                    int id;
                    if (!DevelopServiceHelper.Helper.AddTag(GroupModel.Database, new Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>(vv.RealTagMode, vv.HisTagMode), out id))
                    {
                        MessageBox.Show(string.Format(Res.Get("AddTagFail"), vv.RealTagMode.Name), Res.Get("erro"), MessageBoxButton.OK, MessageBoxImage.Error);
                        break;
                    }
                    else
                    {
                        vv.RealTagMode.Id = id;
                        if (vv.HisTagMode != null) vv.HisTagMode.Id = id;
                        vv.IsChanged = false;
                        vv.IsNew = false;
                    }
                }
            }

            System.Threading.Tasks.Task.Run(() => { QueryTags();});

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
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetNewName()
        {
            var vtmps = mSelectGroupTags.Select(e => e.Name).ToList();
            string tagName = "tag";
            for (int i = 1; i < int.MaxValue; i++)
            {
                tagName = "tag" + i;
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


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
