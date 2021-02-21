//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/13 11:43:08.
//  Version 1.0
//  种道洋
//==============================================================

using HisDataTools.ViewModel;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows.Input;

namespace HisDataTools
{
    /// <summary>
    /// 
    /// </summary>
    public class MainViewModel:ViewModelBase
    {

        #region ... Variables  ...

        private System.Collections.ObjectModel.ObservableCollection<FunctionItemViewModel> mTreeItems = new System.Collections.ObjectModel.ObservableCollection<FunctionItemViewModel>();

        private ICommand mOpenCommand;

        private string mSelectDatabase;

        private FunctionItemViewModel mCurrentItem;

        private ViewModelBase mCurrentEditModel;

        private bool mIsEnable = false;

        private HisDataQueryModel mDataQueryModel;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        public MainViewModel()
        {
            Init();
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool IsEnable
        {
            get
            {
                return mIsEnable;
            }
            set
            {
                if (mIsEnable != value)
                {
                    mIsEnable = value;
                    OnPropertyChanged("IsEnable");
                }
            }
        }


        public FunctionItemViewModel CurrentItem
        {
            get { return mCurrentItem; }
            set
            {
                if (mCurrentItem != value)
                {
                    mCurrentItem = value;
                    SelectMainModel();
                    OnPropertyChanged("CurrentItem");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<FunctionItemViewModel> TreeItems
        {
            get
            {
                return mTreeItems;
            }
        }

            

        /// <summary>
        /// 
        /// </summary>
        public ICommand OpenCommand
        {
            get
            {
                if(mOpenCommand==null)
                {
                    mOpenCommand = new RelayCommand(() =>
                    {
                        DatabaseSelect();
                    });
                }
                return mOpenCommand;
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public ViewModelBase CurrentEditModel
        {
            get
            {
                return mCurrentEditModel;
            }
            set
            {
                if (mCurrentEditModel != value)
                {
                    mCurrentEditModel = value;
                    OnPropertyChanged("CurrentEditModel");
                }
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void SelectMainModel()
        {
            if(mCurrentItem is HisDataQueryItem)
            {
                CurrentEditModel = mDataQueryModel;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            HisDataQueryItem queryItem = new HisDataQueryItem();
            queryItem.PropertyChanged += QueryItem_PropertyChanged;
            mTreeItems.Add(queryItem);
            mDataQueryModel = new HisDataQueryModel();
        }

        private void QueryItem_PropertyChanged(object sender, System.ComponentModel.PropertyChangedEventArgs e)
        {
            if((sender as FunctionItemViewModel).IsSelected)
            {
                CurrentItem = sender as FunctionItemViewModel;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void DatabaseSelect()
        {
            ListDatabaseViewModel ldm = new ListDatabaseViewModel();
            if (ldm.ShowDialog().Value)
            {
                mSelectDatabase = ldm.SelectDatabase.Name;

                HisDataManager.Manager.ScanDatabase(mSelectDatabase);

                mDataQueryModel.LoadData(mSelectDatabase);
                IsEnable = true;
            }
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
