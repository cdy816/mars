//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/1/2 10:05:09.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace DBInStudio.Desktop
{
    public class ViewModelBase : INotifyPropertyChanged
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        protected void OnPropertyChanged(string name)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        }

        #endregion ...Methods...

        #region ... Interfaces ...
        /// <summary>
        /// 
        /// </summary>
        public event PropertyChangedEventHandler PropertyChanged;
        #endregion ...Interfaces...

    }
}
