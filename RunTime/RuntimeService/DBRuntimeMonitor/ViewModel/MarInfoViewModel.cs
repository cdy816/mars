//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/8/18 18:02:26.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace DBRuntimeMonitor
{
    /// <summary>
    /// 
    /// </summary>
    public class MarInfoViewModel: ViewModelBase
    {

        #region ... Variables  ...
        private string mTimeString;
        private System.Timers.Timer tim;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public MarInfoViewModel()
        {
            tim = new System.Timers.Timer();
            tim.Interval = 1000;
            tim.Elapsed += Tim_Elapsed;
            tim.Start();
        }

        
        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public string MarsTitle
        {
            get
            {
                return Res.Get("MarsTitle");
            }
        }

        public string TimeString
        {
            get
            {
                return mTimeString;
            }
            set
            {
                mTimeString = value;
                OnPropertyChanged("TimeString");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void Tim_Elapsed(object sender, System.Timers.ElapsedEventArgs e)
        {
            TimeString = DateTime.Now.ToString();
        }


        #endregion ...Properties...

        #region ... Methods    ...

        public override void Dispose()
        {
            tim.Stop();
            tim.Dispose();
            base.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
