//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/5 18:21:17.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace DBInStudio.Desktop.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class CompressParameterModelBase:ViewModelBase
    {

        #region ... Variables  ...
        private Dictionary<string, double> mParameters = new Dictionary<string, double>();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        
        public Dictionary<string, double> Parameters
        {
            get
            {
                return mParameters;
            }
            set
            {
                mParameters = value;
                Init();
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public virtual void Init()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void FillParameters()
        {

        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class DeadAreaCompressParameterViewModel : CompressParameterModelBase
    {

        #region ... Variables  ...
        private double mDeadValue;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
            /// 死区值
            /// </summary>
        public double DeadValue
        {
            get
            {
                return mDeadValue;
            }
            set
            {
                if (mDeadValue != value)
                {
                    mDeadValue = value;
                    FillParameters();
                }
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public override void Init()
        {
            if(Parameters.ContainsKey("DeadValue"))
            {
                this.mDeadValue = Parameters["DeadValue"];
            }
            else
            {
                this.mDeadValue = 0;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public override void FillParameters()
        {
            if (Parameters.ContainsKey("DeadValue"))
            {
                Parameters["DeadValue"] = DeadValue;
            }
            else
            {
                Parameters.Add("DeadValue", DeadValue);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    public class SlopeCompressParameterViewModel : CompressParameterModelBase
    {

        #region ... Variables  ...
        private double mSlopeValue;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
            /// 斜率值
            /// </summary>
        public double SlopeValue
        {
            get
            {
                return mSlopeValue;
            }
            set
            {
                if (mSlopeValue != value)
                {
                    mSlopeValue = value;
                    FillParameters();
                    OnPropertyChanged("SlopeValue");
                }
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public override void Init()
        {
            if (Parameters.ContainsKey("SlopeValue"))
            {
                this.mSlopeValue = Parameters["SlopeValue"];
            }
            else
            {
                this.mSlopeValue = 0;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public override void FillParameters()
        {
            if (Parameters.ContainsKey("SlopeValue"))
            {
                Parameters["SlopeValue"] = SlopeValue;
            }
            else
            {
                Parameters.Add("SlopeValue", SlopeValue);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


}
