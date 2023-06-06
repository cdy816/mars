using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;

namespace DBRuntimeMonitor.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class HisTagViewer:WindowViewModelBase
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private string mTagName = "";
        private DateTime mStartDate;
        private DateTime mEndDate;
        private int mStartHour = 0;
        private int mEndHour = 0;

        private int mFittingType = 0;

        //private ICommand? mQueryCommand;

        private DateTime mStartTime;
        private DateTime mEndTime;

        private QueryType mQType;

        private System.Collections.ObjectModel.ObservableCollection<ValuePoint> mValues = new System.Collections.ObjectModel.ObservableCollection<ValuePoint>();

        private System.Collections.ObjectModel.ObservableCollection<ValuePoint> mAllValues = new System.Collections.ObjectModel.ObservableCollection<ValuePoint>();

        /// <summary>
        /// 
        /// </summary>
        public enum QueryType
        {
            /// <summary>
            /// 原始值
            /// </summary>
            Original,
            /// <summary>
            /// 拟合值
            /// </summary>
            Fitting,
            /// <summary>
            /// 统计值
            /// </summary>
            Statistics
        }

        private bool mIsBusy = false;

        private int mTimeDuration=1;

        private bool mIsOriginalValue = true;
        private bool mIsFittingValue = false;

        private ICommand? mPreCommand;
        private ICommand? mNextCommand;
        private ICommand mRefreshCommand;

        public event EventHandler Update;

        private bool mIgnorClosedQuality = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public HisTagViewer()
        {
            DefaultWidth = 1024;
            DefaultHeight = 600;
            mStartTime = DateTime.Now.Date;
            mEndTime = DateTime.Now.Date.AddDays(1);

            mStartDate = mStartTime.Date;
            mEndDate = mEndTime.Date;

            Title = Res.Get("HisTagViewer");
            HourList=new int[12];
            for(int i=0;i<HourList.Length;i++)
            {
                HourList[i] = i;
            }

            IsOkCancel = false;
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
            /// 
            /// </summary>
        public bool IgnorClosedQuality
        {
            get
            {
                return mIgnorClosedQuality;
            }
            set
            {
                if (mIgnorClosedQuality != value)
                {
                    mIgnorClosedQuality = value;
                    OnPropertyChanged("IgnorClosedQuality");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public int[] HourList { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int StartHour
        {
            get
            {
                return mStartHour;
            }
            set
            {
                if (mStartHour != value)
                {
                    mStartHour = value;
                    StartTime = mStartDate.AddHours(mStartHour);
                    OnPropertyChanged("StartHour");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int EndHour
        {
            get
            {
                return mEndHour;
            }
            set
            {
                if (mEndHour != value)
                {
                    mEndHour = value;
                    EndTime = mEndTime.AddHours(mEndHour);
                    OnPropertyChanged("EndHour");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public DateTime StartDate
        {
            get
            {
                return mStartDate;
            }
            set
            {
                if (mStartDate != value)
                {
                    mStartDate = value;
                    StartTime = mStartDate.AddHours(mStartHour);
                    OnPropertyChanged("StartDate");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public DateTime EndDate
        {
            get
            {
                return mEndDate;
            }
            set
            {
                if (mEndDate != value)
                {
                    mEndDate = value;
                    EndTime = mEndDate.AddHours(mEndHour);
                    OnPropertyChanged("EndDate");
                }
            }
        }


        public DateTime StartTime
        {
            get
            {
                return mStartTime;
            }
            set
            {
                mStartTime = value;
                ExecuteQuery();
                OnPropertyChanged("StartTime");
            }
        }

        public DateTime EndTime
        {
            get
            {
                return mEndTime;
            }
            set
            {
                mEndTime = value;
                ExecuteQuery();
                OnPropertyChanged("EndTime");
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool IsOriginalValue
        {
            get
            {
                return mIsOriginalValue;
            }
            set
            {
                if (mIsOriginalValue != value)
                {
                    mIsOriginalValue = value;
                    if(value)
                    {
                        QType = 0;
                    }
                    OnPropertyChanged("IsOriginalValue");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsFittingValue
        {
            get
            {
                return mIsFittingValue;
            }
            set
            {
                if (mIsFittingValue != value)
                {
                    mIsFittingValue = value;
                    if(value)
                    {
                        QType = 1;
                    }
                    OnPropertyChanged("IsFittingValue");
                }
            }
        }



        /// <summary>
        /// 
        /// </summary>
        public bool IsBusy
        {
            get
            {
                return mIsBusy;
            }
            set
            {
                if (mIsBusy != value)
                {
                    mIsBusy = value;
                    OnPropertyChanged("IsBusy");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<ValuePoint> Values
        {
            get
            {
                return mValues;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public System.Collections.ObjectModel.ObservableCollection<ValuePoint> AllValues
        {
            get
            {
                return mAllValues;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public DBHighApi.ApiClient? Client
        {
            get;
            set;
        }


        /// <summary>
        /// 
        /// </summary>
        public string TagName
        {
            get
            {
                return mTagName;
            }
            set
            {
                if (mTagName != value)
                {
                    mTagName = value;
                    this.Title += " "+ TagName;
                    OnPropertyChanged("TagName");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public Cdy.Tag.TagType TagType { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int TagId { get; set; }



        /// <summary>
        /// 
        /// </summary>
        public ICommand PreCommand
        {
            get
            {
                if(mPreCommand==null)
                {
                    mPreCommand = new RelayCommand(() => {

                        double dtmp = -(mEndTime - mStartTime).TotalSeconds / 2;

                        mStartTime = mStartTime.AddSeconds(dtmp);
                        mEndTime = mEndTime.AddSeconds(dtmp);

                        mStartDate = mStartTime.Date;
                        mStartHour = mStartTime.Hour;
                        mEndDate = EndTime.Date;
                        mEndHour = EndTime.Hour;

                        ExecuteQuery();
                        OnPropertyChanged("StartTime");
                        OnPropertyChanged("EndTime");
                        OnPropertyChanged("EndDate");
                        OnPropertyChanged("StartDate");
                        OnPropertyChanged("EndHour");
                        OnPropertyChanged("StartHour");
                    });
                }
                return mPreCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand NextCommand
        {
            get
            {
                if(mNextCommand==null)
                {
                    mNextCommand = new RelayCommand(() => {
                        
                        double dtmp = (mEndTime - mStartTime).TotalSeconds / 2;

                        mStartTime = mStartTime.AddSeconds(dtmp);
                        mEndTime = mEndTime.AddSeconds(dtmp);

                        mStartDate = mStartTime.Date;
                        mStartHour = mStartTime.Hour;
                        mEndDate = EndTime.Date;
                        mEndHour = EndTime.Hour;
                        ExecuteQuery();
                        OnPropertyChanged("StartTime");
                        OnPropertyChanged("EndTime");
                        OnPropertyChanged("EndDate");
                        OnPropertyChanged("StartDate");
                        OnPropertyChanged("EndHour");
                        OnPropertyChanged("StartHour");
                    });
                }
                return mNextCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand RefreshCommand
        {
            get
            {
                if(mRefreshCommand==null)
                {
                    mRefreshCommand = new RelayCommand(() => {
                        ExecuteQuery();
                    });
                }
                return mRefreshCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int QType
        {
            get
            {
                return (int)mQType;
            }
            set
            {
                if ((int)mQType != value)
                {
                    mQType = (QueryType)value;
                    OnPropertyChanged("QType");
                    ExecuteQuery();
                }
            }
        }

       


        /// <summary>
        /// 拟合方式
        /// </summary>
        public int FittingType
        {
            get
            {
                return mFittingType;
            }
            set
            {
                if (mFittingType != value)
                {
                    mFittingType = value;
                    OnPropertyChanged("FittingType");
                    ExecuteQuery();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int TimeDuration
        {
            get
            {
                return mTimeDuration;
            }
            set
            {
                if (mTimeDuration != value&&value>=1)
                {
                    mTimeDuration = value;
                    OnPropertyChanged("TimeDuration");
                    ExecuteQuery();
                }
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void ExecuteQuery()
        {

            if (!mIsBusy)
            {
                mIsBusy = true;
                Task.Run(() => {
                    if (Client != null && Client.IsLogin)
                    {
                        if (mQType == QueryType.Original)
                        {
                            ExecuteQueryAll();
                        }
                        else if (mQType == QueryType.Fitting)
                        {
                            ExecuteQueryFitting();
                        }
                        else
                        {
                            ExecuteQueryStatics();
                        }
                        Update?.Invoke(this, new EventArgs());
                    }
                    mIsBusy = false;
                });
            }


        }

        /// <summary>
        /// 执行查询所有值
        /// </summary>
        private void ExecuteQueryAll()
        {
            switch (TagType)
            {
                case Cdy.Tag.TagType.Bool:
                    ExecuteQueryAll<bool>();
                    break;
                case Cdy.Tag.TagType.Byte:
                    ExecuteQueryAll<byte>();
                    break;
                case Cdy.Tag.TagType.DateTime:
                    ExecuteQueryAll<DateTime>();
                    break;
                case Cdy.Tag.TagType.Double:
                    ExecuteQueryAll<double>();
                    break;
                case Cdy.Tag.TagType.Float:
                    ExecuteQueryAll<float>();
                    break;
                case Cdy.Tag.TagType.Int:
                    ExecuteQueryAll<int>();
                    break;
                case Cdy.Tag.TagType.Long:
                    ExecuteQueryAll<long>();
                    break;
                case Cdy.Tag.TagType.Short:
                    ExecuteQueryAll<short>();
                    break;
                case Cdy.Tag.TagType.String:
                    ExecuteQueryAll<string>();
                    break;
                case Cdy.Tag.TagType.UInt:
                    ExecuteQueryAll<uint>();
                    break;
                case Cdy.Tag.TagType.ULong:
                    ExecuteQueryAll<ulong>();
                    break;
                case Cdy.Tag.TagType.UShort:
                    ExecuteQueryAll<ushort>();
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    ExecuteQueryAll<IntPointData>();
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    ExecuteQueryAll<UIntPointData>();
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    ExecuteQueryAll<IntPoint3Data>();
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    ExecuteQueryAll<UIntPoint3Data>();
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    ExecuteQueryAll<LongPointData>();
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    ExecuteQueryAll<ULongPointTag>();
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    ExecuteQueryAll<LongPoint3Data>();
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    ExecuteQueryAll<ULongPoint3Data>();
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private void ExecuteQueryAll<T>()
        {
            var vals = Client?.QueryAllHisValue<T>(TagId, mStartTime, mEndTime);
            if(vals!=null &&  vals.Count > 0)
            {
                FillValues(vals);
            }
            else
            {
                FillNullValues(mStartTime, mEndTime, new TimeSpan(0, 0, (int)((mEndTime - mStartTime).TotalSeconds / 100)));
            }
        }

        /// <summary>
        /// 执行查询拟合值
        /// </summary>
        private void ExecuteQueryFitting()
        {
            switch (TagType)
            {
                case Cdy.Tag.TagType.Bool:
                    ExecuteQueryFitting<bool>();
                    break;
                case Cdy.Tag.TagType.Byte:
                    ExecuteQueryFitting<byte>();
                    break;
                case Cdy.Tag.TagType.DateTime:
                    ExecuteQueryFitting<DateTime>();
                    break;
                case Cdy.Tag.TagType.Double:
                    ExecuteQueryFitting<double>();
                    break;
                case Cdy.Tag.TagType.Float:
                    ExecuteQueryFitting<float>();
                    break;
                case Cdy.Tag.TagType.Int:
                    ExecuteQueryFitting<int>();
                    break;
                case Cdy.Tag.TagType.Long:
                    ExecuteQueryFitting<long>();
                    break;
                case Cdy.Tag.TagType.Short:
                    ExecuteQueryFitting<short>();
                    break;
                case Cdy.Tag.TagType.String:
                    ExecuteQueryFitting<string>();
                    break;
                case Cdy.Tag.TagType.UInt:
                    ExecuteQueryFitting<uint>();
                    break;
                case Cdy.Tag.TagType.ULong:
                    ExecuteQueryFitting<ulong>();
                    break;
                case Cdy.Tag.TagType.UShort:
                    ExecuteQueryFitting<ushort>();
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    ExecuteQueryFitting<IntPointData>();
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    ExecuteQueryFitting<UIntPointData>();
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    ExecuteQueryFitting<IntPoint3Data>();
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    ExecuteQueryFitting<UIntPoint3Data>();
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    ExecuteQueryFitting<LongPointData>();
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    ExecuteQueryFitting<ULongPointTag>();
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    ExecuteQueryFitting<LongPoint3Data>();
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    ExecuteQueryFitting<ULongPoint3Data>();
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private void ExecuteQueryFitting<T>()
        {
            if (IgnorClosedQuality)
            {
                var vals = Client?.QueryHisValueForTimeSpanByIgnorSystemExit<T>(TagId, mStartTime, mEndTime, new TimeSpan(0, 0, mTimeDuration), (QueryValueMatchType)FittingType);
                if (vals != null && vals.Count > 0)
                {
                    FillValues(vals);
                }
            }
            else
            {
                var vals = Client?.QueryHisValueForTimeSpan<T>(TagId, mStartTime, mEndTime, new TimeSpan(0, 0, mTimeDuration), (QueryValueMatchType)FittingType);
                if (vals != null && vals.Count > 0)
                {
                    FillValues(vals);
                }
            }
        }


        /// <summary>
        /// 执行查询统计值
        /// </summary>
        private void ExecuteQueryStatics()
        {
            switch (TagType)
            {
                case Cdy.Tag.TagType.Bool:
                    ExecuteQueryStatics<bool>();
                    break;
                case Cdy.Tag.TagType.Byte:
                    ExecuteQueryStatics<byte>();
                    break;
                case Cdy.Tag.TagType.DateTime:
                    ExecuteQueryStatics<DateTime>();
                    break;
                case Cdy.Tag.TagType.Double:
                    ExecuteQueryStatics<double>();
                    break;
                case Cdy.Tag.TagType.Float:
                    ExecuteQueryStatics<float>();
                    break;
                case Cdy.Tag.TagType.Int:
                    ExecuteQueryStatics<int>();
                    break;
                case Cdy.Tag.TagType.Long:
                    ExecuteQueryStatics<long>();
                    break;
                case Cdy.Tag.TagType.Short:
                    ExecuteQueryStatics<short>();
                    break;
                case Cdy.Tag.TagType.String:
                    ExecuteQueryStatics<string>();
                    break;
                case Cdy.Tag.TagType.UInt:
                    ExecuteQueryStatics<uint>();
                    break;
                case Cdy.Tag.TagType.ULong:
                    ExecuteQueryStatics<ulong>();
                    break;
                case Cdy.Tag.TagType.UShort:
                    ExecuteQueryStatics<ushort>();
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    ExecuteQueryStatics<IntPointData>();
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    ExecuteQueryStatics<UIntPointData>();
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    ExecuteQueryStatics<IntPoint3Data>();
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    ExecuteQueryStatics<UIntPoint3Data>();
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    ExecuteQueryStatics<LongPointData>();
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    ExecuteQueryStatics<ULongPointTag>();
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    ExecuteQueryStatics<LongPoint3Data>();
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    ExecuteQueryStatics<ULongPoint3Data>();
                    break;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        private void ExecuteQueryStatics<T>()
        {
            List<DateTime> tims = new List<DateTime>();
            var vals = Client?.QueryStatisticsAtTimes(TagId, tims);
            if (vals != null && vals.Count > 0)
            {
                mValues.Clear();
                //foreach (var vv in vals.ListAllValue())
                //{
                //    mValues.Add(new ValuePoint() { Time = vv.Time, Value = vv.AvgValue, Quality = vv.Quality });
                //}
            }
        }

        private void FillValues<T>(HisQueryResult<T> points)
        {
            System.Windows.Application.Current.Dispatcher.Invoke(() => {
                mValues.Clear();
                mAllValues.Clear();
                foreach (var vv in points.ListAvaiableValues())
                {
                    mValues.Add(new ValuePoint() { Time = vv.Time.ToLocalTime(), Value = vv.Value, Quality = vv.Quality });
                }

                for(int i=0; i < points.Count; i++)
                {
                    var vv = points.GetValue(i,out DateTime time,out byte quality);
                    mAllValues.Add(new ValuePoint() { Time = time.ToLocalTime(), Value = vv, Quality = quality });
                }

            });
           
        }

        private void FillNullValues(DateTime startime,DateTime endtime,TimeSpan dur)
        {
            System.Windows.Application.Current.Dispatcher.Invoke(() => {
                mAllValues.Clear();
                DateTime dt = startime;
                while(dt<=endtime)
                {
                    mAllValues.Add(new ValuePoint() { Time = dt, Value = 0, Quality = (byte)QualityConst.Null});
                    dt = dt.Add(dur);
                }
            });
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public struct ValuePoint
    {
        /// <summary>
        /// 时间
        /// </summary>
        public DateTime Time { get; set; }
        /// <summary>
        /// 值
        /// </summary>
        public object Value { get; set; }
        /// <summary>
        /// 质量戳
        /// </summary>
        public byte Quality { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsSelected { get; set; }
    }
}
