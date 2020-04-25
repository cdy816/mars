//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/13 17:50:51.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Text;
using System.Windows;
using System.Windows.Input;

namespace HisDataTools.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataQueryModel : ViewModelBase
    {

        #region ... Variables  ...
        private string mSelectTag = string.Empty;

        private DateTime mStartTime =DateTime.Now.Date;
        private int mStartTimeHour;
        private DateTime mEndTime = DateTime.Now.Date;
        private int mEndTimeHour;

        private Dictionary<string, Tuple<int,byte>> mTags;

        private ICommand mQueryCommand;

        private System.Collections.ObjectModel.ObservableCollection<HisDataPoint> mDatas = new System.Collections.ObjectModel.ObservableCollection<HisDataPoint>();

        private bool mIsBusy = false;

        private string mDatabase = "";

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public HisDataQueryModel()
        {
            Init();
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public int[] HourItems { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public IEnumerable<string> TagList
        {
            get
            {
                return mTags.Keys;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string SelectTag
        {
            get
            {
                return mSelectTag;
            }
            set
            {
                if (mSelectTag != value)
                {
                    mSelectTag = value;
                    OnPropertyChanged("SelectTag");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public DateTime StartTime
        {
            get
            {
                return mStartTime;
            }
            set
            {
                if (mStartTime != value)
                {
                    mStartTime = value;
                    OnPropertyChanged("StartTime");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int StartTimeHour
        {
            get
            {
                return mStartTimeHour;
            }
            set
            {
                if (mStartTimeHour != value)
                {
                    mStartTimeHour = value;
                    OnPropertyChanged("StartTimeHour");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime
        {
            get
            {
                return mEndTime;
            }
            set
            {
                if (mEndTime != value)
                {
                    mEndTime = value;
                    OnPropertyChanged("EndTime");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int EndTimeHour
        {
            get
            {
                return mEndTimeHour;
            }
            set
            {
                if (mEndTimeHour != value)
                {
                    mEndTimeHour = value;
                    OnPropertyChanged("EndTimeHour");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand QueryCommand
        {
            get
            {
                if (mQueryCommand == null)
                {
                    mQueryCommand = new RelayCommand(() =>
                    {
                        DateTime stime = StartTime.AddHours(StartTimeHour);
                        DateTime etime = EndTime.AddHours(EndTimeHour);
                        QueryHisData(SelectTag, stime, etime);

                    },()=> { return !string.IsNullOrEmpty(mSelectTag) && !mIsBusy && EndTime.AddHours(EndTimeHour)> StartTime.AddHours(StartTimeHour); });
                }
                return mQueryCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ObservableCollection<HisDataPoint> Datas
        {
            get
            {
                return mDatas;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void LoadData(string database)
        {
            mTags = HisDataManager.Manager.GetHisTagIds(database);
            mDatabase = database;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        private void QueryHisData(string tag,DateTime startTime,DateTime endTime)
        {
            mIsBusy = true;

            Application.Current.Dispatcher.Invoke(new Action(() => {
                mDatas.Clear();
            }));

            int id = mTags[mSelectTag].Item1;
            DateTime sTime = StartTime.AddHours(StartTimeHour);
            DateTime eTime = EndTime.AddHours(EndTimeHour);

            int tcount = (int)(eTime - sTime).TotalSeconds;

            switch (mTags[mSelectTag].Item2)
            {
                case (byte)Cdy.Tag.TagType.Bool:
                    ProcessDataQuery<bool>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.Byte:
                    ProcessDataQuery<byte>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.DateTime:
                    ProcessDataQuery<DateTime>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.Double:
                    ProcessDataQuery<double>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.Float:
                    ProcessDataQuery<float>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.Int:
                    ProcessDataQuery<int>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.Long:
                    ProcessDataQuery<long>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.Short:
                    ProcessDataQuery<short>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.String:
                    ProcessDataQuery<string>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.UInt:
                    ProcessDataQuery<uint>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.ULong:
                    ProcessDataQuery<ulong>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.UShort:
                    ProcessDataQuery<ushort>(id, sTime, eTime);
                    break;
            }
            mIsBusy = false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="sTime"></param>
        /// <param name="eTime"></param>
        private void ProcessDataQuery<T>(int id,DateTime sTime,DateTime eTime)
        {
            var result = HisDataManager.Manager.GetQueryService(mDatabase).ReadAllValue<double>(id, sTime, eTime);

            for (int i = 0; i < result.Count; i++)
            {
                object value;
                DateTime time;
                byte qu = 0;
                value = result.GetValue(i, out time, out qu);
                Application.Current.Dispatcher.BeginInvoke(new Action(() => {
                    
                   
                    mDatas.Add(new HisDataPoint() { DateTime = time, Quality = qu, Value = value });
                }), null);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            List<int> ltmp = new List<int>();
            for(int i=0;i<=24;i++)
            {
                ltmp.Add(i);
            }
            HourItems = ltmp.ToArray();
        }



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public struct HisDataPoint
    {
        /// <summary>
        /// 
        /// </summary>
        public DateTime DateTime { get; set; }

        public string DateTimeString
        {
            get
            {
                return DateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Quality { get; set; }
    }

}
