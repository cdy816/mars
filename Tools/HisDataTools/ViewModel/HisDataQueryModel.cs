//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/13 17:50:51.
//  Version 1.0
//  种道洋
//==============================================================

using HeBianGu.WPF.EChart;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using System.Linq;
using Cdy.Tag;

namespace HisDataTools.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataQueryModel : ViewModelBase
    {

        #region ... Variables  ...
        private string mSelectTag = string.Empty;

        private IEnumerable<string> mTagFilters;

        private DateTime mStartTime =DateTime.Now.Date;
        private int mStartTimeHour;
        private DateTime mEndTime = DateTime.Now.Date;
        private int mEndTimeHour=24;

        private Dictionary<string, Tuple<int,byte>> mTags;

        private ICommand mQueryCommand;

        private List<HisDataPoint> mDatas = new List<HisDataPoint>();

        private List<ICurveEntitySource> mChartSource = new List<ICurveEntitySource>();

        private bool mIsBusy = false;

        private string mDatabase = "";

        private double mMaxXValue = 0;
        private double mMaxYValue = 0;


        private double mMinXValue = 0;
        private double mMinYValue = 0;

        private List<SplitItem> mYLineItems;

        private bool mAllValue=true;

        private int mTimeSpan=10;

        private string mOpMessage;

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
        public string OpMessage
        {
            get
            {
                return mOpMessage;
            }
            set
            {
                if (mOpMessage != value)
                {
                    mOpMessage = value;
                    OnPropertyChanged("OpMessage");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public List<ICurveEntitySource> ChartSource
        {
            get
            {
                return mChartSource;
            }
            set
            {
                mChartSource = value;
                OnPropertyChanged("ChartSource");
            }
        }


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
                return mTagFilters;
            }
            set
            {
                mTagFilters = value;
                OnPropertyChanged("TagList");
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
                    FilterData();
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

                        if (!AllValue)
                        {
                            List<DateTime> dt = new List<DateTime>();
                            DateTime dtt = stime;
                            do
                            {
                                dtt = dtt.AddSeconds(TimeSpan);
                                dt.Add(dtt);
                            }
                            while (dtt <= etime);
                            QueryHisData(SelectTag, dt);
                        }
                        else
                        {
                            QueryHisData(SelectTag, stime, etime);
                        }


                    },()=> { return !string.IsNullOrEmpty(mSelectTag) && !mIsBusy && EndTime.AddHours(EndTimeHour)> StartTime.AddHours(StartTimeHour); });
                }
                return mQueryCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public List<HisDataPoint> Datas
        {
            get
            {
                return mDatas;
            }
            set
            {
                mDatas = value;
                OnPropertyChanged("Datas");
            }
        }
        
        /// <summary>
        /// 
        /// </summary>
        public List<SplitItem> YLineItems
        {
            get
            {
                return mYLineItems;
            }
            set
            {
                if (mYLineItems != value)
                {
                    mYLineItems = value;
                    OnPropertyChanged("YLineItems");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public double MaxYValue
        {
            get
            {
                return mMaxYValue;
            }
            set
            {
                if (mMaxYValue != value)
                {
                    mMaxYValue = value;
                    OnPropertyChanged("MaxYValue");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public double MaxXValue
        {
            get
            {
                return mMaxXValue;
            }
            set
            {
                if (mMaxXValue != value)
                {
                    mMaxXValue = value;
                    OnPropertyChanged("MaxXValue");
                }
            }
        }


        /// <summary>
            /// 
            /// </summary>
        public double MinXValue
        {
            get
            {
                return mMinXValue;
            }
            set
            {
                if (mMinXValue != value)
                {
                    mMinXValue = value;
                    OnPropertyChanged("MinXValue");
                }
            }
        }


        /// <summary>
            /// 
            /// </summary>
        public double MinYValue
        {
            get
            {
                return mMinYValue;
            }
            set
            {
                if (mMinYValue != value)
                {
                    mMinYValue = value;
                    OnPropertyChanged("MinYValue");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public bool AllValue
        {
            get
            {
                return mAllValue;
            }
            set
            {
                if (mAllValue != value)
                {
                    mAllValue = value;
                    OnPropertyChanged("AllValue");
                }
            }
        }

        /// <summary>
            /// 
            /// </summary>
        public int TimeSpan
        {
            get
            {
                return mTimeSpan;
            }
            set
            {
                if (mTimeSpan != value)
                {
                    mTimeSpan = value;
                    OnPropertyChanged("TimeSpan");
                }
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

            FilterData();
            mDatabase = database;
        }

        private void FilterData()
        {
            var query = mTags.Keys.AsQueryable();

            TagList = query.Where(e => e.StartsWith(mSelectTag)).Take(10);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="times"></param>
        private void QueryHisData(string tag,List<DateTime> times)
        {
            mIsBusy = true;

            mDatas.Clear();

            int id = mTags[mSelectTag].Item1;
          

            switch (mTags[mSelectTag].Item2)
            {
                case (byte)Cdy.Tag.TagType.Bool:
                    ProcessDataQuery<bool>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.Byte:
                    ProcessDataQuery<byte>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.DateTime:
                    ProcessDataQuery<DateTime>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.Double:
                    ProcessDataQuery<double>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.Float:
                    ProcessDataQuery<float>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.Int:
                    ProcessDataQuery<int>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.Long:
                    ProcessDataQuery<long>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.Short:
                    ProcessDataQuery<short>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.String:
                    ProcessDataQuery<string>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.UInt:
                    ProcessDataQuery<uint>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.ULong:
                    ProcessDataQuery<ulong>(id, times);
                    break;
                case (byte)Cdy.Tag.TagType.UShort:
                    ProcessDataQuery<ushort>(id, times);
                    break;
            }
            mIsBusy = false;
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

            if (!mTags.ContainsKey(mSelectTag)) return;

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
                case (byte)Cdy.Tag.TagType.IntPoint:
                    ProcessDataQuery<IntPointData>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.UIntPoint:
                    ProcessDataQuery<UIntPointData>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.IntPoint3:
                    ProcessDataQuery<IntPoint3Data>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.UIntPoint3:
                    ProcessDataQuery<UIntPoint3Data>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.LongPoint:
                    ProcessDataQuery<LongPointData>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.ULongPoint:
                    ProcessDataQuery<ULongPointTag>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.LongPoint3:
                    ProcessDataQuery<LongPoint3Data>(id, sTime, eTime);
                    break;
                case (byte)Cdy.Tag.TagType.ULongPoint3:
                    ProcessDataQuery<ULongPoint3Data>(id, sTime, eTime);
                    break;
            }
            mIsBusy = false;
        }

        private void ProcessDataQuery<T>(int id, List<DateTime> times)
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var result = HisDataManager.Manager.GetQueryService(mDatabase).ReadValue<T>(id,times,Cdy.Tag.QueryValueMatchType.Linear);

            sw.Stop();

           

            OpMessage = string.Format(Res.Get("OpMsgFormate"), result.Count, sw.ElapsedMilliseconds);

            CurveEntitySource entity = new CurveEntitySource();
            entity.Text = this.SelectTag;
            entity.Color = System.Windows.Media.Brushes.Red;
            entity.Marker = new CirclePointMarker();
            entity.Marker.Fill = System.Windows.Media.Brushes.Red;
            entity.Marker.Visibility = Visibility.Hidden;


            double maxx = double.MinValue, maxy = double.MinValue, minx = double.MaxValue, miny = double.MaxValue;

            Dictionary<DateTime,Tuple<object, int>> vtmps = new Dictionary<DateTime, Tuple<object, int>>();

            List<HisDataPoint> ltmp = new List<HisDataPoint>();

            int i = 0;
            for (i=0;i<result.Count;i++)
            {
                object value;
                DateTime time;
                byte qu = 0;
                value = result.GetValue(i, out time, out qu);
                vtmps.Add(time, new Tuple<object, int>(value, qu));
            }
            i = 0;
            foreach (var vv in times)
            {
                object value=0;
                DateTime time=vv;
                byte qu = 255;
                if(vtmps.ContainsKey(vv))
                {
                    value = vtmps[vv].Item1;
                    qu = (byte)vtmps[vv].Item2;
                }

                minx = minx > i ? i : minx;
                maxx = maxx < i ? i : maxx;

                var dtmp = ConvertValue(value);

                miny = miny > dtmp ? dtmp : miny;
                maxy = maxy < dtmp ? dtmp : maxy;

                PointC point = new PointC();
                point.X = i;
                point.Y = dtmp;
                point.Text = time.ToString("dd HH:mm:ss");
                entity.Source.Add(point);

                ltmp.Add(new HisDataPoint() { DateTime = time, Quality = qu, Value = value });
                i++;
            }

            Datas = ltmp;

            List<SplitItem> yitems = new List<SplitItem>(5);

            maxy = maxy * 1.2;
            miny = miny - Math.Abs(miny) * 0.2;

            MaxXValue = maxx;
            MaxYValue = maxy;
            MinXValue = minx;
            MinYValue = miny;
            ChartSource = new List<ICurveEntitySource>() { entity };

            var dval = (maxy - miny) / 5;
            for (i = 1; i <= 5; i++)
            {
                yitems.Add(new SplitItem() { SpliteType = SplitItemType.Normal, Text = (miny + i * dval).ToString("f2"), IsShowText = true, Value = miny + i * dval, Color = System.Windows.Media.Brushes.SkyBlue });
            }
            yitems.Add(new SplitItem() { SpliteType = SplitItemType.HeighLight, Text = (miny + (maxy - miny) / 2).ToString("f2"), IsShowText = true, Value = miny + (maxy - miny) / 2, Color = System.Windows.Media.Brushes.DeepSkyBlue, IsShowTrangle = true });



            YLineItems = yitems;
            result.Dispose();
        }

        private double ConvertValue(object value)
        {
           if((value is UIntPointData)|| (value is IntPointData)|| (value is IntPoint3Data)|| (value is UIntPoint3Data)|| (value is LongPointData) || (value is ULongPointData) || (value is LongPoint3Data) || (value is ULongPoint3Data))
            {
                return Convert.ToDouble(((dynamic)value).X);
            }
            return Convert.ToDouble(value);
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
            Stopwatch sw = new Stopwatch();
            sw.Start();
            var result = HisDataManager.Manager.GetQueryService(mDatabase).ReadAllValue<T>(id, sTime, eTime);
            sw.Stop();

            OpMessage = string.Format(Res.Get("OpMsgFormate"), result.Count, sw.ElapsedMilliseconds);

            //Debug.Print("查询耗时:" + sw.ElapsedMilliseconds);

            CurveEntitySource entity = new CurveEntitySource();
            entity.Text = this.SelectTag;
            entity.Color = System.Windows.Media.Brushes.Red;
            entity.Marker = new CirclePointMarker();
            entity.Marker.Fill = System.Windows.Media.Brushes.Red;
            entity.Marker.Visibility = Visibility.Hidden;


            double maxx=double.MinValue, maxy = double.MinValue, minx=double.MaxValue, miny = double.MaxValue;

            List<HisDataPoint> ltmp = new List<HisDataPoint>();

            for (int i = 0; i < result.Count; i++)
            {
                object value;
                DateTime time;
                byte qu = 0;
                value = result.GetValue(i, out time, out qu);

                minx = minx > i ? i : minx;
                maxx = maxx < i ? i : maxx;

                double dtmp = ConvertValue(value);

                miny = miny > dtmp ? dtmp : miny;
                maxy =maxy < dtmp ? dtmp : maxy;

                PointC point = new PointC();
                point.X = i;
                point.Y = dtmp;
                point.Text = time.ToString("dd HH:mm:ss");
                entity.Source.Add(point);

                ltmp.Add(new HisDataPoint() { DateTime = time, Quality = qu, Value = value });
               /// mDatas.Add(new HisDataPoint() { DateTime = time, Quality = qu, Value = value });
            }
            Datas = ltmp;

            List<SplitItem> yitems = new List<SplitItem>(5);

            maxy = maxy * 1.2;
            miny = miny - Math.Abs(miny) * 0.2;

            MaxXValue = maxx;
            MaxYValue = maxy;
            MinXValue = minx;
            MinYValue = miny;
            ChartSource = new List<ICurveEntitySource>() { entity };

            var dval = (maxy - miny) / 5;
            for (int i = 1; i <= 5; i++)
            {
                yitems.Add(new SplitItem() { SpliteType = SplitItemType.Normal, Text = (miny + i * dval).ToString("f2"), IsShowText = true, Value = miny + i * dval, Color = System.Windows.Media.Brushes.SkyBlue });
            }
            yitems.Add(new SplitItem() { SpliteType = SplitItemType.HeighLight, Text = (miny + (maxy - miny) / 2).ToString("f2"), IsShowText = true, Value = miny + (maxy - miny) / 2, Color = System.Windows.Media.Brushes.DeepSkyBlue, IsShowTrangle = true });

            YLineItems = yitems;
            result.Dispose();
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
