using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace SpiderDriverDemo
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window,INotifyPropertyChanged
    {
        private SpiderDriver.ClientApi.DriverProxy driverProxy = new SpiderDriver.ClientApi.DriverProxy();
        private Dictionary<int,Tuple<string,byte>> mAllId = new Dictionary<int, Tuple<string, byte>>();
        private Thread mScanThread;
        private int mCount = 0;

        public event PropertyChangedEventHandler PropertyChanged;

        public MainWindow()
        {
            InitializeComponent();
            this.DataContext = this;
        }

        private void Start_Click(object sender, RoutedEventArgs e)
        {
            driverProxy = new SpiderDriver.ClientApi.DriverProxy();
            driverProxy.Connect(this.ipt.Text, int.Parse(portt.Text));
            driverProxy.ValueChanged = new SpiderDriver.ClientApi.DriverProxy.ProcessDataPushDelegate((values) => { 
                foreach(var vv in values)
                {
                    Debug.Print(vv.Key + "," + vv.Value.ToString());
                }
            });
            mScanThread = new Thread(ThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        private void ThreadPro()
        {
            while(true)
            {
                if (!driverProxy.IsLogin)
                {
                    if(driverProxy.IsConnected)
                    driverProxy.Login("Admin", "Admin");
                    if (driverProxy.IsLogin)
                    {
                        ReadAllIds();
                        driverProxy.AppendRegistorDataChangedCallBack(mAllId.Keys.ToList());
                    }
                }
                else
                {
                    ProcessSetTagValue();
                }
                Thread.Sleep(1000);
            }
        }

        private string mCountValue;

        /// <summary>
        /// 
        /// </summary>
        public string CountValue
        {
            get
            {
                return mCountValue;
            }
            set
            {
                if (mCountValue != value)
                {
                    mCountValue = value;
                    OnPropertyChanged("CountValue");
                }
            }
        }

        private string mSimValue;

        /// <summary>
        /// 
        /// </summary>
        public string SimValue
        {
            get
            {
                return mSimValue;
            }
            set
            {
                if (mSimValue != value)
                {
                    mSimValue = value;
                    OnPropertyChanged("SimValue");
                }
            }
        }

        private string mCosValue;


        /// <summary>
        /// 
        /// </summary>
        public string CosValue
        {
            get
            {
                return mCosValue;
            }
            set
            {
                if (mCosValue != value)
                {
                    mCosValue = value;
                    OnPropertyChanged("CosValue");
                }
            }
        }

        private string mBoolValue;

        /// <summary>
        /// 
        /// </summary>
        public string BoolValue
        {
            get
            {
                return mBoolValue;
            }
            set
            {
                if (mBoolValue != value)
                {
                    mBoolValue = value;
                    OnPropertyChanged("BoolValue");
                }
            }
        }

        private string mDateTimeValue;

        /// <summary>
        /// 
        /// </summary>
        public string DateTimeValue
        {
            get
            {
                return mDateTimeValue;
            }
            set
            {
                if (mDateTimeValue != value)
                {
                    mDateTimeValue = value;
                    OnPropertyChanged("DateTimeValue");
                }
            }
        }



        protected void OnPropertyChanged(string name)
        {
            PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
        }

        /// <summary>
        /// 
        /// </summary>
        private void ReadAllIds()
        {
            mAllId = driverProxy.QueryAllTagIdAndNames();

            if (mAllId.Count > 0)
            {
                var tagName = mAllId.First().Value.Item1;
                var ids = driverProxy.QueryTagId(new List<string>() { tagName });
                if (ids.Count > 0)
                {
                    //test
                }
            }
        }

        private void ProcessSetTagValue()
        {
            mCount++;
            if (mCount > 3600) mCount = 0;
            double sin = Math.Sin(mCount / 180.0 * Math.PI);
            double cos = Math.Cos(mCount / 180.0 * Math.PI);
            bool bval = mCount % 300 == 0;
            byte btmp = (byte)(mCount % 256);
            DateTime dnow = DateTime.Now;

            CountValue = mCount.ToString();
            SimValue = sin.ToString("f4");
            CosValue = cos.ToString("f4");
            BoolValue = bval.ToString();
            DateTimeValue = dnow.ToString();

            var values = new Dictionary<int, Tuple<Cdy.Tag.TagType, object>>();

            var hisvalus = new Dictionary<int, TagValueAndType>();

            foreach(var vv in mAllId)
            {
                switch ((TagType)vv.Value.Item2)
                {
                    case TagType.Double:
                        
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, sin));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = sin, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.Bool:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, bval));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = bval, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.Byte:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, btmp));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = btmp, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.DateTime:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, dnow));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = dnow, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.Float:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, cos));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = cos, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.Int:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, mCount));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = mCount, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.Long:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, mCount));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = mCount, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.UInt:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, mCount));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = mCount, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.ULong:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, mCount));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = mCount, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.UShort:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, mCount));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = mCount, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.Short:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, mCount));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = mCount, ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.IntPoint:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new IntPointData( mCount,mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new IntPointData(mCount, mCount), ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.UIntPoint:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new UIntPointData(mCount, mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new UIntPointData(mCount, mCount), ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.IntPoint3:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new IntPoint3Data(mCount, mCount, mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new IntPoint3Data(mCount, mCount, mCount), ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.UIntPoint3:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new UIntPoint3Data(mCount, mCount, mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new UIntPoint3Data(mCount, mCount, mCount), ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.LongPoint:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new LongPointData(mCount, mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new LongPointData(mCount, mCount), ValueType = (TagType)vv.Value.Item2 });

                        break;
                    case TagType.ULongPoint:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new ULongPointData(mCount, mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new ULongPointData(mCount, mCount), ValueType = (TagType)vv.Value.Item2 });

                        break;
                    case TagType.LongPoint3:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new LongPoint3Data(mCount, mCount, mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new LongPoint3Data(mCount, mCount, mCount), ValueType = (TagType)vv.Value.Item2 });
                        break;
                    case TagType.ULongPoint3:
                        values.Add(vv.Key, new Tuple<TagType, object>((TagType)vv.Value.Item2, new ULongPoint3Data(mCount, mCount, mCount)));
                        hisvalus.Add(vv.Key, new TagValueAndType() { Time = dnow, Quality = 0, Value = new ULongPoint3Data(mCount, mCount, mCount), ValueType = (TagType)vv.Value.Item2 });

                        break;
                }

            }
            driverProxy.SetTagValue(values);
            driverProxy.SetTagHisValue(hisvalus, 100, 5000);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void hisDataWrite_Click(object sender, RoutedEventArgs e)
        {
            List<TagValue> vals = new List<TagValue>();
            DateTime dt = DateTime.Now.AddSeconds(-300);
            Random rd = new Random((int)dt.Ticks);
            double dval = rd.NextDouble();
            for(int i=0;i<300;i++)
            {
                vals.Add(new TagValue() { Quality = 0, Time = dt.AddSeconds(i), Value = dval+i*1.0/10 });
            }

            driverProxy.SetTagHisValue(0, TagType.Double, vals);
        }
    }
}
