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
        private SpiderDriver.ClientApi.DriverProxy driverProxy;

        private SpiderDriver.ClientApi.DriverProxy mHisProxy;

        private Dictionary<int,Tuple<string,byte>> mAllId = new Dictionary<int, Tuple<string, byte>>();

        private SpiderDriver.ClientApi.RealDataBuffer rdb;

        private Thread mScanThread;
        private Thread mHisScanThread;
        private int mCount = 0;

        public event PropertyChangedEventHandler PropertyChanged;

        public MainWindow()
        {
            InitializeComponent();
            this.DataContext = this;
            InitHis();
        }

        private void Start_Click(object sender, RoutedEventArgs e)
        {
            this.Start.IsEnabled = false;
            if (driverProxy == null)
            {
                driverProxy = new SpiderDriver.ClientApi.DriverProxy();
                driverProxy.Open(this.ipt.Text, int.Parse(portt.Text));
                driverProxy.ValueChanged = new SpiderDriver.ClientApi.DriverProxy.ProcessDataPushDelegate((values) =>
                {
                    foreach (var vv in values)
                    {
                        Debug.Print("收到数据下发指令:"+ vv.Key + "," + vv.Value.ToString());
                    }
                });
                driverProxy.DatabaseChanged = new SpiderDriver.ClientApi.DriverProxy.DatabaseChangedDelegate((real, his) => {
                    Debug.Print(real+ ","+his);
                });
            }

            mScanThread = new Thread(RealValueThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
        }

        private void HisStart_Click(object sender, RoutedEventArgs e)
        {
            this.HisStart.IsEnabled = false;
            if (driverProxy == null)
            {
                driverProxy = new SpiderDriver.ClientApi.DriverProxy();
                driverProxy.Open(this.ipt.Text, int.Parse(portt.Text));
                driverProxy.ValueChanged = new SpiderDriver.ClientApi.DriverProxy.ProcessDataPushDelegate((values) =>
                {
                    foreach (var vv in values)
                    {
                        Debug.Print("收到数据下发指令:" + vv.Key + "," + vv.Value.ToString());
                    }
                });
            }
            mHisScanThread = new Thread(HisValueThreadPro);
            mHisScanThread.IsBackground = true;
            mHisScanThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        private void RealValueThreadPro()
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
                    ProcessSetRealTagValue();
                }
                Thread.Sleep(500);
            }
        }


        private void RealValueUpdateByNameThreadPro()
        {
            driverProxy.UserName = "Admin";
            driverProxy.Password = "Admin";
            while (true)
            {
                ProcessSetRealTagValueWithTagName();
                Thread.Sleep(500);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void HisValueThreadPro()
        {
            while(true)
            {
                if (!driverProxy.IsLogin)
                {
                    if (driverProxy.IsConnected)
                        driverProxy.Login("Admin", "Admin");
                    if (driverProxy.IsLogin)
                    {
                        ReadAllIds();
                      //  driverProxy.AppendRegistorDataChangedCallBack(mAllId.Keys.ToList());
                    }
                }
                else
                {
                    ProcessSetHisTagValue();
                }
                Thread.Sleep(2000);
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
            mAllId = driverProxy.QueryAllTagIdAndNames(6000000);

            if (mAllId.Count > 0)
            {
                var tagName = mAllId.First().Value.Item1;
                var ids = driverProxy.QueryTagId(new List<string>() { tagName });
                if (ids.Count > 0)
                {
                    //test
                }

                rdb = new SpiderDriver.ClientApi.RealDataBuffer(mAllId.Count * 32);
            }

            var driverrecordTags = driverProxy.GetDriverRecordTypeTagIds();
            foreach(var vv in driverrecordTags)
            {
                //to do here
            }

            var vvd = driverProxy.CheckRecordTypeByTagId(new List<int>() { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 });
        }

        private void ProcessSetRealTagValueWithTagName()
        {
            mCount++;
            if (mCount > 3600) mCount = 0;
            double sin = Math.Sin(mCount / 180.0 * Math.PI);
            double cos = Math.Cos(mCount / 180.0 * Math.PI);
            bool bval = mCount % 300 == 0;
            byte btmp = (byte)(mCount % 256);
            DateTime dnow = DateTime.UtcNow;

            CountValue = mCount.ToString();
            SimValue = sin.ToString("f4");
            CosValue = cos.ToString("f4");
            BoolValue = bval.ToString();
            DateTimeValue = dnow.ToString();
            if (rdb == null)
            {
                rdb = new SpiderDriver.ClientApi.RealDataBuffer(100*(64+32));
            }

            rdb.Clear();

            Stopwatch sw = new Stopwatch();
            sw.Start();
            //int i = 0;
            StringBuilder sb = new StringBuilder();
            sb.Append(" : ");
            //方法1
            for (int i = 0; i < 100; i++)
            {
                rdb.AppendValue("tag" + i, sin, 0);
            }

            //driverProxy.SetTagValueAndQuality2(rdb);

            //方法2
            List<RealTagValue2> rvals = new List<RealTagValue2>();
            for(int i=0;i<100;i++)
            {
                rvals.Add(new RealTagValue2() { Id = "tag" + i, Value = sin, ValueType = 8, Quality = 0 });
            }
            //driverProxy.SetTagValueAndQuality2(rvals);

            //方法3
            driverProxy.SetTagRealAndHisValue2(rdb);

            //方法4
            //driverProxy.SetTagRealAndHisValue2(rvals);

            this.Dispatcher.BeginInvoke(new Action(() => {

                this.fshs.Content = "发送耗时:" + sw.ElapsedMilliseconds + sb.ToString();
            }));

            // Debug.Print("发送耗时:" + sw.ElapsedMilliseconds +sb.ToString());
        }

        private void ProcessSetRealTagValue()
        {
            mCount++;
            if (mCount > 3600) mCount = 0;
            double sin = Math.Sin(mCount / 180.0 * Math.PI);
            double cos = Math.Cos(mCount / 180.0 * Math.PI);
            bool bval = mCount % 300 == 0;
            byte btmp = (byte)(mCount % 256);
            DateTime dnow = DateTime.UtcNow;

            CountValue = mCount.ToString();
            SimValue = sin.ToString("f4");
            CosValue = cos.ToString("f4");
            BoolValue = bval.ToString();
            DateTimeValue = dnow.ToString();
            if (rdb == null) return;

            rdb.Clear();

            Stopwatch sw = new Stopwatch();
            sw.Start();
            int i = 0;
            StringBuilder sb = new StringBuilder();
            sb.Append(" : ");
            foreach(var vv in mAllId)
            {
                switch ((TagType)vv.Value.Item2)
                {
                    case TagType.Double:
                        rdb.AppendValue(vv.Key,sin,0);
                      
                        break;
                    case TagType.Bool:
                        rdb.AppendValue(vv.Key, bval, 0);
                      
                        break;
                    case TagType.Byte:
                        rdb.AppendValue(vv.Key, btmp, 0);
                       
                        break;
                    case TagType.DateTime:
                        rdb.AppendValue(vv.Key, dnow, 0);
                        
                        break;
                    case TagType.Float:
                        rdb.AppendValue(vv.Key, (float)cos, 0);
                       
                        break;
                    case TagType.Int:
                        rdb.AppendValue(vv.Key, mCount, 0);
                        break;
                    case TagType.Long:
                        rdb.AppendValue(vv.Key, (long)mCount, 0);
                        break;
                    case TagType.UInt:
                        rdb.AppendValue(vv.Key, (uint)mCount, 0);
                        break;
                    case TagType.ULong:
                        rdb.AppendValue(vv.Key, (ulong)mCount, 0);
                        break;
                    case TagType.UShort:
                        rdb.AppendValue(vv.Key, (ushort)mCount, 0);
                        break;
                    case TagType.Short:
                        rdb.AppendValue(vv.Key, (short)mCount, 0);
                        break;
                    case TagType.IntPoint:
                        rdb.AppendValue(vv.Key, new IntPointData(mCount, mCount), 0);
                        break;
                    case TagType.UIntPoint:
                        rdb.AppendValue(vv.Key, new UIntPointData(mCount, mCount), 0);
                        break;
                    case TagType.IntPoint3:
                        rdb.AppendValue(vv.Key, new IntPoint3Data(mCount, mCount, mCount), 0);
                        break;
                    case TagType.UIntPoint3:
                        rdb.AppendValue(vv.Key, new UIntPoint3Data(mCount, mCount, mCount), 0);
                        break;
                    case TagType.LongPoint:
                        rdb.AppendValue(vv.Key, new LongPointData(mCount, mCount), 0);
                        break;
                    case TagType.ULongPoint:
                        rdb.AppendValue(vv.Key, new ULongPointData(mCount, mCount), 0);
                        break;
                    case TagType.LongPoint3:
                        rdb.AppendValue(vv.Key, new LongPoint3Data(mCount, mCount, mCount), 0);
                        break;
                    case TagType.ULongPoint3:
                        rdb.AppendValue(vv.Key, new ULongPoint3Data(mCount, mCount, mCount), 0);
                        break;
                }
                i++;
                if (i % 1000000 == 0)
                {
                    var vtmp = sw.ElapsedMilliseconds;
                    driverProxy.SetTagValueAndQuality(rdb);
                    sb.Append(sw.ElapsedMilliseconds - vtmp+",");
                    rdb.Clear();
                }
            }
            if(i % 1000000 != 0)
            driverProxy.SetTagValueAndQuality(rdb);
            sw.Stop();

            this.Dispatcher.BeginInvoke(new Action(() => {

                this.fshs.Content = "发送耗时:" + sw.ElapsedMilliseconds + sb.ToString();
            }));

           // Debug.Print("发送耗时:" + sw.ElapsedMilliseconds +sb.ToString());
        }


        private void ProcessSetHisTagValue()
        {
            mCount++;
            if (mCount > 3600) mCount = 0;
            double sin = Math.Sin(mCount / 180.0 * Math.PI);
            double cos = Math.Cos(mCount / 180.0 * Math.PI);
            bool bval = mCount % 300 == 0;
            byte btmp = (byte)(mCount % 256);
            DateTime dnow = DateTime.UtcNow;

            CountValue = mCount.ToString();
            SimValue = sin.ToString("f4");
            CosValue = cos.ToString("f4");
            BoolValue = bval.ToString();
            DateTimeValue = dnow.ToString();
            if (rdb == null) return;

            rdb.Clear();

            Stopwatch sw = new Stopwatch();
            sw.Start();

            int i = 0;
            foreach (var vv in mAllId)
            {
                switch ((TagType)vv.Value.Item2)
                {
                    case TagType.Double:
                        rdb.AppendValue(vv.Key, sin,0);
                        break;
                    case TagType.Bool:
                        rdb.AppendValue(vv.Key, bval, 0);
                        break;
                    case TagType.Byte:
                        rdb.AppendValue(vv.Key, btmp, 0);
                        break;
                    case TagType.DateTime:
                        rdb.AppendValue(vv.Key, dnow, 0);
                        break;
                    case TagType.Float:
                        rdb.AppendValue(vv.Key, (float)cos, 0);
                        break;
                    case TagType.Int:
                        rdb.AppendValue(vv.Key, mCount, 0);
                        break;
                    case TagType.Long:
                        rdb.AppendValue(vv.Key, (long)mCount, 0);
                        break;
                    case TagType.UInt:
                        rdb.AppendValue(vv.Key, (uint)mCount, 0);
                        break;
                    case TagType.ULong:
                        rdb.AppendValue(vv.Key, (ulong)mCount, 0);
                        break;
                    case TagType.UShort:
                        rdb.AppendValue(vv.Key, (ushort)mCount, 0);
                        break;
                    case TagType.Short:
                        rdb.AppendValue(vv.Key, (short)mCount, 0);
                        break;
                    case TagType.IntPoint:
                        rdb.AppendValue(vv.Key, new IntPointData(mCount, mCount), 0);
                        break;
                    case TagType.UIntPoint:
                        rdb.AppendValue(vv.Key, new UIntPointData(mCount, mCount), 0);
                        break;
                    case TagType.IntPoint3:
                        rdb.AppendValue(vv.Key, new IntPoint3Data(mCount, mCount, mCount), 0);
                        break;
                    case TagType.UIntPoint3:
                        rdb.AppendValue(vv.Key, new UIntPoint3Data(mCount, mCount, mCount), 0);
                        break;
                    case TagType.LongPoint:
                        rdb.AppendValue(vv.Key, new LongPointData(mCount, mCount), 0);
                        break;
                    case TagType.ULongPoint:
                        rdb.AppendValue(vv.Key, new ULongPointData(mCount, mCount), 0);
                        break;
                    case TagType.LongPoint3:
                        rdb.AppendValue(vv.Key, new LongPoint3Data(mCount, mCount, mCount), 0);
                        break;
                    case TagType.ULongPoint3:
                        rdb.AppendValue(vv.Key, new ULongPoint3Data(mCount, mCount, mCount), 0);
                        break;
                }
                i++;
                if (i % 1000000 == 0)
                {
                    driverProxy.SetTagRealAndHisValue(rdb);
                    rdb.Clear();
                }
            }
            if (i % 1000000 != 0)
                driverProxy.SetTagRealAndHisValue(rdb);
            sw.Stop();
            Debug.Print(DateTime.Now.ToString() + "  发送耗时:  " + sw.ElapsedMilliseconds);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void hisDataWrite_Click(object sender, RoutedEventArgs e)
        {
            List<TagValue> vals = new List<TagValue>();
            DateTime dt = DateTime.UtcNow.AddSeconds(-30000);

            StringBuilder sb = new StringBuilder();
            sb.Append(dt.ToLocalTime().ToString() + "   " + DateTime.Now.ToString());

            Random rd = new Random((int)dt.Ticks);
            double dval = rd.NextDouble();
            for(int i=0;i<30000;i++)
            {
                vals.Add(new TagValue() { Quality = 0, Time = dt.AddSeconds(i), Value = dval+i*1.0/10 });
            }
            //for(int i=6;i<7;i++)
            driverProxy.SetTagHisValue(int.Parse(Hid.Text), TagType.Double, vals);

            MessageBox.Show(sb.ToString());

        }

        private void StartPushOnly_Click(object sender, RoutedEventArgs e)
        {
            this.StartPushOnly.IsEnabled = false;
            if (driverProxy == null)
            {
                driverProxy = new SpiderDriver.ClientApi.DriverProxy();
                driverProxy.Open(this.ipt.Text, int.Parse(portt.Text));
            }
            mScanThread = new Thread(RealValueUpdateByNameThreadPro);
            mScanThread.IsBackground = true;
            mScanThread.Start();
        }

        private void InitHis()
        {
            histag.Text = "0";
            histime.Text=DateTime.Now.TimeOfDay.ToString();
            hisdate.SelectedDate = DateTime.Now.Date;
            
        }

        private void hisconn_Click(object sender, RoutedEventArgs e)
        {
            mHisProxy = new SpiderDriver.ClientApi.DriverProxy();
            mHisProxy.Open(this.ipt2.Text, int.Parse(port2.Text));

            mHisProxy.Login("Admin", "Admin");

            if(mHisProxy.IsLogin)
            {
                MessageBox.Show("登录成功!");
            }
            else
            {
                MessageBox.Show("登录失败!");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void hisset_Click(object sender, RoutedEventArgs e)
        {
            mHisProxy.SetTagHisValue(int.Parse(histag.Text), new TagValueAndType() { Quality = 0, Time = hisdate.SelectedDate.Value.Add(TimeSpan.Parse(histime.Text)).ToUniversalTime(),Value=double.Parse(hisval.Text),ValueType = TagType.Double });
        }
    }
}
