using Cdy.Tag;
using Cdy.Tag.Common;
using DirectAccessDriver.ClientApi;
using Google.Protobuf.WellKnownTypes;
using System;
using System.Collections.Generic;
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

namespace DirectAccessDriverApiDemo
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        Dictionary<string, int> mTagids = new Dictionary<string, int>();

        Dictionary<int, int> mTagTypes = new Dictionary<int, int>();

        Dictionary<int, string> mTagNames = new Dictionary<int, string>();

        private DirectAccessDriver.ClientApi.DriverProxy mProxy;
        public MainWindow()
        {
            InitializeComponent();
            htimestart.Text = DateTime.Now.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss");
            htimeend.Text = DateTime.Now.AddHours(-1).ToString("yyyy-MM-dd HH:mm:ss");
            tc.IsEnabled = false;

        }

       

        private void Login_Click(object sender, RoutedEventArgs e)
        {
            mProxy = new DirectAccessDriver.ClientApi.DriverProxy();
            string[] svstr = this.server.Text.Split(":");
            mProxy.Open(svstr[0], int.Parse(svstr[1]));
            mProxy.Login(user.Text, pass.Text);

            tc.IsEnabled = mProxy.IsLogin;
            if(mProxy.IsLogin)
            {
                MessageBox.Show("登录成功!");
            }
            else
            {
                MessageBox.Show("登录失败!");
            }

            //订购变量值改变通知
            mProxy.ValueChanged = new DirectAccessDriver.ClientApi.DriverProxy.ProcessDataPushDelegate((vals) => {
                StringBuilder sb = new StringBuilder();
                if(vals!=null)
                {
                    foreach(var vv in vals)
                    {
                        sb.AppendLine($"变量:{vv.Key} 值:{vv.Value}");
                    }
                }
                this.Dispatcher.Invoke(() => {
                    this.valchangemsg.AppendText(sb.ToString());
                });
            });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void QueryAllTag_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                string database = mProxy.GetDatabseName();
                var vtags = mProxy.QueryAllTagIdAndNames(50000);
                mTagids.Clear();
                mTagTypes.Clear();
                mTagNames.Clear();

                if (vtags != null)
                {
                    foreach (var vv in vtags)
                    {
                        mTagids.Add(vv.Value.Item1, vv.Key);
                        mTagNames.Add(vv.Key, vv.Value.Item1);
                        mTagTypes.Add(vv.Key, vv.Value.Item2);
                    }
                }

                var rds = mProxy.GetDriverRecordTypeTagIds(50000);

                if(mTagids.Count>0)
                {
                    htagname.ItemsSource = mTagids.Keys;
                    itmsg.Text = $"数据库{database}  变量个数:{mTagids.Count} 变量ID  Min:{mTagids.Values.Min()}   Max:{mTagids.Values.Max()}  驱动记录类型的变量个数:{rds.Count}";
                }
               
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        /// <summary>
        /// 写入一段历史数据
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void hsetval_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                int sname = mTagids[htagname.Text];
                DateTime stime = DateTime.Parse(htimestart.Text).ToUniversalTime();
                DateTime etime = DateTime.Parse(htimeend.Text).ToUniversalTime();
                int spen = int.Parse(htimspan.Text);
                Dictionary<DateTime, double> values = new Dictionary<DateTime, double>();
                //Random rd = new Random((int)DateTime.Now.Ticks);
                int i = 0;
                DateTime dt = stime;
                while (dt <= etime)
                {
                    values.Add(dt, i);
                    dt = dt.AddSeconds(spen);
                    i++;
                }

                DirectAccessDriver.ClientApi.HisDataBuffer hbuffer = new DirectAccessDriver.ClientApi.HisDataBuffer();
                foreach (var vv in values)
                {
                    hbuffer.AppendValue(vv.Key, vv.Value, 0);
                }

                mProxy.SetTagHisValue(sname, (Cdy.Tag.TagType)mTagTypes[sname], hbuffer);
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message);
            }

        }

        /// <summary>
        /// 写入实时数据
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void settagvalueb_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (tagmodsel.IsChecked.Value)
                {
                    int id = mTagids[rtname.Text];
                    double dval = double.Parse(rtval.Text);
                    mProxy.SetTagValueAndQuality(new List<RealTagValue>() { new RealTagValue() { Id = id, Value = dval, ValueType = 8, Quality = 0 } });
                }
                else
                {
                    DirectAccessDriver.ClientApi.RealDataBuffer rbuffer = new DirectAccessDriver.ClientApi.RealDataBuffer();
                    double dval = double.Parse(rtval.Text);
                    for (int i = int.Parse(rtsid.Text); i <= int.Parse(rteid.Text); i++)
                    {
                        rbuffer.AppendValue(i, dval, 0);
                    }
                    mProxy.SetTagValueAndQuality(rbuffer);
                }
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void settagvalb2_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (tagmodsel.IsChecked.Value)
                {
                    int id = mTagids[rtname.Text];
                    double dval = double.Parse(rtval.Text);
                    mProxy.SetTagRealAndHisValue(new List<RealTagValue>() { new RealTagValue() { Id = id, Value = dval, ValueType = 8, Quality = 0 } });
                }
                else
                {
                    DirectAccessDriver.ClientApi.RealDataBuffer rbuffer = new DirectAccessDriver.ClientApi.RealDataBuffer();
                    double dval = double.Parse(rtval.Text);
                    for (int i = int.Parse(rtsid.Text); i <= int.Parse(rteid.Text); i++)
                    {
                        rbuffer.AppendValue(i, dval, 0);
                    }
                    mProxy.SetTagRealAndHisValue(rbuffer);
                }
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        /// <summary>
        /// 订购值改变
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void regb_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                var vatgs = rntagname.Text.Split(",").Select(e => mTagids[e]);
                mProxy.AppendRegistorDataChangedCallBack(vatgs);
                MessageBox.Show("订购完成!");
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void rdg_Click(object sender, RoutedEventArgs e)
        {
            rtval.Text = new Random((int)DateTime.Now.Ticks).NextDouble().ToString();
        }

        private void settagvaluebonedirection_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (tagmodsel.IsChecked.Value)
                {
                    int id = mTagids[rtname.Text];
                    double dval = double.Parse(rtval.Text);
                    mProxy.SetTagValueAndQuality2(new List<RealTagValue2>() { new RealTagValue2() { Id = rtname.Text, Value = dval, ValueType = 8, Quality = 0 } });
                }
            }
            catch(Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        /// <summary>
        /// 一次写入多个变量的一段的历史值
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void hsetval2_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                int sid = int.Parse(htagnames.Text);
                int eid = int.Parse(htagnamee.Text);

                DateTime stime = DateTime.Parse(htimestart.Text).ToUniversalTime();
                DateTime etime = DateTime.Parse(htimeend.Text).ToUniversalTime();
                int spen = int.Parse(htimspan.Text);
                Dictionary<DateTime, double> values = new Dictionary<DateTime, double>();
                Random rd = new Random((int)DateTime.Now.Ticks);
                DateTime dt = stime;
                while (dt <= etime)
                {
                    values.Add(dt, rd.NextDouble());
                    dt = dt.AddSeconds(spen);
                }


                DirectAccessDriver.ClientApi.HisDataBuffer hbuffer = new DirectAccessDriver.ClientApi.HisDataBuffer();
                int cc = 0;
                for (int i = sid; i <= eid; i++)
                {
                    hbuffer.Write(i);
                    hbuffer.Write(values.Count);
                    hbuffer.Write((byte)TagType.Double);
                    foreach (var vv in values)
                    {
                        hbuffer.AppendValue(vv.Key, vv.Value, 0);
                    }
                    cc++;
                }

                mProxy.SetMutiTagHisValue(hbuffer, cc);
               
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void settagvaluetime_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (tagmodsel.IsChecked.Value)
                {
                    int id = mTagids[rtname.Text];
                    double dval = double.Parse(rtval.Text);
                    mProxy.SetTagRealAndHisValueWithTimer(new List<RealTagValueWithTimer>() { new RealTagValueWithTimer() { Id = id, Value = dval, ValueType = 8, Quality = 0,Time = DateTime.UtcNow } });
                }
                else
                {
                    DirectAccessDriver.ClientApi.RealDataBuffer rbuffer = new DirectAccessDriver.ClientApi.RealDataBuffer();
                    double dval = double.Parse(rtval.Text);
                    for (int i = int.Parse(rtsid.Text); i <= int.Parse(rteid.Text); i++)
                    {
                        rbuffer.AppendValue(i, dval, 0);
                    }
                    mProxy.SetTagValueAndQuality(rbuffer);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void settagvaluetime2_Click(object sender, RoutedEventArgs e)
        {
            try
            {
                if (tagmodsel.IsChecked.Value)
                {
                    int id = mTagids[rtname.Text];
                    double dval = double.Parse(rtval.Text);
                    mProxy.SetTagValueTimerAndQuality(new List<RealTagValueWithTimer>() { new RealTagValueWithTimer() { Id = id, Value = dval, ValueType = 8, Quality = 0, Time = DateTime.UtcNow } });
                }
                else
                {
                    DirectAccessDriver.ClientApi.RealDataBuffer rbuffer = new DirectAccessDriver.ClientApi.RealDataBuffer();
                    double dval = double.Parse(rtval.Text);
                    for (int i = int.Parse(rtsid.Text); i <= int.Parse(rteid.Text); i++)
                    {
                        rbuffer.AppendValue(i, dval, 0);
                    }
                    mProxy.SetTagValueAndQuality(rbuffer);
                }
            }
            catch (Exception ex)
            {
                MessageBox.Show(ex.Message);
            }
        }

        private void SettagvalueforScan_Click(object sender, RoutedEventArgs e)
        {
            if (tagmodsel.IsChecked.Value)
            {
                int id = mTagids[rtname.Text];

                Task.Run(() => { 
                
                    while(true)
                    {
                        DateTime dt = DateTime.Now;
                        mProxy.SetTagRealAndHisValueWithTimer(new List<RealTagValueWithTimer>() { new RealTagValueWithTimer() { Id = id, Value = dt.Second, ValueType = 8, Quality = 0, Time = DateTime.UtcNow.AddSeconds(10) } }) ;
                        Thread.Sleep(1000);
                    }
                
                });

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void StartWrite_Click(object sender, RoutedEventArgs e)
        {
            ProcessTagHisValueSet2();
        }

        private void ProcessTagHisValueSet()
        {
            int sid = int.Parse(fromId.Text);
            int eid = int.Parse(toId.Text);
            int circle = int.Parse(writeCircle.Text);
            DirectAccessDriver.ClientApi.RealDataBuffer hbuffer = new DirectAccessDriver.ClientApi.RealDataBuffer(eid * 32);
            Task.Run(() => {

                int valvount = 0;
                while (true)
                {
                    mProxy.CheckAndRemoveTimeoutData();

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    hbuffer.Clear();
                    DateTime date = DateTime.UtcNow;
                    int cc = 0;
                    double dval = Math.Sin(valvount / 180.0 * Math.PI);
                    for (int i = sid; i <= eid; i++)
                    {
                        //hbuffer.AppendTagValueAndQuality(i, TagType.Double, dval, 0);
                        hbuffer.AppendTagValueAndQualityWithTimer(i, TagType.Double, dval, date, 0);
                        cc++;
                    }
                    long ltmp = sw.ElapsedMilliseconds;

                    mProxy.SetTagRealAndHisValueWithTimerAsync(hbuffer, (result) => {
                        sw.Stop();
                        if (result)
                        {
                            string slog = $"{valvount} {date} 写入值:{dval} 数据准备耗时:{ltmp}ms  写入耗时:{sw.ElapsedMilliseconds - ltmp} ms " + "\r\n";
                            this.Dispatcher.Invoke(new Action(() =>
                            {
                                if (valvount % 100 == 0)
                                {
                                    log.SelectAll();
                                    log.Selection.Text = "\r\n";
                                }
                                log.AppendText(slog);
                                log.ScrollToEnd();

                            }));
                        }
                        else
                        {
                            string slog = $"写入超时 " + "\r\n";
                            this.Dispatcher.Invoke(new Action(() =>
                            {
                                log.AppendText(slog);
                                log.ScrollToEnd();
                            }));
                        }
                    }, 5000);
                    valvount++;
                    Thread.Sleep(circle * 1000);
                }
            });
        }

        private void ProcessTagHisValueSet2()
        {
            int sid = int.Parse(fromId.Text);
            int eid = int.Parse(toId.Text);
            int circle = int.Parse(writeCircle.Text);
            List<RealTagValueWithTimer> hbuffer = new List<RealTagValueWithTimer>(eid * 32);
            Task.Run(() => {

                int valvount = 0;
                while (true)
                {
                    mProxy.CheckAndRemoveTimeoutData();

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    hbuffer.Clear();
                    DateTime date = DateTime.UtcNow;
                    int cc = 0;
                    double dval = Math.Sin(valvount / 180.0 * Math.PI);
                    for (int i = sid; i <= eid; i++)
                    {
                        hbuffer.Add(new RealTagValueWithTimer() { Id = i,ValueType = (byte)TagType.Double , Quality = 0, Time = date, Value = dval });
                        //hbuffer.AppendTagValueAndQuality(i, TagType.Double, dval, 0);
                        //hbuffer.AppendTagValueAndQualityWithTimer(i, TagType.Double, dval, date, 0);
                        cc++;
                    }
                    long ltmp = sw.ElapsedMilliseconds;

                    mProxy.SetTagRealAndHisValueWithTimerAsync(hbuffer, (result) => {
                        sw.Stop();
                        if (result)
                        {
                            string slog = $"{valvount} {date} 写入值:{dval} 数据准备耗时:{ltmp}ms  写入耗时:{sw.ElapsedMilliseconds - ltmp} ms " + "\r\n";
                            this.Dispatcher.Invoke(new Action(() =>
                            {
                                if (valvount % 100 == 0)
                                {
                                    log.SelectAll();
                                    log.Selection.Text = "\r\n";
                                }
                                log.AppendText(slog);
                                log.ScrollToEnd();

                            }));
                        }
                        else
                        {
                            string slog = $"写入超时 " + "\r\n";
                            this.Dispatcher.Invoke(new Action(() =>
                            {
                                log.AppendText(slog);
                                log.ScrollToEnd();
                            }));
                        }
                    }, 5000);
                    valvount++;
                    Thread.Sleep(circle * 1000);
                }
            });
        }

        private void gettagvalue_Click(object sender, RoutedEventArgs e)
        {
            int id = mTagids[rtname.Text];
            double dval = double.Parse(rtval.Text);
            var vdatas = mProxy.GetRealData(new List<int>() { id });
            if(vdatas != null && vdatas.Count>0)
            {
                MessageBox.Show(vdatas.First().Value.Value.ToString());
            }
        }

        private void hgetval_Click(object sender, RoutedEventArgs e)
        {
            DateTime stime = DateTime.Parse(htimestart.Text).ToUniversalTime();
            DateTime etime = DateTime.Parse(htimeend.Text).ToUniversalTime();

            int spen = int.Parse(htimspan.Text);
            List<DateTime> values = new List<DateTime>();
            DateTime dt = stime;
            while (dt <= etime)
            {
                values.Add(dt);
                dt = dt.AddSeconds(spen);
            }
            int sname = mTagids[htagname.Text];
            var vals = mProxy.QueryHisValueAtTimes<double>(sname, values, QueryValueMatchType.Previous);
            if (vals != null)
            {
                StringBuilder sb = new StringBuilder();
                foreach (var vv in vals.ListAvaiableValues())
                {
                    sb.AppendLine(vv.Time + " " + vv.Value + " " + vv.Quality);
                }
                Trace.WriteLine(sb.ToString());
            }
        }

        private void hgetallval_Click(object sender, RoutedEventArgs e)
        {
            DateTime stime = DateTime.Parse(htimestart.Text).ToUniversalTime();
            DateTime etime = DateTime.Parse(htimeend.Text).ToUniversalTime();

            int sname = mTagids[htagname.Text];
            var vals =  mProxy.QueryAllHisValue<double>(sname, stime, etime);
            if (vals != null)
            {
                StringBuilder sb = new StringBuilder();
                foreach (var vv in vals.ListAvaiableValues())
                {
                    sb.AppendLine(vv.Time + " " + vv.Value + " " + vv.Quality);
                }
                Trace.WriteLine(sb.ToString());
            }
        }

        private void hgetval2_Click(object sender, RoutedEventArgs e)
        {
            DateTime stime = DateTime.Parse(htimestart.Text).ToUniversalTime();
            DateTime etime = DateTime.Parse(htimeend.Text).ToUniversalTime();

            int spen = int.Parse(htimspan.Text);
            int sname = mTagids[htagname.Text];
            var vals = mProxy.QueryHisValueForTimeSpan<double>(sname, stime,etime,new TimeSpan(0,0, spen), QueryValueMatchType.Previous);
            if (vals != null)
            {
                StringBuilder sb = new StringBuilder();
                foreach (var vv in vals.ListAvaiableValues())
                {
                    sb.AppendLine(vv.Time + " " + vv.Value + " " + vv.Quality);
                }
                Trace.WriteLine(sb.ToString());
            }
        }

        private void StartWriteSync_Click(object sender, RoutedEventArgs e)
        {
            int sid = int.Parse(fromId.Text);
            int eid = int.Parse(toId.Text);
            int circle = int.Parse(writeCircle.Text);
            List<RealTagValueWithTimer> hbuffer = new List<RealTagValueWithTimer>(eid * 32);
            Task.Run(() => {

                int valvount = 0;
                while (true)
                {
                    mProxy.CheckAndRemoveTimeoutData();

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    hbuffer.Clear();
                    DateTime date = DateTime.UtcNow;
                    int cc = 0;
                    double dval = Math.Sin(valvount / 180.0 * Math.PI);
                    for (int i = sid; i <= eid; i++)
                    {
                        hbuffer.Add(new RealTagValueWithTimer() { Id = i, ValueType = (byte)TagType.Double, Quality = 0, Time = date, Value = dval });
                        cc++;
                    }
                    long ltmp = sw.ElapsedMilliseconds;

                    var result = mProxy.SetTagRealAndHisValueWithTimer(hbuffer);

                    sw.Stop();
                    if (result)
                    {
                        string slog = $"{valvount} {date} 写入值:{dval} 数据准备耗时:{ltmp}ms  写入耗时:{sw.ElapsedMilliseconds - ltmp} ms " + "\r\n";
                        this.Dispatcher.Invoke(new Action(() =>
                        {
                            if (valvount % 100 == 0)
                            {
                                log.SelectAll();
                                log.Selection.Text = "\r\n";
                            }
                            log.AppendText(slog);
                            log.ScrollToEnd();

                        }));
                    }
                    else
                    {
                        string slog = $"写入超时 " + "\r\n";
                        this.Dispatcher.Invoke(new Action(() =>
                        {
                            log.AppendText(slog);
                            log.ScrollToEnd();
                        }));
                    }
                    valvount++;
                    Thread.Sleep(circle * 1000);
                }
            });
        }

        private void tbutton_Click(object sender, RoutedEventArgs e)
        {
            int ifv = int.Parse(tf.Text);
            int itv = int .Parse(tt.Text);
            Task.Run(() =>
            {
                while (true)
                {
                    List<RealTagValue> values = new List<RealTagValue>();
                    for (int i = ifv; i < itv; i++)
                    {
                        double dval = Random.Shared.NextDouble();
                        values.Add(new RealTagValue() { Id = i, Value = dval, ValueType = (byte)TagType.Double, Quality = 0 });
                    }
                    mProxy.SetAreaTagHisValue(DateTime.UtcNow, values);
                    Thread.Sleep(1000);
                }
            });
        }

        private void sqlQuery_Click(object sender, RoutedEventArgs e)
        {
            var vals = mProxy.QueryHisValueBySql(sqlexp.Text);
            if (vals is HisQueryTableResult)
            {
                foreach (var vv in (vals as HisQueryTableResult).ReadRows())
                {
                    StringBuilder sb = new StringBuilder();
                    sb.Append(vv.Item1.ToString());
                    foreach (var vv2 in vv.Item2)
                    {
                        sb.Append($",{vv2}");
                    }
                    Debug.WriteLine(sb.ToString());
                }
            }
            else if(vals is List<double>)
            {
                foreach(var vv in vals as List<double>)
                {
                    Debug.Write(vv+",");
                }
                Debug.WriteLine("");
               
            }
            else if(vals is Dictionary<int, TagRealValue>)
            {
                foreach (var vv in vals as Dictionary<int, TagRealValue>)
                {
                    Debug.WriteLine(vv.Value.Value + ",");
                }
            }
        }
    }
}
