using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
                var vtags = mProxy.QueryAllTagIdAndNames();
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

                var rds = mProxy.GetDriverRecordTypeTagIds();

                itmsg.Text = $"变量个数:{ mTagids.Count } 变量ID  Min:{mTagids.Values.Min() }   Max:{mTagids.Values.Max()}  驱动记录类型的变量个数:{rds.Count}";
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
                DateTime stime = DateTime.Parse(htimestart.Text);
                DateTime etime = DateTime.Parse(htimeend.Text);
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

                DateTime stime = DateTime.Parse(htimestart.Text);
                DateTime etime = DateTime.Parse(htimeend.Text);
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
    }
}
