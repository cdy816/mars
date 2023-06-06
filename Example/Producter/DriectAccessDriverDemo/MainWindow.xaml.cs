using System;
using System.Collections.Generic;
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

namespace DriectAccessDriverDemo
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private string mTagName;
        DirectAccess.Client mClient;
        public MainWindow()
        {
            InitializeComponent();
            hisvaluetime.Text = DateTime.Now.AddHours(-1).ToString();
        }

        private void Init()
        {
            mClient = new DirectAccess.Client(ipt.Text, int.Parse(portt.Text));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void hisvaluewrite_Click(object sender, RoutedEventArgs e)
        {
            if(mClient==null)
            {
                Init();
            }
            if(!mClient.IsLogined)
            {
                mClient.Login(username.Text, password.Text);
            }

            DateTime dt = DateTime.Parse(hisvaluetime.Text);
            int count = int.Parse(hisvaluecount.Text);
            int dur = int.Parse(hisvaluetimespan.Text);
            List<Tuple<DateTime,object, byte>> mvalues = new List<Tuple<DateTime, object, byte>>();
            System.Random rd = new Random((int)DateTime.Now.Ticks);
            for(int i=0;i<count;i++)
            {
                mvalues.Add(new Tuple<DateTime, object, byte>(dt, rd.NextDouble(), (byte)0));
                dt = dt.AddSeconds(dur);
            }
            var vals = new Dictionary<string, List<Tuple<DateTime, object, byte>>>();
            mTagName = tagt.Text;
            vals.Add(mTagName, mvalues);
            if (mClient.UpdateTagHisValue(vals))
            {
                MessageBox.Show("历史数据更新完成！");
            }
            else
            {
                MessageBox.Show("历史数据更新失败!");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void realvaluewrite_Click(object sender, RoutedEventArgs e)
        {
            if (mClient == null)
            {
                Init();
            }
            if (!mClient.IsLogined)
            {
                mClient.Login(username.Text, password.Text);
            }
            mTagName = tagt.Text;
            Task.Run(() => {
                System.Random rd = new Random((int)DateTime.Now.Ticks);
                while (true)
                {
                    var rdv = rd.NextDouble();
                    if(mClient.IsLogined)
                    {
                        mClient.UpdateTagValue(mTagName, rdv, 0);
                    }
                    Thread.Sleep(1000);
                }
            });
        }

        private void ReadHisVal_Click(object sender, RoutedEventArgs e)
        {

        }

        private void readRealValue_Click(object sender, RoutedEventArgs e)
        {
            var vals = mClient.GetTagRealValue(new List<string>() { tagt.Text });
            if(vals != null)
            {
                MessageBox.Show(vals.Values.First().Value.ToString());
            }
        }
    }
}
