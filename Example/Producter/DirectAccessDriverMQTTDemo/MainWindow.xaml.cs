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
using System.Xml.Linq;

namespace DirectAccessDriverMQTTDemo
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        DirectAccessMqtt.Client.Client mclient;
        public MainWindow()
        {
            InitializeComponent();
            mclient = new DirectAccessMqtt.Client.Client();
        }

        private void loginb_Click(object sender, RoutedEventArgs e)
        {
            mclient.Server = serverip.Text;
            mclient.Port = int.Parse(serverport.Text);
            mclient.UserName = serverusername.Text;
            mclient.Password = serverpass.Text;

            mclient.ServerTopic = servertopic.Text;
            mclient.ServerUser = dbuser.Text;
            mclient.ServerPassword = dbpass.Text;
            mclient.Connect();
            mclient.LoginChangedEvent += (islogin) => {
                if (mclient.IsLogin)
                    MessageBox.Show("登录成功!");
            };
          
        }

        private void updateb_Click(object sender, RoutedEventArgs e)
        {
           string[] sname =tagname.Text.Split(',');
            Dictionary<string,object> dtmp = new Dictionary<string,object>();
            foreach (string s in sname)
            {
                dtmp.Add(s, tagvalue.Text);
            }
            mclient.UpdateData(dtmp);
        }

        private void updateareb_Click(object sender, RoutedEventArgs e)
        {
            string[] sname = tagname.Text.Split(',');
            Dictionary<string, object> dtmp = new Dictionary<string, object>();
            foreach (string s in sname)
            {
                dtmp.Add(s, tagvalue.Text);
            }
            mclient.UpdateAreaData(dtmp);
        }

        private void updatehisb_Click(object sender, RoutedEventArgs e)
        {
            Dictionary<string, Dictionary<DateTime, object>> vals = new Dictionary<string, Dictionary<DateTime, object>>();
            string[] sname = tagname.Text.Split(',');
            DateTime dt = DateTime.UtcNow.AddDays(-1);
            foreach (string s in sname)
            {
                Dictionary<DateTime, object> dtmp = new Dictionary<DateTime, object>();
                for(int i = 0; i < 60;i++)
                {
                    dtmp.Add(dt.AddSeconds(i), i);
                }
                vals.Add(s, dtmp);
            }
            mclient.UpdateHisData(vals);
        }
    }
}
