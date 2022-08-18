using System;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using System.Windows;

namespace DbManager.Desktop
{
    /// <summary>
    /// Interaction logic for App.xaml
    /// </summary>
    public partial class App : Application
    {
        public App()
        {
            this.Startup += App_Startup;
        }

        private void App_Startup(object sender, StartupEventArgs e)
        {
            if(e.Args.Length==4)
            {
                DBInStudio.Desktop.AutoLogin.HostIP = e.Args[0];
                DBInStudio.Desktop.AutoLogin.Database = e.Args[1];
                DBInStudio.Desktop.AutoLogin.UserName = e.Args[2];
                DBInStudio.Desktop.AutoLogin.Password = e.Args[3];
            }
        }
    }
}
