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

namespace DevelopWebApiDemo
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        DBDevelopClientWebApi.DevelopServiceHelper mHelper;
        public MainWindow()
        {
            InitializeComponent();
            mHelper = new DBDevelopClientWebApi.DevelopServiceHelper();
        }

        private void loginb_Click(object sender, RoutedEventArgs e)
        {
            mHelper.Server = serverIp.Text;
            mHelper.Login("Admin", "Admin");
        }
    }
}
