using DBRuntimeMonitor.ViewModel;
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

namespace DBRuntimeMonitor.View
{
    /// <summary>
    /// LogDetailView.xaml 的交互逻辑
    /// </summary>
    public partial class LogDetailView : UserControl
    {
        public LogDetailView()
        {
            InitializeComponent();
            this.Loaded += LogDetailView_Loaded;
        }

        private void LogDetailView_Loaded(object sender, RoutedEventArgs e)
        {
            this.Loaded -= LogDetailView_Loaded;
            (this.DataContext as LogDetailViewModel).TextBox = this.logshow;
        }
    }
}
