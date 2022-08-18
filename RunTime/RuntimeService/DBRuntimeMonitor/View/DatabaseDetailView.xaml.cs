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
    /// DatabaseDetailView.xaml 的交互逻辑
    /// </summary>
    public partial class DatabaseDetailView : UserControl
    {

        private ScottPlot.Plottable.PiePlot mCpu;
        private ScottPlot.Plottable.PiePlot mMemory;
        private ScottPlot.Plottable.RadialGaugePlot mNetwork;

        DatabaseDetailViewModel mViewModel;

        /// <summary>
        /// 
        /// </summary>
        public DatabaseDetailView()
        {
            InitializeComponent();
        }

        
    }
}
