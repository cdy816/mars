using DBInStudio.Desktop.ViewModel;
using System;
using System.Collections.Generic;
using System.Text;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace DBInStudio.Desktop.View
{
    /// <summary>
    /// TagGroupDetailView.xaml 的交互逻辑
    /// </summary>
    public partial class TagGroupDetailView : UserControl
    {
        public TagGroupDetailView()
        {
            InitializeComponent();
        }

        private void DataGrid_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            if((e.ExtentHeight - e.VerticalOffset)<50)
            {
                (this.DataContext as TagGroupDetailViewModel).ContinueLoadData();
            }
        }
    }

    
}
