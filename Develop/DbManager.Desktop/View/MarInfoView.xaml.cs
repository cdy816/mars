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
    /// MarInfoView.xaml 的交互逻辑
    /// </summary>
    public partial class MarInfoView : UserControl
    {
        public MarInfoView()
        {
            InitializeComponent();
        }


        private void TextBlock_MouseEnter(object sender, MouseEventArgs e)
        {
            (sender as FrameworkElement).Opacity = 0.9;
        }

        private void TextBlock_MouseLeave(object sender, MouseEventArgs e)
        {
            (sender as FrameworkElement).Opacity = 0.5;
        }
    }
}
