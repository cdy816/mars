using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;
using System.Xml.Linq;

namespace DBInStudio.Desktop
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        private Point mLastClickLoc;
        public MainWindow()
        {
            InitializeComponent();
            this.DataContext = new MainViewModel();
            this.StateChanged += MainWindow_StateChanged;
        }

        private void MainWindow_StateChanged(object sender, EventArgs e)
        {
            if(this.WindowState == WindowState.Normal)
            {
                
            }
        }

        private void closeB_Click(object sender, RoutedEventArgs e)
        {
            var mm = (this.DataContext as MainViewModel);
            if (mm.LogoutCommand.CanExecute(null))
                mm.LogoutCommand.Execute(null);
            this.Close();
        }

        private void minB_Click(object sender, RoutedEventArgs e)
        {
            this.WindowState = WindowState.Minimized;
        }

        private void Grid_MouseLeftButtonDown(object sender, MouseButtonEventArgs e)
        {
            //(sender as Grid).CaptureMouse();
            if (e.ClickCount > 1)
            {
                if (WindowState == WindowState.Maximized)
                {
                    WindowState = WindowState.Normal;
                }
                else
                {
                    WindowState = WindowState.Maximized;
                }
            }
            else
            {
                
                if(WindowState == WindowState.Maximized)
                {
                    var rec = this.RestoreBounds;
                    var ll = e.GetPosition(this);
                    var dx = ll.X / this.ActualWidth * rec.Width;
                    var dy = ll.Y / this.ActualHeight * rec.Height;
                    var pp = ll;

                    this.Left = pp.X - dx-8;
                    this.Top = pp.Y - dy-24;

                    this.WindowState = WindowState.Normal;

                }

                this.DragMove();
            }
        }

        private void Grid_MouseMove(object sender, MouseEventArgs e)
        {
            //if(e.LeftButton == MouseButtonState.Pressed)
            //{
            //    if(WindowState == WindowState.Maximized)
            //    {
            //        WindowState = WindowState.Normal;
            //    }
            //}
        }

        private void Grid_MouseLeftButtonUp(object sender, MouseButtonEventArgs e)
        {
           
        }

        private void maxB_Click(object sender, RoutedEventArgs e)
        {
            if (WindowState == WindowState.Maximized)
            {
                WindowState = WindowState.Normal;
            }
            else
            {
                WindowState = WindowState.Maximized;
            }
        }


        private void TreeView_SelectedItemChanged(object sender, RoutedPropertyChangedEventArgs<object> e)
        {
            (this.DataContext as MainViewModel).CurrentSelectGroup = tv.SelectedItem as TreeItemViewModel;
        }

        private void TextBox_KeyDown(object sender, KeyEventArgs e)
        {
            if(e.Key == Key.Enter)
            {
                (sender as TextBox).GetBindingExpression(TextBox.TextProperty).UpdateSource();
            }
        }

        private void TextBox_IsVisibleChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
           if((sender as TextBox).IsVisible)
            {
                (sender as TextBox).SelectAll();
                (sender as TextBox).Focus();
                
            }
        }

        private void TextBox_LostFocus(object sender, RoutedEventArgs e)
        {
            (sender as TextBox).GetBindingExpression(TextBox.TextProperty).UpdateSource();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void TextBox_Loaded(object sender, RoutedEventArgs e)
        {
            (sender as TextBox).SelectAll();
            (sender as TextBox).Focus();
        }

        private void Image_MouseEnter(object sender, MouseEventArgs e)
        {
            (sender as Image).Opacity = 0.8;
        }

        private void Image_MouseLeave(object sender, MouseEventArgs e)
        {
            (sender as Image).Opacity = 0.1;
        }

        private void Grid_IsVisibleChanged(object sender, DependencyPropertyChangedEventArgs e)
        {
            if ((sender as FrameworkElement).IsVisible)
            {
                (this.FindResource("WaitAnimate") as Storyboard).Begin();
            }
            else
            {
                (this.FindResource("WaitAnimate") as Storyboard).Stop();
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class DoubleFormateConvert : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return ((double)value).ToString("f1") + " %";
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

}
