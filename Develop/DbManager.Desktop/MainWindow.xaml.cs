using DBInStudio.Desktop.ViewModel;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Data;
using System.Windows.Documents;
using System.Windows.Input;
using System.Windows.Interop;
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
        private double mOldHeight;
        private double mOldWidth;

        private WindowState mWindowState;

        //private Point mLastClickLoc;
        public MainWindow()
        {
            InitializeComponent();
            this.DataContext = new MainViewModel();

            mOldHeight = 768;
            mOldWidth = 1024;
            this.StateChanged += MainWindow_StateChanged;
            this.SizeChanged += MainWindow_SizeChanged;
            this.Loaded += MainWindow_Loaded;
            InitBd();

            RegistorEditTypeManager.Instance.Init();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MainWindow_Loaded(object sender, RoutedEventArgs e)
        {
            this.Loaded -= MainWindow_Loaded;
            EnableBlur();
            (this.DataContext as MainViewModel).CheckAutoLogin();
        }

        private void InitBd()
        {
            Border bd = new Border();
            Grid.SetRowSpan(bd, 3);
            bd.BorderThickness = new Thickness(2);
            bd.BorderBrush = Brushes.DarkGray;
            bd.CornerRadius = new CornerRadius(5);
            bg.Children.Add(bd);
        }

        private void MainWindow_SizeChanged(object sender, SizeChangedEventArgs e)
        {
            bd.Clip = new RectangleGeometry() { Rect = new Rect(1, 1, bd.ActualWidth-2, bd.ActualHeight-2), RadiusX = 5, RadiusY = 5 };
        }

        private void MainWindow_StateChanged(object sender, EventArgs e)
        {
            if(this.WindowState == WindowState.Maximized)
            {
                this.WindowState = WindowState.Normal;
                Max();
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
                if (mWindowState == WindowState.Maximized)
                {
                    Normal();
                    
                }
                else
                {
                    Max();
                }
            }
            else
            {
                
                if(mWindowState == WindowState.Maximized)
                {

                    double dsx = PresentationSource.FromVisual(this).CompositionTarget.TransformToDevice.M11;

                    var rec = this.RestoreBounds;
                    var ll =  e.GetPosition(this);
                    var dx = ll.X / this.ActualWidth * mOldWidth;
                    var dy = ll.Y / this.ActualHeight * mOldHeight;
                    var pp = this.PointToScreen(ll);

                    pp = new Point(pp.X / dsx, pp.Y / dsx);

                    this.Left = pp.X - dx-8;
                    this.Top = pp.Y - dy-4;
                    this.Normal(false);

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

        private void Max()
        {
            mOldHeight = this.Height;
            mOldWidth = this.Width;
            this.Top = SystemParameters.WorkArea.Top;
            this.Left = SystemParameters.WorkArea.Left;
            this.Height = SystemParameters.WorkArea.Height;
            this.Width = SystemParameters.WorkArea.Width;
            mWindowState = WindowState.Maximized;
        }

        private void Normal(bool iscenter=true)
        {
            this.Height = mOldHeight;
            this.Width = mOldWidth;
            mWindowState = WindowState.Normal;
            if(iscenter)
            {
                this.Left = (SystemParameters.WorkArea.Width - this.Width) / 2;
                this.Top = (SystemParameters.WorkArea.Height - this.Height) / 2;
            }
        }

        private void maxB_Click(object sender, RoutedEventArgs e)
        {
            if (mWindowState == WindowState.Normal)
            {
                Max();
            }
            else
            {
                Normal();
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

        [DllImport("user32.dll")]
        internal static extern int SetWindowCompositionAttribute(IntPtr hwnd, ref WindowCompositionAttributeData data);

        internal void EnableBlur()
        {
            var windowHelper = new WindowInteropHelper(this);

            var accent = new AccentPolicy();
            accent.AccentState = AccentState.ACCENT_ENABLE_BLURBEHIND;

            var accentStructSize = Marshal.SizeOf(accent);

            var accentPtr = Marshal.AllocHGlobal(accentStructSize);
            Marshal.StructureToPtr(accent, accentPtr, false);

            var data = new WindowCompositionAttributeData();
            data.Attribute = WindowCompositionAttribute.WCA_ACCENT_POLICY;
            data.SizeOfData = accentStructSize;
            data.Data = accentPtr;

            SetWindowCompositionAttribute(windowHelper.Handle, ref data);

            Marshal.FreeHGlobal(accentPtr);
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
