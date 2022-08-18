using DBRuntimeMonitor;
using DBRuntimeMonitor.ViewModel;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
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

namespace DBRuntimeMonitor
{
    /// <summary>
    /// TagGroupDetailView.xaml 的交互逻辑
    /// </summary>
    public partial class TagGroupDetailView : UserControl
    {
        private TagGroupDetailViewModel mModel;
        public TagGroupDetailView()
        {
            InitializeComponent();
            this.Loaded += TagGroupDetailView_Loaded;
        }

        private void TagGroupDetailView_Loaded(object sender, RoutedEventArgs e)
        {
            this.Loaded -= TagGroupDetailView_Loaded;
            mModel = this.DataContext as TagGroupDetailViewModel;
            mModel.grid = this.dg;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="target"></param>
        /// <param name="ll"></param>
        private void GetVisualParent(DependencyObject target, List<string> ll)
        {
            var parent = LogicalTreeHelper.GetParent(target);
            if (parent != null)
            {
                ll.Add(parent.ToString());
                GetVisualParent(parent, ll);
            }
        }

        private void DataGrid_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            List<string> ll = new List<string>();
            GetVisualParent(e.OriginalSource as DependencyObject, ll);
            if (ll.Contains("System.Windows.Controls.Primitives.Popup")) return;

            var dscoll = (e.ExtentHeight - e.ViewportHeight);

            int pcount = (int)(dg.ActualHeight / dg.MinRowHeight);
            var start = e.VerticalOffset-3;
            start = start < 0 ? 0 : start;


            if (dscoll > 0 && (dscoll - e.VerticalOffset) / dscoll < 0.25)
            {
                if (mModel.CanContinueLoadData())
                    mModel.ContinueLoadData();
            }
            mModel?.UpdateViewPort((int)start, (int)(start+pcount+3));
        }

        private void kwinput_KeyDown(object sender, KeyEventArgs e)
        {
            if(e.Key == Key.Enter)
            {
                (sender as FrameworkElement).MoveFocus(new TraversalRequest(FocusNavigationDirection.Next));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DataGrid_SelectedCellsChanged(object sender, SelectedCellsChangedEventArgs e)
        {

        }

        private void Type_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if((sender as FrameworkElement).IsLoaded)
            {
                if (!(sender as ComboBox).IsEditable)
                    dg.CommitEdit();
            }
        }

        private void TextBox_Loaded(object sender, RoutedEventArgs e)
        {
            if ((sender as TextBox).IsVisible)
            {
                (sender as TextBox).Focus();
            }
        }

        private void ComboBox_Loaded(object sender, RoutedEventArgs e)
        {
            if ((sender as ComboBox).IsVisible)
            {
                (sender as ComboBox).Focus();
            }
        }
    }



    public class QualityValueConvert : IValueConverter
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="targetType"></param>
        /// <param name="parameter"></param>
        /// <param name="culture"></param>
        /// <returns></returns>
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            int ival = System.Convert.ToInt32(value);
            if (ival < 20)
            {
                return Res.Get("Good");
            }
            else if (ival == 63)
            {
                return Res.Get("Init");
            }
            else if (ival > 20 && ival < 100)
            {
                return Res.Get("Bad");
            }
            else if (ival > 100 && ival < 200)
            {
                return ival-100;
            }
            else if (ival == 255)
            {
                return Res.Get("Null");
            }
            else
            {
                return value;
            }
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

    public class ResultValueConvert : IValueConverter
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="targetType"></param>
        /// <param name="parameter"></param>
        /// <param name="culture"></param>
        /// <returns></returns>
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            if (value is double || value is float)
            {
                return (System.Convert.ToDouble(value)).ToString("f4");
            }
            return value;
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }

}
