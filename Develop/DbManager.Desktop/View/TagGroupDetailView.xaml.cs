using DBInStudio.Desktop.ViewModel;
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

namespace DBInStudio.Desktop.View
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

            if (dscoll > 0 && (dscoll - e.VerticalOffset) / dscoll < 0.25)
            {
                if (mModel.CanContinueLoadData())
                    mModel.ContinueLoadData();
            }

            //if ((e.ExtentHeight - e.VerticalOffset)<100 && e.ExtentHeight>0 && e.VerticalOffset>0)
            //{
            //    mModel.ContinueLoadData();
            //}
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
            if (((sender as DataGrid).SelectionUnit == DataGridSelectionUnit.CellOrRowHeader) || ((sender as DataGrid).SelectionUnit == DataGridSelectionUnit.Cell))
            {
                (sender as DataGrid).EndInit();
                (sender as DataGrid).BeginEdit();
            }
            mModel.SelectedCells = (sender as DataGrid).SelectedCells;
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

    



}
