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

        private void DataGrid_ScrollChanged(object sender, ScrollChangedEventArgs e)
        {
            if((e.ExtentHeight - e.VerticalOffset)<100 && e.ExtentHeight>0 && e.VerticalOffset>0)
            {
                mModel.ContinueLoadData();
            }
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
            mModel.SelectedCells = (sender as DataGrid).SelectedCells;
          
        }

        private void Type_SelectionChanged(object sender, SelectionChangedEventArgs e)
        {
            if((sender as FrameworkElement).IsLoaded)
            {
                dg.CommitEdit();
            }
        }
    }

    



}
