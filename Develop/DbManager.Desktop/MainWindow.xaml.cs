﻿using Grpc.Net.Client;
using System;
using System.Collections.Generic;
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
using System.Windows.Media.Imaging;
using System.Windows.Navigation;
using System.Windows.Shapes;

namespace DBInStudio.Desktop
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        public MainWindow()
        {
            InitializeComponent();
            this.DataContext = new MainViewModel();
        }

        private void closeB_Click(object sender, RoutedEventArgs e)
        {
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
                this.DragMove();
            }
        }

        private void Grid_MouseMove(object sender, MouseEventArgs e)
        {
            if(e.LeftButton == MouseButtonState.Pressed)
            {
                if(WindowState == WindowState.Maximized)
                {
                    WindowState = WindowState.Normal;
                }
            }
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


    }
}
