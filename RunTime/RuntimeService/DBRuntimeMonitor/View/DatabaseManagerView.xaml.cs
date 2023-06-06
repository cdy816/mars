﻿using DBRuntimeMonitor.ViewModel;
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
    /// DatabaseManagerView.xaml 的交互逻辑
    /// </summary>
    public partial class DatabaseManagerView : UserControl
    {
        public DatabaseManagerView()
        {
            InitializeComponent();
            this.Loaded += DatabaseManagerView_Loaded;
        }

        private void DatabaseManagerView_Loaded(object sender, RoutedEventArgs e)
        {
            pb.Password = (this.DataContext as DatabaseManagerViewModel).Password;
            (this.DataContext as DatabaseManagerViewModel).Load();
        }

        private void PasswordBox_PasswordChanged(object sender, RoutedEventArgs e)
        {
            (this.DataContext as DatabaseManagerViewModel).Password = pb.Password;
        }
    }
}