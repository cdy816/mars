using System;
using System.Collections.Generic;
using System.ComponentModel;
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

namespace HisDataTools
{
    /// <summary>
    /// 按照步骤 1a 或 1b 操作，然后执行步骤 2 以在 XAML 文件中使用此自定义控件。
    ///
    /// 步骤 1a) 在当前项目中存在的 XAML 文件中使用该自定义控件。
    /// 将此 XmlNamespace 特性添加到要使用该特性的标记文件的根
    /// 元素中:
    ///
    ///     xmlns:MyNamespace="clr-namespace:HisDataTools.Common"
    ///
    ///
    /// 步骤 1b) 在其他项目中存在的 XAML 文件中使用该自定义控件。
    /// 将此 XmlNamespace 特性添加到要使用该特性的标记文件的根
    /// 元素中:
    ///
    ///     xmlns:MyNamespace="clr-namespace:HisDataTools.Common;assembly=HisDataTools.Common"
    ///
    /// 您还需要添加一个从 XAML 文件所在的项目到此项目的项目引用，
    /// 并重新生成以避免编译错误:
    ///
    ///     在解决方案资源管理器中右击目标项目，然后依次单击
    ///     “添加引用”->“项目”->[浏览查找并选择此项目]
    ///
    ///
    /// 步骤 2)
    /// 继续操作并在 XAML 文件中使用控件。
    ///
    ///     <MyNamespace:AutoFilterCombox/>
    ///
    /// </summary>
    public class AutoFilterCombox : ComboBox
    {
        private TextBox mTextBox;
        private bool mIsBusy = false;
        private System.Collections.ObjectModel.ObservableCollection<string> mInnerItems = new System.Collections.ObjectModel.ObservableCollection<string>();
        //private ICollectionView view;


        static AutoFilterCombox()
        {
            DefaultStyleKeyProperty.OverrideMetadata(typeof(AutoFilterCombox), new FrameworkPropertyMetadata(typeof(AutoFilterCombox)));
        }

        public AutoFilterCombox()
        {
            this.IsTextSearchEnabled = false;
            this.ItemsSource = mInnerItems;
        }

        /// <summary>
        /// 
        /// </summary>
        public IEnumerable<string> OrignalItemSource
        {
            get { return (IEnumerable<string>)GetValue(OrignalItemSourceProperty); }
            set { SetValue(OrignalItemSourceProperty, value); }
        }

        /// <summary>
        /// 
        /// </summary>
        public static readonly DependencyProperty OrignalItemSourceProperty = DependencyProperty.Register("OrignalItemSource", typeof(IEnumerable<string>), typeof(AutoFilterCombox), new FrameworkPropertyMetadata(null, OnOrignalItemSourcePropertyChanged));


        private static void OnOrignalItemSourcePropertyChanged(DependencyObject sender,DependencyPropertyChangedEventArgs arg)
        {
            //(sender as AutoFilterCombox).CreatFilter();
            (sender as AutoFilterCombox).FilterData();
        }

        /// <summary>
        /// 
        /// </summary>
        public override void OnApplyTemplate()
        {
            base.OnApplyTemplate();
            mTextBox = this.GetTemplateChild("PART_EditableTextBox") as TextBox;
            mTextBox.GotKeyboardFocus += MTextBox_GotKeyboardFocus;
            mTextBox.TextChanged += MTextBox_TextChanged;
            mTextBox.PreviewKeyDown += MTextBox_PreviewKeyDown;
            mTextBox.PreviewMouseDown += MTextBox_PreviewMouseDown;
            mTextBox.AutoWordSelection = false;
            
        }

        private void MTextBox_PreviewMouseDown(object sender, MouseButtonEventArgs e)
        {
            if(!IsDropDownOpen)
            IsDropDownOpen = true;
        }

        private void MTextBox_PreviewKeyDown(object sender, KeyEventArgs e)
        {
            IsDropDownOpen = true;
        }

        private void MTextBox_GotKeyboardFocus(object sender, KeyboardFocusChangedEventArgs e)
        {
            IsDropDownOpen = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MTextBox_TextChanged(object sender, TextChangedEventArgs e)
        {
            //Task.Run(() => {
            //    mCount = 0;
            //    this.view.Refresh();
            //});
            FilterData();
        }

        //private void CreatFilter()
        //{
        //    mCount = 0;
        //    view = CollectionViewSource.GetDefaultView(OrignalItemSource);
        //    view.Filter = FilterCallback;
        //    this.ItemsSource = view;
        //}

        private void FilterData()
        {
            if (mIsBusy) return;
            mIsBusy = true;
            var vtext = mTextBox.Text;
            mInnerItems.Clear();
            var vtmp = OrignalItemSource;
            Task.Run(() => {
              
                foreach (var vv in vtmp.Where(e => e.StartsWith(vtext)).Take(50))
                {
                    this.Dispatcher.Invoke(() => {
                        mInnerItems.Add(vv);
                    });
                   
                }
                mIsBusy = false;
                this.Dispatcher.Invoke(() => {
                    if (vtext != mTextBox.Text)
                    {
                        FilterData();
                    }
                });
                
            });
            


            //Task.Run(() => {
            //    var val = OrignalItemSource.Where(e => e.StartsWith(vtext)).Take(50).ToList();
            //    mIsBusy = false;
            //    this.Dispatcher.BeginInvoke(new Action(() => {
            //        if (vtext != mTextBox.Text)
            //        {
            //            FilterData();
            //        }
            //    }));
            //});
        }

        //private int mCount = 0;
        //private bool FilterCallback(object item)
        //{
        //    if(item.ToString().StartsWith(mTextBox.Text)&& mCount<50)
        //    {
        //        mCount++;
        //        return true;
        //    }
        //    return false;
        //}

    }
}
