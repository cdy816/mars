using Cdy.Tag;
using System;
using System.Collections.Generic;
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

namespace HisDataTools.View
{
    /// <summary>
    /// HisDataQueryView.xaml 的交互逻辑
    /// </summary>
    public partial class HisDataQueryView : UserControl
    {
        public HisDataQueryView()
        {
            InitializeComponent();
            
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
            if(value is double || value is float)
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
            int ival = (int)value;
            if(ival<20)
            {
                return Res.Get("good");
            }
            else if (ival == (byte)QualityConst.Init)
            {
                return Res.Get("Init");
            }
            else if(ival>20&&ival<100)
            {
                return Res.Get("bad");
            }
            else if(ival>100&&ival<200)
            {
                return Res.Get("startvalue");
            }
            else if(ival==255)
            {
                return Res.Get("null");
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

}
