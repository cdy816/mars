#region <版 本 注 释>
/*
 * ========================================================================
 * Copyright(c) 四川*******公司, All Rights Reserved.
 * ========================================================================
 *    
 * 作者：[HeBianGu]   时间：2018/1/18 11:28:41 
 * 文件名：ChartPlotter 
 * 说明：
 * 
 * 
 * 修改者：           时间：               
 * 修改说明：
 * ========================================================================
*/
#endregion
using HeBianGu.WPF.EChart;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Media;

namespace HeBianGu.WPF.EChart
{

    /// <summary> 图表基类 </summary>
    public abstract class ChartPlotter : ViewPlotter
    {
        #region - 基础方法 -

        /// <summary> 获取值对应Canvas的位置 </summary>
        public double GetY(double value)
        {
            if ((this.MaxValueY - this.MinValueY) == 0) return 0;
            var bottom = this.ParallelCanvas.ActualHeight - ((value - this.MinValueY) / (this.MaxValueY - this.MinValueY)) * this.ParallelCanvas.ActualHeight;

            return bottom;
        }

        /// <summary> 获取值对应Canvas的位置 </summary>
        public double GetX(double value)
        {
            if ((this.MaxValueX - this.MinValueX) == 0) return 0;
            var bottom = ((value - this.MinValueX) / (this.MaxValueX - this.MinValueX)) * this.ParallelCanvas.ActualWidth;


            return bottom;
        }

        /// <summary> 获取值对应Canvas的位置应有的 Y 值 </summary>
        public double GetYValue(double value)
        {
            //var bottom = this.ParallelCanvas.ActualHeight - ((value - this.MinValueY) / (this.MaxValueY - this.MinValueY)) * this.ParallelCanvas.ActualHeight;

            var bottom = (((this.ParallelCanvas.ActualHeight - value) / this.ParallelCanvas.ActualHeight) * (this.MaxValueY - this.MinValueY)) + this.MinValueY;
            return bottom;
        }

        #endregion

        #region - 控制参数 -


        public double MinValueY
        {
            get { return (double)GetValue(MinValueYProperty); }
            set { SetValue(MinValueYProperty, value); }
        }

        // Using a DependencyProperty as the backing store for MinValueX.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty MinValueYProperty =
            DependencyProperty.Register("MinValueY", typeof(double), typeof(ChartPlotter), new PropertyMetadata(0.0));



        public double MaxValueY
        {
            get { return (double)GetValue(MaxValueYProperty); }
            set { SetValue(MaxValueYProperty, value); }
        }

        // Using a DependencyProperty as the backing store for MaxValueX.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty MaxValueYProperty =
            DependencyProperty.Register("MaxValueY", typeof(double), typeof(ChartPlotter), new PropertyMetadata(0.0));

        //private double _maxValueX;
        ///// <summary> 要显示的最大值 </summary>
        //public double MaxValueX
        //{
        //    get { return _maxValueX; }
        //    set { _maxValueX = value; }
        //}

        //private double _minValueX;
        ///// <summary> 要显示的最小值 </summary>
        //public double MinValueX
        //{
        //    get { return _minValueX; }
        //    set { _minValueX = value; }
        //}

        public double MinValueX
        {
            get { return (double)GetValue(MinValueXProperty); }
            set { SetValue(MinValueXProperty, value); }
        }

        // Using a DependencyProperty as the backing store for MinValueX.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty MinValueXProperty =
            DependencyProperty.Register("MinValueX", typeof(double), typeof(ChartPlotter), new PropertyMetadata(0.0));



        public double MaxValueX
        {
            get { return (double)GetValue(MaxValueXProperty); }
            set { SetValue(MaxValueXProperty, value); }
        }

        // Using a DependencyProperty as the backing store for MaxValueX.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty MaxValueXProperty =
            DependencyProperty.Register("MaxValueX", typeof(double), typeof(ChartPlotter), new PropertyMetadata(0.0));


        //private List<SplitItem> _splitItemYs = new List<SplitItem>();
        ///// <summary> Y范围分割线 </summary>
        //public List<SplitItem> SplitItemYs
        //{
        //    get { return _splitItemYs; }
        //    set { _splitItemYs = value; }
        //}



        public List<SplitItem> SplitItemYs
        {
            get { return (List<SplitItem>)GetValue(SplitItemYsProperty); }
            set { SetValue(SplitItemYsProperty, value); }
        }

        // Using a DependencyProperty as the backing store for SplitItemYs.  This enables animation, styling, binding, etc...
        public static readonly DependencyProperty SplitItemYsProperty =  DependencyProperty.Register("SplitItemYs", typeof(List<SplitItem>), typeof(ChartPlotter), new PropertyMetadata(null));



        private List<SplitItem> _slpitItemXs = new List<SplitItem>();
        /// <summary> 说明 </summary>
        public List<SplitItem> SlpitItemXs
        {
            get { return _slpitItemXs; }
            set { _slpitItemXs = value; }
        }



        //public List<SplitItem> SlpitItemXs
        //{
        //    get { return (List<SplitItem>)GetValue(SlpitItemXsProperty); }
        //    set { SetValue(SlpitItemXsProperty, value); }
        //}

        //// Using a DependencyProperty as the backing store for SlpitItemXs.  This enables animation, styling, binding, etc...
        //public static readonly DependencyProperty SlpitItemXsProperty =
        //    DependencyProperty.Register("SlpitItemXs", typeof(List<SplitItem>), typeof(ChartPlotter), new PropertyMetadata(new List<SplitItem>()));



        #endregion





    }


}
