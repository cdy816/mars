using Cdy.Tag;
using DBRuntimeMonitor.ViewModel;
using System;
using System.Collections.Generic;
using System.Globalization;
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
    /// HisTagViewerView.xaml 的交互逻辑
    /// </summary>
    public partial class HisTagViewerView : UserControl
    {
        private ScottPlot.Plottable.ScatterPlot? mchart;

        private List<ScottPlot.Plottable.ScatterPlot> mCharts = new List<ScottPlot.Plottable.ScatterPlot>();

        ScottPlot.Plottable.Crosshair? Crosshair;
        public HisTagViewerView()
        {
            InitializeComponent();
            this.Loaded += HisTagViewerView_Loaded;
            Init();
        }

        private void HisTagViewerView_Loaded(object sender, RoutedEventArgs e)
        {
            (this.DataContext as HisTagViewer).Update += HisTagViewerView_Update;
           (this.DataContext as HisTagViewer)?.ExecuteQuery();
        }

        private void HisTagViewerView_Update(object? sender, EventArgs e)
        {
            UpdateChart();
        }

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            mchart=  this.tb.Plot.AddScatterLines(new double[] { 0}, new double[] { DateTime.Now.ToOADate()}, System.Drawing.Color.Blue, 1, ScottPlot.LineStyle.Solid, "");
            tb.Plot.XAxis.DateTimeFormat(true);
            tb.Plot.XAxis.Color(System.Drawing.Color.White);
            tb.Plot.YAxis.Color(System.Drawing.Color.White);
            //Crosshair = tb.Plot.AddCrosshair(0, 0);
            tb.Plot.Style(figureBackground: System.Drawing.Color.Transparent);
            tb.Plot.Style(dataBackground: System.Drawing.Color.Transparent);
            this.tb.Refresh();
            //tb.Plot.XAxis.ManualTickSpacing(1, ScottPlot.Ticks.DateTimeUnit.Minute);
        }

        private bool IsNumberValue(TagType typ)
        {
            switch(typ)
            {
                case TagType.Bool:
                case TagType.Byte:
                case TagType.Short:
                case TagType.UShort:
                case TagType.Int:
                case TagType.UInt:
                case TagType.Long:
                case TagType.ULong:
                case TagType.Double:
                case TagType.Float:
                    return true;
                default:
                    return false;
            }
        }

        private void UpdateChart()
        {
            this.Dispatcher.Invoke(() => {
                var model = (this.DataContext as HisTagViewer);
                if (!IsNumberValue(model.TagType)) return;

                Dictionary<Dictionary<double, double>,bool> serises = new Dictionary<Dictionary<double, double>, bool>();
                Dictionary<double, double> data = new Dictionary<double, double>();
                data.Add(model.StartTime.ToOADate(), 0);
                ValuePoint vtag = new ValuePoint();

                int i = 0;
                for(;i<model.AllValues.Count;i++)
                {
                    vtag = model.AllValues[i];
                    var vtim = vtag.Time.ToOADate();
                    if (vtag.Quality == (byte)QualityConst.Null || vtag.Quality == (byte)QualityConst.Close)
                    {
                      
                        if (!data.ContainsKey(vtim))
                            data.Add(vtim, vtag.Value != null ? Convert.ToDouble(vtag.Value) : 0);
                    }
                    else if ((vtag.Quality == (byte)QualityConst.Init) || (vtag.Quality == 100 + (byte)QualityConst.Init))
                    {
                        if (!data.ContainsKey(vtim))
                            data.Add(vtim, vtag.Value != null ? Convert.ToDouble(vtag.Value) : 0);
                        break;
                    }
                    else
                    {
                        if (!data.ContainsKey(vtim))
                            data.Add(vtim, vtag.Value != null ? Convert.ToDouble(vtag.Value) : 0);
                        break;
                    }
                }
                serises.Add(data, false);

                bool isgood = true;
                data = new Dictionary<double, double>();
                for (; i < model.AllValues.Count; i++)
                {
                    vtag = model.AllValues[i];
                    if (vtag.Quality == (byte)QualityConst.Null || vtag.Quality == (byte)QualityConst.Close)
                    {
                        if (isgood)
                        {
                            if (!data.ContainsKey(vtag.Time.ToOADate()))
                                data.Add(vtag.Time.ToOADate(), vtag.Value != null ? Convert.ToDouble(vtag.Value) : 0);

                            serises.Add(data, isgood);
                            data = new Dictionary<double, double>();
                            isgood = false;
                        }
                    }
                    else if((vtag.Quality == (byte)QualityConst.Init)||(vtag.Quality == 100+(byte)QualityConst.Init))
                    {
                        if(isgood)
                        {
                            if (data.Count > 0)
                            {
                                serises.Add(data, isgood);
                                var vval = data.Last();
                                data = new Dictionary<double, double>();
                                data.Add(vval.Key, vval.Value);
                                data.Add(vtag.Time.ToOADate(), Convert.ToDouble(vtag.Value));
                                serises.Add(data, false);
                                data = new Dictionary<double, double>();
                            }
                            isgood = true;
                        }
                    }
                    else
                    {
                        if (!isgood)
                        {
                            if (!data.ContainsKey(vtag.Time.ToOADate()))
                                data.Add(vtag.Time.ToOADate(), vtag.Value != null ? Convert.ToDouble(vtag.Value) : 0);

                            serises.Add(data, isgood);
                            data = new Dictionary<double, double>();
                            isgood = true;
                        }
                    }
                    var tim = vtag.Time.ToOADate();
                    if (!data.ContainsKey(tim))
                        data.Add(tim, vtag.Value != null ? Convert.ToDouble(vtag.Value) : 0);
                }
                serises.Add(data, isgood);
                if (vtag.Quality != (byte)QualityConst.Null &&  vtag.Quality != (byte)QualityConst.Close)
                {
                    data = new Dictionary<double, double>();
                    data.Add(vtag.Time.ToOADate(), vtag.Value != null ? Convert.ToDouble(vtag.Value) : 0);
                    data.Add(model.EndTime.ToOADate(), 0);
                    serises.Add(data, false);
                }
                else if ((vtag.Quality == (byte)QualityConst.Init) || (vtag.Quality == 100 + (byte)QualityConst.Init))
                {
                    if (isgood)
                    {
                        serises.Add(data, isgood);
                        var vval = data.Last();
                        data = new Dictionary<double, double>();
                        data.Add(vval.Key, vval.Value);
                        data.Add(vtag.Time.ToOADate(), Convert.ToDouble(vtag.Value));
                        serises.Add(data, false);
                    }
                }
                else
                {
                    var vd = model.EndTime.ToOADate();
                    if(!data.ContainsKey(vd))
                    {
                        data.Add(vd, 0);
                    }
                    
                }



                this.tb.Plot.Clear();
                bool isvailed = false;
                bool isinvailed = false;
                mCharts.Clear();
                foreach (var vv in serises)
                {
                    if (vv.Value)
                    {
                        mchart = this.tb.Plot.AddScatter(vv.Key.Keys.ToArray(), vv.Key.Values.ToArray(), vv.Value ? System.Drawing.Color.Yellow : System.Drawing.Color.Blue, 1, 2, ScottPlot.MarkerShape.filledCircle, vv.Value ? ScottPlot.LineStyle.Solid : ScottPlot.LineStyle.Dash, !isvailed ? "有效值" : "");
                        mCharts.Add(mchart);
                        isvailed = true;
                    }
                    else
                    {
                        mchart = this.tb.Plot.AddScatterLines(vv.Key.Keys.ToArray(), vv.Key.Values.ToArray(), vv.Value ? System.Drawing.Color.Yellow : System.Drawing.Color.Blue, 1, vv.Value ? ScottPlot.LineStyle.Solid : ScottPlot.LineStyle.Dash, !isinvailed ? "无效值" : "");
                        isinvailed = true;
                    }
                }
                tb.Plot.XAxis.DateTimeFormat(true);
                tb.Plot.XAxis.Color(System.Drawing.Color.White);
                tb.Plot.YAxis.Color(System.Drawing.Color.White);
                tb.Plot.Grid(true,System.Drawing.Color.DarkGray,ScottPlot.LineStyle.Dot,false);

                tb.Plot.SetAxisLimitsX(model.StartTime.ToOADate(), model.EndTime.ToOADate());
                tb.Plot.SetOuterViewLimits(model.StartTime.ToOADate(), model.EndTime.ToOADate());
                Crosshair = tb.Plot.AddCrosshair(0, 0);
                //Crosshair.HorizontalLine.PositionLabelOppositeAxis = true;
                //Crosshair.VerticalLine.PositionLabelOppositeAxis = true;
                Crosshair.VerticalLine.PositionLabel = false;
                Crosshair.IsDateTimeX = true;
                Crosshair.StringFormatX = "yyyy-MM-dd HH:mm:ss";
                tb.Plot.Style(figureBackground: System.Drawing.Color.Transparent);
                tb.Plot.Style(dataBackground: System.Drawing.Color.Transparent);

                var lg = tb.Plot.Legend(true, ScottPlot.Alignment.UpperRight);
                lg.FillColor = System.Drawing.Color.Transparent;
                lg.OutlineColor = System.Drawing.Color.Transparent;
                lg.FontColor = System.Drawing.Color.WhiteSmoke;

                

                tb.Refresh();
            });
           
        }

        private void tb_MouseEnter(object sender, MouseEventArgs e)
        {
            if(Crosshair!=null)
            Crosshair.IsVisible = true;
            tb.Refresh();
        }

        private void tb_MouseLeave(object sender, MouseEventArgs e)
        {
            if (Crosshair != null)
                Crosshair.IsVisible = false;
            tb.Refresh();
        }

        private void tb_MouseMove(object sender, MouseEventArgs e)
        {
            int pixelX = (int)e.MouseDevice.GetPosition(tb).X;
            int pixelY = (int)e.MouseDevice.GetPosition(tb).Y;

            (double coordinateX, double coordinateY) = tb.GetMouseCoordinates();

            //tb.Content = $"{pixelX:0.000}";
            //tb.Content = $"{pixelY:0.000}";

            //XCoordinateLabel.Content = $"{tb.Plot.GetCoordinateX(pixelX):0.00000000}";
            //YCoordinateLabel.Content = $"{tb.Plot.GetCoordinateY(pixelY):0.00000000}";
            ttip.Text = "";
            var pval = tb.Plot.GetCoordinateY(pixelY);
            foreach (var vv in mCharts)
            {
                var vmin = vv.GetAxisLimits().XMin;
                var vmax = vv.GetAxisLimits().XMax;
                var pmin = tb.Plot.GetPixelX(vmin);
                var pmax = tb.Plot.GetPixelX(vmax);
                if (pixelX >= pmin && pixelX <= pmax)
                {
                    var vcc = vv.GetPointNearestX(coordinateX);
                    ttip.Text = DateTime.FromOADate(vcc.x) + Environment.NewLine + vcc.y.ToString("f4");
                }
            }

            var loc = tb.TranslatePoint(new Point(pixelX, pixelY), tp.Parent as UIElement);

            //if (pval-tb.Plot.YAxis.Dims.Min > tb.Plot.YAxis.Dims.Span/2)
            //{
            tp.Margin = new Thickness(loc.X, 0, 0, 0);
            if (!string.IsNullOrEmpty(ttip.Text))
            {
                tp.Visibility = Visibility.Visible;
            }
            else
            {
                tp.Visibility = Visibility.Hidden;
            }
            //}
            //else
            //{
            //    tp.Margin = new Thickness(loc.X, tb.ActualHeight - tp.ActualHeight-64, 0, 0);
            //}


            if (Crosshair != null)
            {
                Crosshair.X = coordinateX;
                Crosshair.Y = coordinateY;

            }

            tb.Refresh();
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class DateTimeDisplayConverter : IValueConverter
    {
        public object Convert(object value, Type targetType, object parameter, CultureInfo culture)
        {
            return ((DateTime)value).ToString("yyyy-MM-dd HH:mm:ss");
        }

        public object ConvertBack(object value, Type targetType, object parameter, CultureInfo culture)
        {
            throw new NotImplementedException();
        }
    }


}
