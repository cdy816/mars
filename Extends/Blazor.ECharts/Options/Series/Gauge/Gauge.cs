using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Blazor.ECharts.Options.Series.Gauge
{
    /// <summary>
    /// 仪表盘
    /// </summary>
    public record Gauge : SeriesBase
    {
        public Gauge() : base("gauge") { }
        public Detail Detail { get; set; }
        public object radius { get; set; } = "80%";
        public double startAngle { get; set; } = 200;
        public double endAngle { get; set; } = -20;
        public double min { get; set; } = 0;
        public double max { get; set; } = 100;
        public string[] center { get; set; } = new string[2] { "50%", "50%" };
        public int splitNumber { get; set; } = 5;

        public SplitLine splitLine { get; set; }

        public axisTick axisTick { get; set; }

        public AxisLine axisLine { get; set; }

        public axisLabel axisLabel { get; set; }

        public Progress progress { get; set; }

    }
}
