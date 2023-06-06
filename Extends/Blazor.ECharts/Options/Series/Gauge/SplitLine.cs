using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Blazor.ECharts.Options.Series.Gauge
{
    public record SplitLine
    {
        public bool? show { get; set; } = true;
        public double length { get; set; } = 10;
        public double distance { get; set; } = 10;
        public LineStyle lineStyle { get; set; }
    }

    public record axisTick
    {
        public bool? show { get; set; } = true;
        public int splitNumber { get; set; } = 5;
        public int length { get; set; } = 6;
        public int distance { get; set; } = 10;
        public LineStyle lineStyle { get; set; }

    }

    public record axisLabel
    {
        public bool? show { get; set; } = true;
        public int distance { get; set; } = 10;
        public int rotate { get; set; } = 0;
        public string Color { get; set; } = "#464646";

    }

    public record Progress
    {
        public bool? show { get; set; } = true;
        public int width {get;set; } = 10;
        public bool roundCap { get; set; } = false;
        public bool clip { get; set;} = true;
        public ItemStyle itemStyle { get; set; }
    }
}
