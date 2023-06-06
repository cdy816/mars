using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Blazor.ECharts.Options.Series
{
    public record Detail
    {
        public object Formatter { set; get; }
        public string color { get; set; } = "white";
        public object[] offsetCenter { get; set; }=new object[2] { 0,"-20%"};
        public int fontSize { get; set; } = 12;
    }
}
