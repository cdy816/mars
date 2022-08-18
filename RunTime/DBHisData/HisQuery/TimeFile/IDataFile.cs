using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    public interface IDataFile
    {

        public DateTime LastTime { get; }
        /// <summary>
        /// 
        /// </summary>
        public string FId { get; set; }

        public string FileName { get; set; }


        public string BackFileName { get; set; }

        public DateTime StartTime { get; set; }

        public DateTime EndTime { get; set; }


        public TimeSpan Duration { get; set; }

        void UpdateLastDatetime();
    }
}
