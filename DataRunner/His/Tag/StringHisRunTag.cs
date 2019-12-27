using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class StirngHisRunTag:HisRunTag
    {
        /// <summary>
        /// 
        /// </summary>
        public override byte SizeOfValue => (byte)(RealEnginer.StringSize);

        

    }
}
