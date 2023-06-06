using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag.Consume
{
    public interface IEmbedProxy
    {

        void Init();
        /// <summary>
        /// 
        /// </summary>
        void Start();
        
        /// <summary>
        /// 
        /// </summary>
        void Stop();
    }
}
