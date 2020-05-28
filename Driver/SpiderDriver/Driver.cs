using Cdy.Tag.Driver;
using System;

namespace SpiderDriver
{
    /// <summary>
    /// 
    /// </summary>
    public class Driver : Cdy.Tag.Driver.IProducterDriver
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name => "Spider";

        /// <summary>
        /// 
        /// </summary>
        public string[] Registors => new string[0];

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <returns></returns>
        public bool Start(IRealTagProduct tagQuery)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            throw new NotImplementedException();
        }
    }
}
