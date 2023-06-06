using Cdy.Tag.Consume;
using DBHighApi.Api;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DBHighApiEmbedded
{
    /// <summary>
    /// 
    /// </summary>
    public class HighApi : IEmbedProxy
    {
        private static bool mIsClosed = false;


        private static int ReadServerPort()
        {
            try
            {
                string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(HighApi).Assembly.Location), "Config", "DBHighApi.cfg");
                if (System.IO.File.Exists(spath))
                {
                    XElement xx = XElement.Load(spath);
                    if (xx.Element("Allow") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadAllowFromXML(xx.Element("Allow"));

                    if (xx.Element("Forbidden") != null)
                        Cdy.Tag.Common.ClientAuthorization.Instance.LoadForbiddenFromXML(xx.Element("Forbidden"));
                    return int.Parse(xx.Attribute("ServerPort")?.Value);
                }

            }
            catch
            {

            }
            return 14332;
        }

        private int mPort= 14332;

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Init()
        {
            mPort= ReadServerPort();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            DataService.Service.Start(mPort);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            DataService.Service.Stop();
        }
    }
}
