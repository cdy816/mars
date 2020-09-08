using Cdy.Tag.Driver;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Xml.Linq;

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

        private int mPort = 3600;
        private int mEndPort = 3600;

        private List<DataService> mService;

        public void Load()
        {

            string sfileName = Assembly.GetEntryAssembly().Location;
            string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sfileName), "Config");
            sfileName = System.IO.Path.Combine(spath,  "SpiderDriver.cfg");

            if (System.IO.File.Exists(sfileName))
            {
                XElement xe = XElement.Load(sfileName);
                if (xe.Element("Server") == null)
                    return;
                xe = xe.Element("Server");
               
                if (xe.Attribute("StartPort") != null)
                {
                    mPort = int.Parse(xe.Attribute("StartPort").Value);
                }

                if (xe.Attribute("EndPort") != null)
                {
                    mEndPort = int.Parse(xe.Attribute("EndPort").Value);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <returns></returns>
        public bool Start(IRealTagProduct tagQuery)
        {
            Load();
            mService = new List<DataService>();

            RealDataServerProcess.AllowTagIds = new HashSet<int>(tagQuery.GetTagIdsByLinkAddress(this.Name+":"));
            
            for (int i = mPort; i <= mEndPort; i++)
            {
                try
                {
                    var mSvc = new DataService();
                    mSvc.Start(i);
                    mService.Add(mSvc);
                }
                catch
                {

                }
                
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Stop()
        {
            foreach(var vvs in mService)
            vvs.Stop();
            return true;
        }
    }
}
