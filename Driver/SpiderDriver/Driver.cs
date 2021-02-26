using Cdy.Tag;
using Cdy.Tag.Driver;
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Xml.Linq;
using System.Linq;

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

        public static HashSet<int> AllowTagIds = new HashSet<int>();


        private void Load()
        {

            string sfileName = Assembly.GetEntryAssembly().Location;
            string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sfileName), "Config");
            sfileName = System.IO.Path.Combine(spath,  "SpiderDriver.cfg");

            var dbpath = ServiceLocator.Locator.Resolve("DatabaseLocation");
            if(dbpath!=null)
            {
                var sdp = System.IO.Path.Combine(dbpath.ToString(), "SpiderDriver.cfg");
                if(System.IO.File.Exists(sdp))
                {
                    sfileName = sdp;
                }
            }

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
        public void Save()
        {
            Save(PathHelper.helper.GetApplicationFilePath("Config", "SpiderDriver.cfg"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void SaveForDatabase(string database)
        {
            Save(System.IO.Path.Combine(PathHelper.helper.GetDatabasePath(database), "SpiderDriver.cfg"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<string,string> LoadFromDatabase(string database)
        {
            Dictionary<string, string> re = new Dictionary<string, string>();

            var sfileName = System.IO.Path.Combine(PathHelper.helper.GetDatabasePath(database), "SpiderDriver.cfg");
            if (System.IO.File.Exists(sfileName))
            {
                XElement xe = XElement.Load(sfileName);
                if (xe.Element("Server") != null)
                {
                    xe = xe.Element("Server");

                    if (xe.Attribute("StartPort") != null)
                    {
                        var mPort = int.Parse(xe.Attribute("StartPort").Value);
                        re.Add("StartPort", mPort.ToString());
                    }

                    if (xe.Attribute("EndPort") != null)
                    {
                        var mEndPort = int.Parse(xe.Attribute("EndPort").Value);
                        re.Add("EndPort", mEndPort.ToString());
                    }
                }
            }
            else
            {
                re.Add("StartPort", mPort.ToString());
                re.Add("EndPort", mEndPort.ToString());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Save(string file)
        {
            XElement xx = new XElement("Config");
            XElement xe = new XElement("Server");
            xe.SetAttributeValue("StartPort", mPort);
            xe.SetAttributeValue("EndPort", mEndPort);
            xx.Add(xe);
            xx.Save(file);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagQuery"></param>
        /// <returns></returns>
        public bool Start(IRealTagProduct tagQuery, ITagHisValueProduct tagHisValueService)
        {
            Load();
            mService = new List<DataService>();

            AllowTagIds = new HashSet<int>(tagQuery.GetTagIdsByLinkAddress(this.Name+":"));
            
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

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public Dictionary<string, string> GetConfig(string database)
        {
            return LoadFromDatabase(database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="config"></param>
        public void UpdateConfig(string database, Dictionary<string, string> config)
        {
            if(config.ContainsKey("StartPort"))
            {
                mPort = int.Parse(config["StartPort"]);
            }
            if (config.ContainsKey("EndPort"))
            {
                mEndPort = int.Parse(config["EndPort"]);
            }

            SaveForDatabase(database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool Init()
        {
            Load();
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="arg"></param>
        public void OnRealTagChanged(TagChangedArg arg)
        {
            foreach (var vv in arg.AddedTags.Where(e => e.Value.StartsWith(this.Name) && !AllowTagIds.Contains(e.Key)))
            {
                AllowTagIds.Add(vv.Key);
            }

            foreach(var vv in arg.ChangedTags)
            {
                if(vv.Value.StartsWith(this.Name))
                {
                    if(!AllowTagIds.Contains(vv.Key))
                    {
                        AllowTagIds.Add(vv.Key);
                    }
                }
                else
                {
                    if(AllowTagIds.Contains(vv.Key))
                    {
                        AllowTagIds.Remove(vv.Key);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="arg"></param>
        public void OnHisTagChanged(HisTagChangedArg arg)
        {
            foreach(var vv in mService)
            {
                vv.NotifyDatabaseChangd(false, true);
            }
        }
    }
}
