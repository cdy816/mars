//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/16 10:10:13.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag
{
    public class DatabaseSerise
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static DatabaseSerise manager = new DatabaseSerise();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Database Dbase { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public Database Load(string name)
        {
            Dbase = LoadDatabaseSelf(PathHelper.helper.GetDataPath(name,name + ".db"));
            
            Dbase.RealDatabase = new RealDatabaseSerise().LoadByName(name);
            
            Dbase.HisDatabase = new HisDatabaseSerise().LoadByName(name);

            Dbase.Security = new SecuritySerise().LoadByName(name);

            return Dbase;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public Database PartLoad(string name)
        {
            Dbase = LoadDatabaseSelf(PathHelper.helper.GetDataPath(name, name + ".db"));
            Dbase.Security = new SecuritySerise().LoadByName(name);
            Dbase.RealDatabase = null;
            Dbase.HisDatabase = null;
            return Dbase;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void ContinuePartLoad(string name)
        {
            Dbase.RealDatabase = new RealDatabaseSerise().LoadByName(name);
            Dbase.HisDatabase = new HisDatabaseSerise().LoadByName(name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sname"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public Database LoadDifference(string name, Database database)
        {
            var Dbase = LoadDatabaseSelf(PathHelper.helper.GetDataPath(name, name + ".db"));
            Dbase.Security = new SecuritySerise().LoadByName(name);
            Dbase.RealDatabase = new RealDatabaseSerise().LoadDifferenceByName(name, database.RealDatabase);
            Dbase.HisDatabase = new HisDatabaseSerise().LoadDifferenceByName(name, database.HisDatabase);
            return Dbase;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="database"></param>
        /// <param name="hiscompareaction"></param>
        /// <returns></returns>
        public Database LoadDifference(string name, RealDatabase database,Func<HisTag,bool> hiscompareaction)
        {
            var Dbase = LoadDatabaseSelf(PathHelper.helper.GetDataPath(name, name + ".db"));
            Dbase.Security = new SecuritySerise().LoadByName(name);
            Dbase.RealDatabase = new RealDatabaseSerise().LoadDifferenceByName(name, database);
            Dbase.HisDatabase = new HisDatabaseSerise().LoadDifferenceByName(name, hiscompareaction);
            return Dbase;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public Database LoadRealDatabase(string name)
        {
            Dbase = LoadDatabaseSelf(PathHelper.helper.GetDataPath(name, name + ".db"));
            Dbase.RealDatabase = new RealDatabaseSerise().LoadByName(name);
            Dbase.Security = new SecuritySerise().LoadByName(name);
            return Dbase;
        }

        /// <summary>
        /// 
        /// </summary>
        private Database LoadDatabaseSelf(string path)
        {
            Database doc = new Database();

            if (System.IO.File.Exists(path))
            {
                XElement xe = XElement.Load(path);

                doc.Name = xe.Attribute("Name").Value;
                doc.Desc = xe.Attribute("Desc") != null ? xe.Attribute("Desc").Value : string.Empty;
                doc.Version = xe.Attribute("Version").Value;

                if (xe.Element("Setting") != null)
                {
                    doc.Setting = LoadSetting(xe.Element("Setting"));
                }
            }

            return doc;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        private void SaveDatabaseSelf(string path)
        {

            XElement doc = new XElement("RealDatabase");
            doc.SetAttributeValue("Name", Dbase.Name);
            doc.SetAttributeValue("Desc", Dbase.Desc);
            doc.SetAttributeValue("Version", Dbase.Version);
            doc.SetAttributeValue("Auther", "cdy");

            doc.Add(Save(Dbase.Setting));

            var spath = System.IO.Path.GetDirectoryName(path);
            if(!System.IO.Directory.Exists(spath))
            {
                System.IO.Directory.CreateDirectory(spath);
            }

            doc.Save(path);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        private SettingDoc LoadSetting(XElement xe)
        {
            SettingDoc doc = new SettingDoc();
            if (xe.Attribute("RealDataServerPort") != null)
            {
                doc.RealDataServerPort = int.Parse(xe.Attribute("RealDataServerPort").Value);
            }
            return doc;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="doc"></param>
        /// <returns></returns>
        private XElement Save(SettingDoc doc)
        {
            XElement xe = new XElement("Setting");
            xe.SetAttributeValue("RealDataServerPort", doc.RealDataServerPort);
            return xe;
        }


        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            SaveDatabaseSelf(PathHelper.helper.GetDataPath(Dbase.Name, Dbase.Name + ".db"));
            new RealDatabaseSerise() { Database = Dbase.RealDatabase }.Save();
            new HisDatabaseSerise() { Database = Dbase.HisDatabase }.Save();
            new SecuritySerise() { Document = Dbase.Security }.Save();
            SaveRDDCSecurity(Dbase.Name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        private void SaveRDDCSecurity(string databaseName)
        {
            string sfile = PathHelper.helper.GetDataPath(databaseName, "RDDC.cfg");
            if(!System.IO.File.Exists(sfile))
            {
                XElement xx = new XElement("RDDC");
                xx.SetAttributeValue("Enable", false);
                xx.SetAttributeValue("Port", 7000);
                xx.SetAttributeValue("RemoteIp", "127.0.0.1");
                xx.Save(sfile);
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
