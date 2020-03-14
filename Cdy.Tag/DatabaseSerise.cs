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
        private Database LoadDatabaseSelf(string path)
        {
            Database doc = new Database();

            if (System.IO.File.Exists(path))
            {
                XElement xe = XElement.Load(path);

                doc.Name = xe.Attribute("Name").Value;
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
            doc.SetAttributeValue("Version", Dbase.Version);
            doc.SetAttributeValue("Auther", "cdy");

            doc.Add(Save(Dbase.Setting));

            doc.Save(path);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        private SettingDoc LoadSetting(XElement xe)
        {
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="doc"></param>
        /// <returns></returns>
        private XElement Save(SettingDoc doc)
        {
            XElement xe = new XElement("Setting");
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
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
