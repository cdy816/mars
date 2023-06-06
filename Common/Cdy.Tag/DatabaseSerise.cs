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
using System.IO;
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

            Dbase.ComplexTagClass = new ComplexTagClassDocumentSerise().LoadByName(name);

            Dbase.ComplexTagClass.Name = name;

            Dbase.RealDatabase.Owner = Dbase;
            Dbase.HisDatabase.Owner = Dbase;

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
            Dbase.ComplexTagClass = new ComplexTagClassDocumentSerise().LoadByName(name);
            if(string.IsNullOrEmpty(Dbase.ComplexTagClass.Name))
            {
                Dbase.ComplexTagClass.Name = name;
            }
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
            Dbase.RealDatabase.Owner = Dbase;
            Dbase.HisDatabase.Owner = Dbase;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sname"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public Database LoadDifference(string name, Database database, out List<int> mRemovedRealTags, out List<long> mRemovedHisTags)
        {
            var Dbase = LoadDatabaseSelf(PathHelper.helper.GetDataPath(name, name + ".db"));
            Dbase.Security = new SecuritySerise().LoadByName(name);
            Dbase.RealDatabase = new RealDatabaseSerise().LoadDifferenceByName(name, database.RealDatabase,out mRemovedRealTags);
            Dbase.HisDatabase = new HisDatabaseSerise().LoadDifferenceByName(name, database.HisDatabase,out mRemovedHisTags);
            Dbase.RealDatabase.Owner = Dbase;
            Dbase.HisDatabase.Owner = Dbase;
            return Dbase;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="database"></param>
        /// <param name="hiscompareaction"></param>
        /// <returns></returns>
        public Database LoadDifference(string name, RealDatabase database,Func<HisTag,bool> hiscompareaction,out List<int> mRemovedRealTags)
        {
            var Dbase = LoadDatabaseSelf(PathHelper.helper.GetDataPath(name, name + ".db"));
            Dbase.Security = new SecuritySerise().LoadByName(name);
            Dbase.RealDatabase = new RealDatabaseSerise().LoadDifferenceByName(name, database,out mRemovedRealTags);
            Dbase.HisDatabase = new HisDatabaseSerise().LoadDifferenceByName(name, hiscompareaction);
            Dbase.RealDatabase.Owner = Dbase;
            Dbase.HisDatabase.Owner = Dbase;
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
            Dbase.RealDatabase.Owner = Dbase;
            
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
        private bool SaveDatabaseSelf(string path)
        {

            XElement doc = new XElement("RealDatabase");
            doc.SetAttributeValue("Name", Dbase.Name);
            doc.SetAttributeValue("Desc", Dbase.Desc);
            doc.SetAttributeValue("Version", Dbase.Version);
            doc.SetAttributeValue("Auther", "cdy");

            doc.Add(Save(Dbase.Setting));

            return doc.SaveXMLToFile(path, "SaveDatabaseSelf");
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

            if (xe.Attribute("EnableWebApi") != null)
            {
                doc.EnableWebApi = bool.Parse(xe.Attribute("EnableWebApi").Value);
            }

            if (xe.Attribute("EnableGrpcApi") != null)
            {
                doc.EnableGrpcApi = bool.Parse(xe.Attribute("EnableGrpcApi").Value);
            }

            if (xe.Attribute("EnableHighApi") != null)
            {
                doc.EnableHighApi = bool.Parse(xe.Attribute("EnableHighApi").Value);
            }

            if (xe.Attribute("EnableOpcServer") != null)
            {
                doc.EnableOpcServer = bool.Parse(xe.Attribute("EnableOpcServer").Value);
            }

            if (xe.Attribute("WorkMode") != null)
            {
                if(xe.Attribute("WorkMode").Value.Length>2)
                {
                    doc.HisWorkMode = (HisWorkMode)Enum.Parse(typeof(HisWorkMode), xe.Attribute("WorkMode").Value);
                }
                else
                {
                    doc.HisWorkMode = (HisWorkMode)int.Parse(xe.Attribute("WorkMode").Value);
                }
               
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
            xe.SetAttributeValue("EnableWebApi", doc.EnableWebApi);
            xe.SetAttributeValue("EnableGrpcApi", doc.EnableGrpcApi);
            xe.SetAttributeValue("EnableHighApi", doc.EnableHighApi);
            xe.SetAttributeValue("EnableOpcServer", doc.EnableOpcServer);
            xe.SetAttributeValue("WorkMode", (int)doc.HisWorkMode);
            return xe;
        }


        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            bool re = true;
            re = SaveDatabaseSelf(PathHelper.helper.GetDataPath(Dbase.Name, Dbase.Name + ".db"));
            re &= new RealDatabaseSerise() { Database = Dbase.RealDatabase }.Save();
            re &= new HisDatabaseSerise() { Database = Dbase.HisDatabase }.Save();
            re &= new SecuritySerise() { Document = Dbase.Security }.Save();
            re &= new ComplexTagClassDocumentSerise() { Document = Dbase.ComplexTagClass }.Save();
            re &= SaveRDDCSecurity(Dbase.Name);
            if(!re)
            {
                LoggerService.Service.Erro("DatabaseSerise", "Save failed!");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        private bool SaveRDDCSecurity(string databaseName)
        {
            string sfile = PathHelper.helper.GetDataPath(databaseName, "RDDC.cfg");
            if(!System.IO.File.Exists(sfile))
            {
                XElement xx = new XElement("RDDC");
                xx.SetAttributeValue("Enable", false);
                xx.SetAttributeValue("Port", 7000);
                xx.SetAttributeValue("RemoteIp", "127.0.0.1");
                return xx.SaveXMLToFile(sfile, "SaveRDDCSecurity");
            }
            return true;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
