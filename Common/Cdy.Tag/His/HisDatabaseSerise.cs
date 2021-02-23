//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDatabaseSerise
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static HisDatabaseSerise Manager = new HisDatabaseSerise();
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public HisDatabase Database { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public HisDatabase Load()
        {
            return Load(PathHelper.helper.GetDataPath("local","local.hdb"));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public HisDatabase LoadByName(string name)
        {
            return Load(PathHelper.helper.GetDataPath(name,name + ".hdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public HisSettingDoc LoadSettingOnly(string databaseName)
        {
            string stmp = PathHelper.helper.GetDataPath(databaseName, databaseName + ".hdb");
            if(System.IO.File.Exists(stmp))
            {
                XElement xe = XElement.Load(stmp);
                if (xe.Element("HisSetting") != null)
                {
                    return xe.Element("HisSetting").LoadHisSettingDocFromXML();
                }
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="target"></param>
        /// <returns></returns>
        public HisDatabase LoadDifferenceByName(string name, HisDatabase target)
        {
            return LoadDifference(PathHelper.helper.GetDataPath(name, name + ".hdb"), target);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="target"></param>
        /// <returns></returns>
        public HisDatabase LoadDifference(string file,HisDatabase target)
        {
            HisDatabase db = new HisDatabase();
            if (System.IO.File.Exists(file))
            {
                XElement xe = XElement.Load(file);

                db.Name = xe.Attribute("Name").Value;
                db.Version = xe.Attribute("Version").Value;

                if (xe.Element("Tags") != null)
                {
                    //Parallel.ForEach(xe.Element("Tags").Elements(), (vv) =>
                    //{
                    //    var tag = vv.LoadHisTagFromXML();
                    //    lock (db.HisTags)
                    //        db.HisTags.Add(tag.Id, tag);
                    //});

                    foreach (var vv in xe.Element("Tags").Elements())
                    {
                        var tag = vv.LoadHisTagFromXML();
                        
                        if (!target.HisTags.ContainsKey(tag.Id) || !tag.Equals(target.HisTags[tag.Id]))
                            db.HisTags.Add(tag.Id, tag);
                    }
                }

                if (xe.Element("HisSetting") != null)
                {
                    db.Setting = xe.Element("HisSetting").LoadHisSettingDocFromXML();
                }
            }
            this.Database = db;
            return db;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <returns></returns>
        public HisDatabase Load(string file)
        {
            HisDatabase db = new HisDatabase();
            if (System.IO.File.Exists(file))
            {
                XElement xe = XElement.Load(file);

                db.Name = xe.Attribute("Name").Value;
                db.Version = xe.Attribute("Version").Value;

                if (xe.Element("Tags") != null)
                {
                    Parallel.ForEach(xe.Element("Tags").Elements(), (vv) =>
                    {
                        var tag = vv.LoadHisTagFromXML();
                        lock (db.HisTags)
                            db.HisTags.Add(tag.Id, tag);
                    });

                    //foreach (var vv in xe.Element("Tags").Elements())
                    //{
                    //    var tag = vv.LoadHisTagFromXML();
                    //    db.HisTags.Add(tag.Id, tag);
                    //}
                }

                if(xe.Element("HisSetting") !=null)
                {
                    db.Setting = xe.Element("HisSetting").LoadHisSettingDocFromXML();
                }
            }
            this.Database = db;
            return db;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            Save(PathHelper.helper.GetDataPath(this.Database.Name, this.Database.Name + ".hdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void SaveAs(string name)
        {
            Save(PathHelper.helper.GetDataPath(name, name + ".hdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Save(string file)
        {
            XElement xe = new XElement("HisDatabase");
            xe.SetAttributeValue("Version", Database.Version);
            xe.SetAttributeValue("Name", Database.Name);
            xe.SetAttributeValue("Auther", "cdy");
            xe.Add(Database.Setting.SaveToXML());

            XElement xx = new XElement("Tags");

            foreach(var vv in Database.HisTags)
            {
                xx.Add(vv.Value.SaveToXML());
            }
            xe.Add(xx);

           

            xe.Save(file);
            Database.IsDirty = false;
        }


        public void Save(System.IO.Stream file)
        {
            XElement xe = new XElement("HisDatabase");
            xe.SetAttributeValue("Version", Database.Version);
            xe.SetAttributeValue("Name", Database.Name);
            xe.SetAttributeValue("Auther", "cdy");
            xe.Add(Database.Setting.SaveToXML());

            XElement xx = new XElement("Tags");

            foreach (var vv in Database.HisTags)
            {
                xx.Add(vv.Value.SaveToXML());
            }
            xe.Add(xx);
            xe.Save(file);
            
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
