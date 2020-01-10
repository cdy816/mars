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
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DatabaseManager
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public static DatabaseManager Manager = new DatabaseManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public Database Database { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public Database Load()
        {
            return Load(PathHelper.helper.GetDataPath("local.xdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void LoadByName(string name)
        {
            Load(PathHelper.helper.GetDataPath(name+".xdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        public Database Load(string path)
        {
            Database db = new Database();
            if (System.IO.File.Exists(path))
            {
                XElement xe = XElement.Load(path);

                db.Name = xe.Attribute("Name").Value;
                db.Version = xe.Attribute("Version").Value;

                if(xe.Element("Tags") !=null)
                {
                    foreach(var vv in xe.Element("Tags").Elements())
                    {
                        var tag = vv.LoadTagFromXML();
                        db.Tags.Add(tag.Id, tag);
                    }
                }
                if(xe.Element("Groups")!=null)
                {
                    foreach (var vv in xe.Element("Groups").Elements())
                    {
                        var grp = vv.LoadTagGroupFromXML();
                        if(!db.Groups.ContainsKey(grp.FullNameString))
                        {
                            db.Groups.Add(grp.FullNameString, grp);
                        }
                    }
                }

                foreach(var vv in db.Groups.Values)
                {
                    if(!string.IsNullOrEmpty(vv.ParentName) && db.Groups.ContainsKey(vv.ParentName))
                    {
                        vv.Parent = db.Groups[vv.ParentName];
                    }
                }

            }
            this.Database = db;
            return db;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void SaveAs(string name)
        {
            Save(PathHelper.helper.GetDataPath(name + "/" + name + ".xdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            Save(PathHelper.helper.GetDataPath(this.Database.Name + ".xdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        public void Save(string sfile)
        {
            XElement doc = new XElement("RealDatabase");
            doc.SetAttributeValue("Name", Database.Name);
            doc.SetAttributeValue("Version", Database.Version);
            doc.SetAttributeValue("Auther", "cdy");
            XElement xe = new XElement("Tags");
            foreach(var vv in Database.Tags.Values)
            {
                xe.Add(vv.SaveToXML());
            }
            doc.Add(xe);

            xe = new XElement("Groups");
            foreach(var vv in Database.Groups.Values)
            {
                xe.Add(vv.SaveToXML());
            }
            doc.Add(xe);

            string sd = System.IO.Path.GetDirectoryName(sfile);
            if(!System.IO.Directory.Exists(sd))
            {
                System.IO.Directory.CreateDirectory(sd);
            }
            doc.Save(sfile);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
