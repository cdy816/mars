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
    public class RealDatabaseSerise
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        public static RealDatabaseSerise Manager = new RealDatabaseSerise();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public RealDatabase Database { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public RealDatabase Load()
        {
            return Load(PathHelper.helper.GetDataPath("local", "local.xdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public RealDatabase LoadByName(string name)
        {
           return  Load(PathHelper.helper.GetDataPath(name, name+".xdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        public RealDatabase Load(string path)
        {
            RealDatabase db = new RealDatabase();
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

                Dictionary<string, TagGroup> groups = new Dictionary<string, TagGroup>();
                Dictionary<TagGroup, string> parents = new Dictionary<TagGroup, string>(); 
                if(xe.Element("Groups")!=null)
                {
                    foreach (var vv in xe.Element("Groups").Elements())
                    {
                        TagGroup group = new TagGroup();
                        group.Name = vv.Attribute("Name").Value;
                        string parent = vv.Attribute("Parent") != null ? xe.Attribute("Parent").Value : "";

                        string fullName = vv.Attribute("FullName").Value;

                        if(!groups.ContainsKey(fullName))
                        {
                            groups.Add(fullName, group);
                        }

                        parents.Add(group, parent);
                    }
                }
                db.Groups = groups;

                foreach(var vv in parents)
                {
                    if(!string.IsNullOrEmpty(vv.Value) && db.Groups.ContainsKey(vv.Value))
                    {
                        vv.Key.Parent = db.Groups[vv.Value];
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
            Save(PathHelper.helper.GetDataPath(name , name + ".xdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            Save(PathHelper.helper.GetDataPath(this.Database.Name,this.Database.Name + ".xdb"));
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
