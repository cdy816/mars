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
using System.Linq;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ComplexTagClassDocumentSerise
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
        public ComplexTagClassDocument Document { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public ComplexTagClassDocument Load()
        {
            return Load(PathHelper.helper.GetDataPath("local", "local.cls"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public ComplexTagClassDocument LoadByName(string name)
        {
           return  Load(PathHelper.helper.GetDataPath(name, name+ ".cls"));
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        public ComplexTagClassDocument Load(string path)
        {
            ComplexTagClassDocument db = new ComplexTagClassDocument();
            if (System.IO.File.Exists(path))
            {
                var xx = XElement.Load(path);
                db.Name = xx.Attribute("Name") != null ? xx.Attribute("Name").Value : "";
                db.Version = xx.Attribute("Version") != null ? xx.Attribute("Version").Value : "";
                foreach (var xe in xx.Element("Class").Elements())
                {
                    db.AddClass(xe.LoadComplexTagClassFromXML());
                }
            }
            this.Document = db;
            return db;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void SaveAs(string name)
        {
            Save(PathHelper.helper.GetDataPath(name , name + ".cls"));
        }

        /// <summary>
        /// 
        /// </summary>
        public bool Save()
        {
           return Save(PathHelper.helper.GetDataPath(this.Document.Name,this.Document.Name + ".cls"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        public bool Save(string sfile)
        {
            XElement doc = new XElement("ComplexTagClassDocument");
            doc.SetAttributeValue("Name", Document.Name);
            doc.SetAttributeValue("Version", Document.Version);
            doc.SetAttributeValue("Auther", "cdy");

            XElement xe = new XElement("Class");
            foreach (var vv in Document.Class.Values)
            {
                xe.Add(vv.SaveToXML());
            }
            doc.Add(xe);
            return doc.SaveXMLToFile(sfile, "ComplexTagClassSerise");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="stream"></param>
        public void Save(System.IO.Stream stream)
        {
            XElement doc = new XElement("ComplexTagClassDocument");
            doc.SetAttributeValue("Name", Document.Name);
            doc.SetAttributeValue("Version", Document.Version);
            doc.SetAttributeValue("Auther", "cdy");

            XElement xe = new XElement("Class");
            foreach (var vv in Document.Class.Values)
            {
                xe.Add(vv.SaveToXML());
            }
            doc.Add(xe);
            doc.Save(stream);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
