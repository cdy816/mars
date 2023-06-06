﻿using Cdy.Tag;
using Cdy.Tag.Common.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DBDevelopService.Security
{
    public class SecuritySerise
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        public Security Document { get; set; }
        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            string sfile = System.IO.Path.Combine(PathHelper.helper.DataPath, "Sec.sdb");
            System.IO.File.WriteAllText(sfile, Md5Helper.Encode(SaveToXML().ToString()));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public XElement SaveToXML()
        {
            XElement doc = new XElement("Security");
            doc.SetAttributeValue("Version", this.Document.Version);
            doc.SetAttributeValue("Auther", "cdy");
            doc.Add(Save(this.Document.User));
            return doc;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        private XElement Save(UserDocument user)
        {
            XElement xe = new XElement("Users");

            foreach (var vv in user.Users)
            {
                XElement xx = new XElement("User");
                xx.SetAttributeValue("Name", vv.Value.Name);
                xx.SetAttributeValue("Password", vv.Value.Password);
                xx.SetAttributeValue("NewDatabase", vv.Value.NewDatabase);
                xx.SetAttributeValue("DeleteDatabase", vv.Value.DeleteDatabase);
                xx.SetAttributeValue("IsAdmin", vv.Value.IsAdmin);
                if (vv.Value.Databases != null && vv.Value.Databases.Count > 0)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var vvv in vv.Value.Databases)
                    {
                        sb.Append(vvv + ",");
                    }
                    sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                    xx.SetAttributeValue("Database", sb.ToString());
                }
                xe.Add(xx);
            }
            return xe;
        }

        public void Load()
        {
            string sfile = System.IO.Path.Combine(PathHelper.helper.DataPath, "Sec.sdb");
            if (System.IO.File.Exists(sfile))
            {
                var txt = Md5Helper.Decode(System.IO.File.ReadAllText(sfile));
                this.Document = LoadFromXML(XElement.Parse(txt));
            }
            else
            {
                Security db = new Security();
                db.User = new UserDocument();
                db.User.AddDefaultUser();
                this.Document = db;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        public Security LoadFromXML(XElement xe)
        {
            Security db = new Security();
            db.Version = xe.Attribute("Version").Value;
            if (xe.Element("Users") != null)
            {
                db.User = LoadUser(xe.Element("Users"));
            }
            return db;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public UserDocument LoadUser(XElement xe)
        {
            var Document = new UserDocument();
            foreach (var vv in xe.Elements())
            {
                User user = new User();
                if (vv.Attribute("Name") != null)
                {
                    user.Name = vv.Attribute("Name").Value;
                }
                if (vv.Attribute("Password") != null)
                {
                    user.Password = vv.Attribute("Password").Value;
                }
                if (vv.Attribute("NewDatabase") != null)
                {
                    user.NewDatabase = bool.Parse(vv.Attribute("NewDatabase").Value);
                }
                if (vv.Attribute("DeleteDatabase") != null)
                {
                    user.DeleteDatabase = bool.Parse(vv.Attribute("DeleteDatabase").Value);
                }
                if (vv.Attribute("IsAdmin") != null)
                {
                    user.IsAdmin = bool.Parse(vv.Attribute("IsAdmin").Value);
                }

                if (vv.Attribute("Database") != null)
                {
                    string sval = vv.Attribute("Database").Value;
                    user.Databases.AddRange(sval.Split(new char[] { ',' }));
                }
                Document.AddUser(user);
            }
            return Document;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
