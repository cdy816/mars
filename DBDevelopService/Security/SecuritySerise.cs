using System;
using System.Collections.Generic;
using System.Linq;
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

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Save(string file)
        {
            XElement doc = new XElement("Security");
            doc.SetAttributeValue("Version", this.Document.Version);
            doc.SetAttributeValue("Auther", "cdy");
            doc.Add(Save(this.Document.User));
            doc.Add(Save(this.Document.Permission));
            doc.Save(file);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Load()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Load(string file)
        {
            Security db = new Security();
            if (System.IO.File.Exists(file))
            {
                XElement xe = XElement.Load(file);

                db.Version = xe.Attribute("Version").Value;

                if (xe.Element("User") != null)
                {
                    db.User = LoadUser(xe.Element("User"));
                }

                if (xe.Element("Permissions") != null)
                {
                    db.Permission = LoadPermission(xe.Element("Permissions"));
                }
            }
            this.Document = db;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public UserDocument LoadUser(XElement element)
        {
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public PermissionDocument LoadPermission(XElement element)
        {
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        private XElement Save(UserDocument user)
        {
            XElement xe = new XElement("User");
            return xe;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="permission"></param>
        /// <returns></returns>
        private XElement Save(PermissionDocument permission)
        {
            XElement xe = new XElement("Permission");
            return xe;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
