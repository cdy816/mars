using Cdy.Tag.Common.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DBDevelopService.Security
{
    public class UserDocumentSerise
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public UserDocument Document { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Sec.sdb");
            System.IO.File.WriteAllText(sfile, Md5Helper.Encode(SaveToXML().ToString()));
        }

        private XElement SaveToXML()
        {
            XElement xe = new XElement("UserDoc");
            xe.SetAttributeValue("Version", Document.Version);
            foreach(var vv in Document.Users)
            {
                XElement xx = new XElement("User");
                xx.SetAttributeValue("Name", vv.Value.Name);
                xx.SetAttributeValue("Password", vv.Value.Password);
                xx.SetAttributeValue("NewDatabase", vv.Value.NewDatabase);
                xx.SetAttributeValue("DeleteDatabase", vv.Value.DeleteDatabase);
                xx.SetAttributeValue("IsAdmin", vv.Value.IsAdmin);
                if(vv.Value.Databases!=null&&vv.Value.Databases.Count>0)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach(var vvv in vv.Value.Databases)
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

        /// <summary>
        /// 
        /// </summary>
        public void Load()
        {
            string sfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Sec.sdb");
            var txt = System.IO.File.ReadAllText(sfile);
            LoadFromXML(XElement.Parse(txt));
        }

        public void LoadFromXML(XElement xe)
        {
            Document = new UserDocument();
            if(xe.Attribute("Version") !=null)
            {
                Document.Version = xe.Attribute("Version").Value;
            }
            foreach(var vv in xe.Elements())
            {
                User user = new User();
                if(vv.Attribute("Name") !=null)
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
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
