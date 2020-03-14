//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/22 10:28:31.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;
using System.Linq;

namespace Cdy.Tag
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

        public SecurityDocument Document { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public SecurityDocument Load()
        {
            return Load(PathHelper.helper.GetDataPath("local","local.sdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public SecurityDocument LoadByName(string name)
        {
            return Load(PathHelper.helper.GetDataPath(name,name + ".sdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="path"></param>
        public SecurityDocument Load(string path)
        {
            SecurityDocument db = new SecurityDocument();
            if (System.IO.File.Exists(path))
            {
                XElement xe = XElement.Load(path);

                db.Name = xe.Attribute("Name").Value;
                db.Version = xe.Attribute("Version").Value;
                
                if (xe.Element("User") != null)
                {
                    db.User = LoadUsers(xe.Element("User"));
                }

                if (xe.Element("Permissions") != null)
                {
                    db.Permission = LoadPermission(xe.Element("Permissions"));
                }
            }
            this.Document = db;
            return db;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public UserDocument LoadUsers(XElement element)
        {
            UserDocument re = new UserDocument();

            if(element.Element("Users") !=null)
            {
                re.Users = new Dictionary<string, UserItem>();
                foreach(var vv in element.Element("Users").Elements())
                {
                    var user = LoadUserItem(vv);
                    if(re.Users.ContainsKey(user.Name))
                    {
                        re.Users.Add(user.Name, user);
                    }
                }
            }

            if(element.Element("Groups") !=null)
            {
                Dictionary<string, UserGroup> groups = new Dictionary<string, UserGroup>();
                Dictionary<UserGroup, string> parents = new Dictionary<UserGroup, string>();
                foreach (var vv in element.Element("Groups").Elements())
                {
                    string sname = vv.Attribute("Name").Value;
                    string fullName = vv.Attribute("FullName").Value;
                    string parent = "";
                    if(vv.Attribute("Parent") !=null) 
                    parent = vv.Attribute("Parent").Value;
                    UserGroup grp = new UserGroup() { Name = sname };
                    if(vv.Attribute("Permissions") !=null)
                    {
                        grp.Permissions = vv.Attribute("Permissions").Value.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries).ToList();
                    }
                    groups.Add(fullName, grp);
                    parents.Add(grp, parent);
                }

                foreach (var vv in parents)
                {
                    if (!string.IsNullOrEmpty(vv.Value))
                        if (groups.ContainsKey(vv.Value))
                        {
                            vv.Key.Parent = groups[vv.Value];
                        }
                }

            }

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="element"></param>
        /// <returns></returns>
        public PermissionDocument LoadPermission(XElement element)
        {
            PermissionDocument re = new PermissionDocument();
            re.Permisstion = new Dictionary<string, PermissionItem>();
            foreach(var vv in element.Elements())
            {
                var pp = LoadPermissionItem(vv);
                if(!re.Permisstion.ContainsKey(pp.Name))
                {
                    re.Permisstion.Add(pp.Name, pp);
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <returns></returns>
        private XElement Save(UserDocument user)
        {
            XElement xe = new XElement("UserDocument");
            XElement re = new XElement("Users");
            xe.Add(re);
            foreach (var vv in user.Users)
            {
                re.Add(Save(vv.Value));
            }

            re = new XElement("Groups");
            foreach(var vv in user.Groups.Values)
            {
                XElement grp = new XElement("Group");
                grp.SetAttributeValue("Name", vv.Name);
                grp.SetAttributeValue("FullName", vv.FullName);
                if (vv.Parent != null)
                    grp.SetAttributeValue("Parent", vv.Parent.FullName);
                if(vv.Permissions!=null)
                {
                    StringBuilder sb = new StringBuilder();
                    foreach (var vvv in vv.Permissions)
                    {
                        sb.Append(vvv + ",");
                    }
                    sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                    re.SetAttributeValue("Permissions", sb.ToString());
                }
            }

            return xe;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        /// <returns></returns>
        private UserItem LoadUserItem(XElement xe)
        {
            UserItem re = new UserItem();
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        /// <returns></returns>
        private XElement Save(UserItem item)
        {
            XElement re = new XElement("User");
            re.SetAttributeValue("Name", item.Name);
            re.SetAttributeValue("Password", item.Password);
            if(!string.IsNullOrEmpty(item.Group))
            re.SetAttributeValue("Group", item.Group);
            if(item.Permissions!=null)
            {
                StringBuilder sb = new StringBuilder();
                foreach (var vv in item.Permissions)
                {
                    sb.Append(vv + ",");
                }
                sb.Length = sb.Length > 0 ? sb.Length - 1 : sb.Length;
                re.SetAttributeValue("Permissions", sb.ToString());
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="permission"></param>
        /// <returns></returns>
        private XElement Save(PermissionDocument permission)
        {
            XElement xe = new XElement("Permission");
            foreach( var vv in permission.Permisstion)
            {
                xe.Add(Save(vv.Value));
            }
            return xe;
        }

        private XElement Save(PermissionItem permission)
        {
            XElement re = new XElement("PermissionItem");
            re.SetAttributeValue("Name", permission.Name);
            re.SetAttributeValue("Desc", permission.Desc);
            re.SetAttributeValue("EnableWrite", permission.EnableWrite);
            if (permission.Group != null)
            {
                StringBuilder sb = new StringBuilder();
                foreach (var vv in permission.Group)
                {
                    sb.Append(vv + ",");
                }
                sb.Length = sb.Length > 0 ? sb.Length - 1:sb.Length;
                re.SetAttributeValue("Group", sb.ToString());
            }
            return re;
        }

        private PermissionItem LoadPermissionItem(XElement xe)
        {
            PermissionItem re = new PermissionItem();
            if(xe.Attribute("Name") !=null)
            {
                re.Name = xe.Attribute("Name").Value;
            }
            if (xe.Attribute("Desc") != null)
            {
                re.Desc = xe.Attribute("Desc").Value;
            }
            if (xe.Attribute("EnableWrite") != null)
            {
                re.EnableWrite = bool.Parse(xe.Attribute("EnableWrite").Value);
            }
            if (xe.Attribute("Group") != null)
            {
                re.Group = xe.Attribute("Group").Value.Split(new char[] { ',' },StringSplitOptions.RemoveEmptyEntries).ToList();
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void SaveAs(string name)
        {
            Save(PathHelper.helper.GetDataPath(name , name + ".sdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        public void Save()
        {
            Save(PathHelper.helper.GetDataPath(this.Document.Name,this.Document.Name + ".sdb"));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        public void Save(string sfile)
        {
            XElement doc = new XElement("Security");
            doc.SetAttributeValue("Name", this.Document.Name);
            doc.SetAttributeValue("Version", this.Document.Version);
            doc.SetAttributeValue("Auther", "cdy");

            doc.Add(Save(this.Document.User));
            doc.Add(Save(this.Document.Permission));

           
            doc.Save(sfile);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
