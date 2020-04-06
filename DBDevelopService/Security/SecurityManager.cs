using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService
{
    /// <summary>
    /// 
    /// </summary>
    public class SecurityManager
    {

        #region ... Variables  ...
        
        /// <summary>
        /// 
        /// </summary>
        public static SecurityManager Manager = new SecurityManager();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, string> mLogins = new Dictionary<string, string>();

        /// <summary>
        /// 
        /// </summary>
        private List<string> mAvaiableKey = new List<string>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Security.Security Securitys { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            Security.SecuritySerise ss = new Security.SecuritySerise();
            ss.Load();
            this.Securitys = ss.Document;
         }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        private bool LoginInner(string userName,string pass)
        {
            if(Securitys!=null&&Securitys.User.Users.ContainsKey(userName))
            {
                return Securitys.User.Users[userName].Password == pass;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <returns></returns>
        public bool CheckKeyAvaiable(string key)
        {
            return mAvaiableKey.Contains(key);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="loginId"></param>
        /// <returns></returns>
        public string GetUserName(string loginId)
        {
            var users = mLogins.Where(e => e.Value == loginId).FirstOrDefault().Key;
            return users;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="key"></param>
        /// <param name="permission"></param>
        /// <returns></returns>
        public bool CheckPermission(string key,string permission)
        {
            if (string.IsNullOrEmpty(permission)) return true;

            var users = mLogins.Where(e => e.Value == key).FirstOrDefault().Key;
            if(Securitys.User.Users.ContainsKey(users))
            {
                var us = Securitys.User.Users[users];
                if(us.Permissions.Contains(permission)) return true;

                foreach(var vv in us.Permissions)
                {
                    if(Securitys.Permission.Permissions.ContainsKey(vv)&& Securitys.Permission.Permissions[vv].IsAdmin)
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public string Login(string userName,string password,string database="local")
        {
            if(LoginInner(userName,password))
            {
                string sid = Guid.NewGuid().ToString();
                string skey = userName;
                if (!mLogins.ContainsKey(userName))
                {
                    mLogins.Add(userName, sid);
                    mAvaiableKey.Add(sid);
                }
                else
                {
                    sid = mLogins[userName];
                }
                return sid;
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="userName"></param>
        public void Logout(string id)
        {
            var user = mLogins.Where(e => e.Value == id);
            if(user.Count()>0)
            {
               var userName = user.FirstOrDefault().Key;
                if (mLogins.ContainsKey(userName))
                {
                    var ids = mLogins[userName];
                    if (mAvaiableKey.Contains(ids))
                    {
                        mAvaiableKey.Remove(ids);
                    }
                    mLogins.Remove(userName);
                }
            }
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
