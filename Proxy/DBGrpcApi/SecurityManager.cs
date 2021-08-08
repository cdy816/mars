using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cdy.Tag;

namespace DBGrpcApi
{
    public class SecurityManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static SecurityManager Manager = new SecurityManager();

        private Dictionary<string, List<string>> mUserReaderPermissionCach = new Dictionary<string, List<string>>();


        private Dictionary<string, List<string>> mUserWriterPermissionCach = new Dictionary<string, List<string>>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, bool> mSuperUsers = new Dictionary<string, bool>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public bool IsLogin(string token)
        {
            var rs = ServiceLocator.Locator.Resolve<IRuntimeSecurity>();
            if (rs != null)
                return rs.CheckLogin(token);
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public bool CheckWritePermission(string token, string group)
        {
            var securityService = ServiceLocator.Locator.Resolve<IRuntimeSecurity>();

            var user = securityService.GetUserByLoginId(token);
            lock (mSuperUsers)
            {
                try
                {
                    if (mUserWriterPermissionCach.ContainsKey(user))
                    {
                        return mSuperUsers[user] || (mUserWriterPermissionCach[user].Contains(group));
                    }
                    else
                    {
                        var pps = securityService.GetPermission(user);
                        bool issuerper = false;
                        List<string> mgroups = new List<string>();
                        foreach (var vv in pps)
                        {
                            issuerper = issuerper | vv.SuperPermission;
                            if (vv.Group != null && vv.EnableWrite)
                                mgroups.AddRange(vv.Group);
                        }
                        if (mSuperUsers.ContainsKey(user)) mSuperUsers[user] = issuerper;
                        else mSuperUsers.Add(user, issuerper);
                        mUserWriterPermissionCach.Add(user, mgroups);

                        return mSuperUsers[user] || (mUserReaderPermissionCach[user].Contains(group));
                    }
                }
                catch (Exception ex)
                {
                    LoggerService.Service.Warn("DbInRunWebApi_SecurityManager", ex.Message);
                    return false;
                }
            }

        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public bool CheckReaderPermission(string token, string group)
        {

            var securityService = ServiceLocator.Locator.Resolve<IRuntimeSecurity>();
            var user = securityService.GetUserByLoginId(token);

            lock (mSuperUsers)
            {
                try
                {
                    if (mUserReaderPermissionCach.ContainsKey(user))
                    {
                        return mSuperUsers[user] || (mUserReaderPermissionCach[user].Contains(group));
                    }
                    else
                    {
                        var pps = securityService.GetPermission(user);
                        bool issuerper = false;
                        List<string> mgroups = new List<string>();
                        foreach (var vv in pps)
                        {
                            issuerper = issuerper | vv.SuperPermission;
                            if (vv.Group != null)
                                mgroups.AddRange(vv.Group);
                        }

                        if (mSuperUsers.ContainsKey(user)) mSuperUsers[user] = issuerper;
                        else mSuperUsers.Add(user, issuerper);

                        mUserReaderPermissionCach.Add(user, mgroups);

                        return mSuperUsers[user] || (mUserReaderPermissionCach[user].Contains(group));
                    }
                }
                catch (Exception ex)
                {
                    LoggerService.Service.Warn("DbInRunWebApi_SecurityManager", ex.Message);
                    return false;
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
