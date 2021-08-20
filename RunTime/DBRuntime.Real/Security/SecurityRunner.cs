//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/9 21:36:54.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class SecurityRunner: IRuntimeSecurity
    {

        #region ... Variables  ...
        private SecurityDocument mDocument;

        private Dictionary<string, DateTime> mLastLogin = new Dictionary<string, DateTime>();

        private Dictionary<string, string> mUseIdMap = new Dictionary<string, string>();


        private Dictionary<long, DateTime> mLastLogin2 = new Dictionary<long, DateTime>();

        private Dictionary<long, string> mUseIdMap2 = new Dictionary<long, string>();

        public  int mTimeout = 10;

        private bool mIsClosed = false;

        private Thread mScanThread;

        private object mLockObj = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...
        /// <summary>
        /// 
        /// </summary>
        public SecurityDocument Document
        {
            get
            {
                return mDocument;
            }
            set
            {
                mDocument = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public int TimeOut
        {
            get
            {
                return mTimeout;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mScanThread = new Thread(ProcessTimeOut);
            mScanThread.IsBackground = true;
            mScanThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            mIsClosed = true;
        }

        /// <summary>
        /// 
        /// </summary>
        private void ProcessTimeOut()
        {
            List<string> ltmp = new List<string>();
            while(!mIsClosed)
            {
                ltmp.Clear();
                DateTime dt = DateTime.Now;
                lock (mLockObj)
                {
                    foreach (var vv in mLastLogin)
                    {
                        if ((dt - vv.Value).TotalMinutes >= mTimeout)
                        {
                            ltmp.Add(vv.Key);
                        }
                    }
                    foreach (var vv in ltmp)
                    {
                        mLastLogin.Remove(vv);
                        if (mUseIdMap.ContainsKey(vv)) mUseIdMap.Remove(vv);
                    }
                }

                Thread.Sleep(500);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool CheckLogin(string id)
        {
            lock (mLockObj)
                return mLastLogin.ContainsKey(id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool CheckLogin(long id)
        {
            lock (mLockObj)
                return mLastLogin2.ContainsKey(id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private string GetUId()
        {
            return Guid.NewGuid().ToString().Replace("-", "");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private long GetUIdByLong()
        {
            byte[] buffer = Guid.NewGuid().ToByteArray();
            var re = BitConverter.ToInt64(buffer, 0);
            if (re <= 0)
                return GetUIdByLong();
            else
                return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        public string Login(string user, string pass)
        {

            if(mDocument!=null&&mDocument.User.Users.ContainsKey(user)&&mDocument.User.Users[user].Password == pass)
            {
                string sid = Guid.NewGuid().ToString().Replace("-", "");
                lock(mLockObj)
                {
                    mLastLogin.Add(sid, DateTime.Now);
                    mUseIdMap.Add(sid, user);
                }
                return sid;
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool Logout(string id)
        {
            lock (mLockObj)
            {
                if (mLastLogin.ContainsKey(id))
                {
                    mLastLogin.Remove(id);
                }
                if (mUseIdMap.ContainsKey(id)) return mUseIdMap.Remove(id);
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool Logout(long id)
        {
            lock (mLockObj)
            {
                if (mLastLogin2.ContainsKey(id))
                {
                    mLastLogin2.Remove(id);
                }
                if (mUseIdMap2.ContainsKey(id)) return mUseIdMap2.Remove(id);
                return true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public string GetUserByLoginId(string id)
        {
            lock (mLockObj)
            {
                if (mUseIdMap.ContainsKey(id))
                {
                    return mUseIdMap[id];
                }
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public List<UserPermission> GetPermission(string id)
        {
            List<UserPermission> re = new List<UserPermission>();
           if (mDocument.User.Users.ContainsKey(id))
            {
                var vtmp = mDocument.User.Users[id].Permissions;
                if (vtmp != null)
                {
                    foreach (var vv in vtmp)
                    {
                        if(mDocument.Permission.Permissions.ContainsKey(vv))
                        {
                            re.Add(mDocument.Permission.Permissions[vv]);
                        }
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool FreshUserId(string id)
        {
            lock (mLockObj)
            {
                if (mLastLogin.ContainsKey(id))
                {
                    mLastLogin[id] = DateTime.Now;
                    return true;
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public DateTime GetLastAccessTime(string id)
        {
            lock (mLockObj)
            {
                if (mLastLogin.ContainsKey(id))
                {
                    return mLastLogin[id];
                }
                return DateTime.MinValue;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        public long Login(string user, string pass,string clientid)
        {
            lock (mLockObj)
            {
                if (mDocument != null && mDocument.User.Users.ContainsKey(user) && mDocument.User.Users[user].Password == pass)
                {
                    long sid = GetUIdByLong();
                    lock (mLockObj)
                    {
                        mLastLogin2.Add(sid, DateTime.Now);
                        mUseIdMap2.Add(sid, clientid);
                    }
                    return sid;
                }
                return -1;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool LogoutByClientId(string id)
        {
            lock (mLockObj)
            {
                var vv = mUseIdMap2.Where(e => e.Value == id);
                if (vv != null && vv.Count() > 0)
                {
                    return Logout(vv.First().Key);
                }
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        /// <param name="pass"></param>
        /// <returns></returns>
        public bool LoginOnce(string user, string pass)
        {
            return mDocument != null && mDocument.User.Users.ContainsKey(user) && mDocument.User.Users[user].Password == pass;
        }



        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
