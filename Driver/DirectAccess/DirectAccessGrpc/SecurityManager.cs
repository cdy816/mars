using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DirectAccessGrpc
{
    public class SecurityManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static SecurityManager Manager = new SecurityManager();

        private Dictionary<string, DateTime> mLoginCach = new Dictionary<string, DateTime>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        public int Timeout { get; set; } = 10 * 60;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Clear()
        {
            lock(mLoginCach)
            mLoginCach.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void RemoveUser(string id)
        {
            lock (mLoginCach)
            {
                if (mLoginCach.ContainsKey(id))
                {
                    mLoginCach.Remove(id);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void CachUser(string id)
        {
            lock (mLoginCach)
            {
                if (!mLoginCach.ContainsKey(id))
                {
                    mLoginCach.Add(id, DateTime.Now);
                }
                else
                {
                    mLoginCach[id] = DateTime.Now;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool IsLogin(string id)
        {
            lock (mLoginCach)
            {
                DateTime dnow = DateTime.Now;
                if (mLoginCach.ContainsKey(id))
                {
                    var re = (dnow - mLoginCach[id]).TotalSeconds < Timeout;
                    if (re)
                    {
                        mLoginCach[id] = dnow;
                    }
                    return re;
                }
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        public void RefreshLogin(string id)
        {
            lock (mLoginCach)
            {
                if (mLoginCach.ContainsKey(id))
                {
                    mLoginCach[id] = DateTime.Now;
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
