using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService
{
    public class UserDocument
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, User> mUsers = new Dictionary<string, User>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>

        public Dictionary<string,User> Users
        {
            get
            {
                return mUsers;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public User GetUser(string name)
        {
            if (mUsers.ContainsKey(name))
            {
                return mUsers[name];
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        public void AddUser(User user)
        {
            if(!mUsers.ContainsKey(user.Name))
            {
                mUsers.Add(user.Name, user);
            }

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void RemoveUser(string name)
        {
            if (mUsers.ContainsKey(name))
            {
                mUsers.Remove(name);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="user"></param>
        public void ModifyUser(User user)
        {
            if (mUsers.ContainsKey(user.Name))
            {
                mUsers[user.Name] = user;
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
