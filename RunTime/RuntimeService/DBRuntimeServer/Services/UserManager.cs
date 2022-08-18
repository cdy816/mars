namespace DBRuntimeServer
{
    public class UserManager
    {

        public static UserManager Manager = new UserManager();

        private Dictionary<string, DateTime> mLoginUsers = new Dictionary<string, DateTime>();

        /// <summary>
        /// 登录超时，分钟
        /// </summary>
        public int TimeOut { get { return 10; } }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public string Login(string database,string username,string password)
        {
            var re = Cdy.Tag.ServiceLocator.Locator.Resolve<DBRuntimeAPI.IDatabaseService>().CheckDatabaseUser(database, username, password);
            if(re)
            {
                var id = Guid.NewGuid().ToString();
                lock (mLoginUsers)
                    mLoginUsers.Add(id, DateTime.Now);
                return id;
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        /// <returns></returns>
        public bool Logout(string token)
        {
            lock(mLoginUsers)
            if(mLoginUsers.ContainsKey(token))
            {
                mLoginUsers.Remove(token);
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        public void Fresh(string token)
        {
            lock (mLoginUsers)
            {
                if (mLoginUsers.ContainsKey(token))
                    mLoginUsers[token] = DateTime.Now;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="token"></param>
        public bool CheckLogin(string token)
        {
            return mLoginUsers.ContainsKey(token);
        }

    }
}
