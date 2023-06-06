using DBRuntimeServer;
using static DBRuntimeServer.DBServer;

namespace DBWebMonitor
{
    public class RuntimeClient
    {
        private DBRuntimeServer.Client mClient;

        public string UserName { get; set; }

        public string Password { get; set; }

        public string Database { get; set; }

        bool mIsLogin=false;
        /// <summary>
        /// 
        /// </summary>
        public RuntimeClient()
        {
            mClient = new DBRuntimeServer.Client("127.0.0.1", 14000);
        }

        public void Login()
        {
            mIsLogin= mClient.Login(UserName, Password, Database);
        }

        private void CheckLogin()
        {
            if(!mIsLogin)
            {
                Login();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public List<string> GetLogs(DateTime starttime, DateTime endtime, string type)
        {
            CheckLogin();
            return mClient.GetLogs(Database,starttime, endtime, type);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public MachineResourceItem GetHostResource()
        {
            CheckLogin();
            return mClient.GetHostResource();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public MachineInfo? GetMachineInfo()
        {
            CheckLogin();
            return mClient.GetMachineInfo();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<DiskInfoItem> GetDiskInfo()
        {
            CheckLogin();
            return mClient.GetDiskInfo(Database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<ApiItem> GetApiSetting()
        {
            CheckLogin();
            return mClient.GetApiSetting(Database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<ProcessInfoItem> GetProcessInfo()
        {
            CheckLogin();
            return mClient.GetProcessInfo();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="api"></param>
        /// <returns></returns>
        public bool IsApiIsRunning(string api)
        {
            CheckLogin();
            return mClient.IsApiIsRunning(api);
        }
    }
}
