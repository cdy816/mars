using Cdy.Tag;
using System.Xml.Linq;

namespace DBWebStudio
{
    public class MarsProxy
    {

        #region ... Variables  ...
        
        private string mCurrentDatabase;
        private bool mIsLogin;
        private DBDevelopClientApi.DevelopServiceHelper mDSHelper;
        private string mIp="127.0.0.1";
        private string mUser="Admin";
        private string mPass="Admin";

        private string mLoginUserId;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public MarsProxy()
        {
            Init();
        }
        #endregion ...Constructor...

        #region ... Properties ...

        public Action RefreshAction { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsLogin
        {
            get
            {
                return mIsLogin;
            }
            set
            {
                if (mIsLogin != value)
                {
                    mIsLogin = value;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string CurrentDatabase
        {
            get
            {
                return mCurrentDatabase;
            }
            set
            {
                if (mCurrentDatabase != value)
                {
                    mCurrentDatabase = value;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string CurrentUser { get { return mUser; }}
        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            mDSHelper = new DBDevelopClientApi.DevelopServiceHelper();
            mDSHelper.UseTls = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool Login(string username, string password)
        {
            mUser = username;
            mPass = password;
            mLoginUserId = mDSHelper.Login(mIp, mUser, mPass);
            this.IsLogin = !string.IsNullOrEmpty(mLoginUserId);
            return this.IsLogin;
        }

        public bool Save()
        {
            return mDSHelper.Save(CurrentDatabase);
        }

        public bool Cancel()
        {
            return mDSHelper.Cancel(CurrentDatabase);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool ReRun()
        {
            return mDSHelper.ReRunDatabase(CurrentDatabase);
        }


        public bool Start()
        {
            return mDSHelper.StartDatabase(CurrentDatabase);
        }

        public bool Stop()
        {
            return mDSHelper.StopDatabase(CurrentDatabase);
        }


        public bool IsStarted()
        {
            return mDSHelper.IsDatabaseRunning(CurrentDatabase);
        }
        

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Dictionary<string,string> ListDatabase()
        {
            return mDSHelper.ListDatabase();
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="callback"></param>
        public void QueryAllTag(Action<int, int, Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>>> callback)
        {
            mDSHelper.QueryAllTag(CurrentDatabase, callback);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="filter"></param>
        /// <param name="mfilters"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagByGroup(string group,Dictionary<string,string> mfilters)
        {
            return mDSHelper.QueryTagByGroup(CurrentDatabase, group, mfilters);
        }


        public Dictionary<int, Tuple<Tagbase, HisTag>> QueryTagByGroup(string group, int index, out int totalCount, out int tagCount, Dictionary<string, string> mFilters = null)
        {
            return mDSHelper.QueryTagByGroup(CurrentDatabase,group, index, out totalCount, out tagCount, mFilters);
        }

        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagSubTags(int id, Dictionary<string, string> mFilters = null)
        {
            return mDSHelper.QueryTagSubTags(CurrentDatabase,id,mFilters);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cls"></param>
        /// <returns></returns>
        public bool UpdateLinkedClassTags(string cls)
        {
            return mDSHelper.UpdateLinkedClassTags(CurrentDatabase,cls);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="index"></param>
        /// <param name="totalCount"></param>
        /// <param name="tagCount"></param>
        /// <param name="mFilters"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTagByClass(string group, int index, out int totalCount, out int tagCount, Dictionary<string, string> mFilters = null)
        {
            return mDSHelper.QueryTagByClass(CurrentDatabase, group, index, out totalCount,out tagCount, mFilters);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="basename"></param>
        /// <returns></returns>
        public string GetAvaiableTagName(string group,string basename)
        {
            return mDSHelper.GetAvaiableTagName(CurrentDatabase,group,basename);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="basename"></param>
        /// <returns></returns>
        public string GetAvaiableClassTagName(string group,string basename)
        {
            return mDSHelper.GetAvaiableClassTagName(CurrentDatabase,group,basename);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public bool ReNameTagGroup(string oldName, string newName)
        {
            return mDSHelper.ReNameTagGroup(CurrentDatabase, oldName, newName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <returns></returns>
        public bool ClearTagByGroup(string group)
        {
            return mDSHelper.ClearTagByGroup(CurrentDatabase, group);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool ClearAllTag()
        {
            return mDSHelper.ClearTagAll(CurrentDatabase);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveTagGroup(string database, string name)
        {
            return mDSHelper.RemoveTagGroup(CurrentDatabase, name);
        }

        public bool Remove( int id)
        {
            return mDSHelper.Remove(CurrentDatabase,id);
        }

        public bool RemoveTagClass( string name)
        {
            return mDSHelper.RemoveTagClass(CurrentDatabase, name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cls"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool RemoveClassTag(string cls, int id)
        {
            return mDSHelper.RemoveClassTag(CurrentDatabase, cls,id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <param name="name"></param>
        /// <param name="parentName"></param>
        /// <returns></returns>
        public string AddTagGroup(string database, string name, string parentName)
        {
            return mDSHelper.AddTagGroup(CurrentDatabase, name,parentName);
        }

        public string AddTagClass( string name)
        {
            return mDSHelper.AddTagClass(CurrentDatabase, name);
        }

        public bool RenameTagClass(string oldname, string newName)
        {
            return mDSHelper.ReNameTagClass(CurrentDatabase, oldname, newName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool AddTag(Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag, out int id)
        {
            return mDSHelper.AddTag(CurrentDatabase,tag, out id);
        }

        public bool AddClassTag( string classname, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag, out int id)
        {
            return mDSHelper.AddClassTag(CurrentDatabase,classname, tag, out id);
        }

        /// <summary>
        /// 导入数据
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="mode"></param>
        /// <param name="id"></param>
        /// <returns></returns>
        public bool Import( Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag, int mode, out int id)
        {
            return mDSHelper.Import(CurrentDatabase,tag,mode,out id);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool UpdateTag(Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag)
        {
            return mDSHelper.UpdateTag(CurrentDatabase, tag);
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="cls"></param>
        /// <param name="tag"></param>
        /// <returns></returns>
        public bool UpdateClassTag(string cls, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag> tag)
        {
            return mDSHelper.UpdateClassTag(CurrentDatabase,cls, tag);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="group"></param>
        /// <param name="tagname"></param>
        /// <returns></returns>
        public bool CheckTagNameExits(string group, string tagname)
        {
            return mDSHelper.CheckTagNameExits(CurrentDatabase, group, tagname);
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="desc"></param>
        /// <returns></returns>
        public bool NewDatabase(string name,string desc)
        {
           return  mDSHelper.NewDatabase(name, desc);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool CanNewDatabase()
        {
            return mDSHelper.CanNewDatabase();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<Tuple<string, string, string>> ListTagGroup()
        {
            return mDSHelper.QueryTagGroups(mCurrentDatabase);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<Tuple<string,string>> ListClass()
        {
            return mDSHelper.QueryTagClass(mCurrentDatabase);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Dictionary<string, Tuple<string[], string>> GetRegistorDrivers()
        {
            return mDSHelper.GetRegistorDrivers(CurrentDatabase);
        }

        /// <summary>
        /// 获取驱动配置
        /// </summary>
        /// <param name="driver"></param>
        /// <returns></returns>
        public Dictionary<string, string> GetDriverSetting(string driver)
        {
            return mDSHelper.GetDriverSetting(CurrentDatabase, driver);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool UpdateDriverSetting(string name,Dictionary<string, string> value)
        {
            return mDSHelper.UpdateDriverSetting(CurrentDatabase, name, value);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="datapath"></param>
        /// <param name="databackpath"></param>
        /// <param name="keeptime"></param>
        /// <param name="keepNoZipTime"></param>
        /// <returns></returns>
        public bool UpdateHisSetting(string datapath,string databackpath,int keeptime,double keepNoZipTime)
        {
            return mDSHelper.UpdateHisSetting(CurrentDatabase, datapath, databackpath, keeptime, keepNoZipTime);
        }

        public Tuple<string,string,int,double> GetHisSetting()
        {
            return mDSHelper.GetHisSetting(CurrentDatabase);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="webapi"></param>
        /// <param name="grpcapi"></param>
        /// <param name="highapi"></param>
        /// <param name="opcserver"></param>
        /// <returns></returns>
        public bool UpdateApiSetting(bool webapi,bool grpcapi,bool highapi,bool opcserver)
        {
            try
            {
                mDSHelper.UpdateApiSetting(CurrentDatabase, webapi, grpcapi, highapi, opcserver);
                return true;
            }
            catch
            { return false; }
        }

        public Tuple<bool,bool,bool,bool> GetApiSetting()
        {
            return mDSHelper.GetApiSetting(CurrentDatabase);
        }

        public int  GetRealServerPort()
        {
            return mDSHelper.GetRealServerPort(CurrentDatabase);
        }

        public bool SetRealServerPort(int port)
        {
            return mDSHelper.SetRealServerPort(CurrentDatabase, port);
        }

        public List<Cdy.Tag.UserItem> GetUsersByGroup()
        {
            return mDSHelper.GetUsersByGroup(CurrentDatabase, "");
        }


        public bool UpdateDatabaseUser(Cdy.Tag.UserItem user)
        {
            return mDSHelper.UpdateDatabaseUser(CurrentDatabase, user);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveDatabaseUser(string name)
        {
            return mDSHelper.RemoveDatabaseUser(CurrentDatabase, name);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public bool RemoveDatabasePermission(string name)
        {
            return mDSHelper.RemoveDatabasePermission(CurrentDatabase,name);
        }

        public bool UpdateDatabasePermission(Cdy.Tag.UserPermission pers)
        {
            return mDSHelper.UpdateDatabasePermission(CurrentDatabase, pers);
        }

        public bool RenameDatabasePermission(string name, string newName)
        {
            return mDSHelper.ReNameDatabasePermission(CurrentDatabase, newName, name);
        }

        public bool RenameDatabaseUser(string oldname,string newname)
        {
            return mDSHelper.ReNameDatabaseUser(CurrentDatabase,oldname,newname);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool UpdateDatabaseUserPassword(string username,string password) { 
            return mDSHelper.UpdateDatabaseUserPassword(CurrentDatabase, username, password);
        }

        public List<Cdy.Tag.UserPermission> GetAllDatabasePermission()
        {
            return mDSHelper.GetAllDatabasePermission(CurrentDatabase);
        }

        public List<string> GetAllDatabaseUserNames()
        {
            return mDSHelper.GetAllDatabaseUserNames(CurrentDatabase);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldpassword"></param>
        /// <param name="newpassword"></param>
        /// <returns></returns>
        public bool ModifyUserPassword(string oldpassword,string newpassword)
        {
            return mDSHelper.ModifyUserPassword(CurrentDatabase,oldpassword,newpassword);
        }

        public bool AddUser(string name, string password, bool isadmin, bool cannewdatabase)
        {
            return mDSHelper.AddUser(name,password,isadmin,cannewdatabase);
        }

        public bool RemoveUser(string name)
        {
            return mDSHelper.RemoveUser(name);
        }

        public bool ReNameUser(string oldname, string newname)
        {
            return mDSHelper.ReNameUser(oldname,newname);
        }

        public Dictionary<string, Tuple<bool, bool, List<string>>> GetUsers()
        {
            return mDSHelper.GetUsers();
        }

        public bool IsAdmin()
        {
            return mDSHelper.IsAdmin();
        }

        public bool UpdateUser(string name, bool isadmin, bool cannewdatabase, List<string> databases)
        {
            return mDSHelper.UpdateUser(name,isadmin,cannewdatabase,databases);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
