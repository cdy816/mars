using Cdy.Tag;
using System.Xml.Linq;

namespace DBWebMonitor
{
    public class MarsProxy
    {

        #region ... Variables  ...
        
        private string mCurrentDatabase;
        private bool mIsLogin;
        private DBHighApi.ApiClient mDSHelper;
        private string mIp="127.0.0.1";
        private string mUser="Admin";
        private string mPass="Admin";

        private string mLoginUserId;

        public static int Port = 14332;

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
            mDSHelper = new DBHighApi.ApiClient();
            
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
            if(!mDSHelper.IsConnected)
            mDSHelper.Open(mIp, Port);
            this.IsLogin = mDSHelper.Login(mUser, mPass);
            mCurrentDatabase = mDSHelper.GetRunnerDatabase();
            return this.IsLogin;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filters"></param>
        /// <param name="totalCount"></param>
        /// <param name="skip"></param>
        /// <param name="take"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<Cdy.Tag.Tagbase, Cdy.Tag.HisTag>> QueryTags(Dictionary<string, string> filters, out int totalCount, int skip = 0, int take = 0, int timeout = 30000)
        {
            return mDSHelper.QueryTags(filters,out totalCount, skip, take, timeout);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Dictionary<string,int> GetTagStatisticsInfos()
        {
            return mDSHelper.GetTagStatisticsInfos();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<string> ListALlTagGroup(int timeout = 30000)
        {
            return mDSHelper.ListALlTagGroup();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public object QueryValueBySql(string sql, int timeout = 5000)
        {
            return mDSHelper.QueryValueBySql(sql,timeout);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<object, DateTime, byte>> ReadRealValue(List<int> ids)
        {
            return mDSHelper.GetRealData(ids);
        }

        public HisQueryResult<T> QueryAllHisValue<T>(int id, DateTime startTime, DateTime endTime, int timeout = 5000)
        {
            return mDSHelper.QueryAllHisValue<T>(id, startTime, endTime, timeout);
        }

        public HisQueryResult<T> QueryHisValueAtTimes<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            return mDSHelper.QueryHisValueAtTimes<T>(id,times,matchType, timeout);
        }

        public HisQueryResult<T> QueryHisValueAtTimesByIgnorSystemExit<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            return mDSHelper.QueryHisValueAtTimesByIgnorSystemExit<T>(id,times,matchType,timeout);
        }

        public HisQueryResult<T> QueryHisValueForTimeSpan<T>(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            return mDSHelper.QueryHisValueForTimeSpan<T>(id,startTime,endTime,span,matchType,timeout);
        }

        public HisQueryResult<T> QueryHisValueForTimeSpanByIgnorSystemExit<T>(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            return mDSHelper.QueryHisValueForTimeSpanByIgnorSystemExit<T>(id, startTime, endTime, span, matchType, timeout);
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
