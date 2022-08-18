using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntimeMonitor.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class DatabaseManagerViewModel:WindowViewModelBase
    {
        private List<string> mListDatabase=new List<string>();

        /// <summary>
        /// 
        /// </summary>
        public DatabaseManagerViewModel(string title):this()
        {
            this.Title = title;
        }

        /// <summary>
        /// 
        /// </summary>
        public DatabaseManagerViewModel()
        {
            DefaultWidth = 460;
            DefaultHeight = 200;
        }

        private string mHost="127.0.0.1";
        public string Host
        {
            get
            {
                return mHost;
            }
            set
            {
                if (mHost != value)
                {
                    mHost = value;
                    ListDatabase = ListDatabases();
                }
                OnPropertyChanged("Host");
            }
        }

        private string mDatabase="";
        public string Database
        {
            get
            {
                return mDatabase;
            }
            set
            {
                mDatabase = value;
                OnPropertyChanged("Database");
            }
        }


        private string mUserName="";
        public string UserName
        {
            get
            {
                return mUserName;
            }
            set
            {
                mUserName = value;
                OnPropertyChanged("UserName");
            }
        }


        private string mPassword="";
        public string Password
        {
            get
            {
                return mPassword;
            }
            set
            {
                mPassword = value;
                OnPropertyChanged("Password");
            }
        }

        private bool mIsEnable = true;

        /// <summary>
        /// 
        /// </summary>
        public bool IsEnable
        {
            get
            {
                return mIsEnable;
            }
            set
            {
                if (mIsEnable != value)
                {
                    mIsEnable = value;
                    OnPropertyChanged("IsEnable");
                }
            }
        }



        public List<string> ListDatabase
        {
            get
            {
                return mListDatabase;
            }
            set
            {
                if(mListDatabase!=null)
                {
                    mListDatabase = value;
                }
                OnPropertyChanged("ListDatabase");
            }
        }

        private List<string> ListDatabases()
        {
            try
            {
                IsEnable = false;
                DBRuntimeServer.Client client = new DBRuntimeServer.Client(mHost, 14000) { UserName = mUserName, Password = Password };
                return client.ListDatabase();
            }
            finally
            {
                IsEnable = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void SetDatabase(Database database)
        {
            Host = database.HostAddress;
            Database = database.DatabseName;
            UserName = database.UserName;
            Password = database.Password;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Load()
        {
            Task.Run(() => {
                ListDatabase = ListDatabases();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Database GetDatabase()
        {
            return new Database() { HostAddress = Host, DatabseName = Database, UserName = UserName, Password = Password };
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        protected override bool CanOKCommandProcess()
        {
            return !string.IsNullOrEmpty(UserName);
        }

    }
}
