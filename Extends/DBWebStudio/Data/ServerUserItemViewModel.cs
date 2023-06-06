namespace DBWebStudio.Data
{
    public class ServerUserItemViewModel
    {
        private string mName;
        private string mPassword;
        private bool mIsAdmin;
        private bool mCanNewDatabase;
        private List<string> mDatabases = new List<string>();

        public bool IsChanged { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Databases
        {
            get
            {
                return mDatabases;
            }
            set
            {
                if (mDatabases != value)
                {
                    mDatabases = value;
                    IsChanged = true;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool CanNewDatabase
        {
            get
            {
                return mCanNewDatabase;
            }
            set
            {
                if (mCanNewDatabase != value)
                {
                    mCanNewDatabase = value;
                    IsChanged = true;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public bool IsAdmin
        {
            get
            {
                return mIsAdmin;
            }
            set
            {
                if (mIsAdmin != value)
                {
                    mIsAdmin = value;
                    IsChanged = true;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public string Name
        {
            get
            {
                return mName;
            }
            set
            {
                if (mName != value)
                {
                    mName = value;
                    IsChanged = true;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public bool CanRename()
        {
            return mName != "Admin";
        }

        public bool CanDelete()
        {
            return mName != "Admin";
        }

        /// <summary>
        /// 
        /// </summary>
        public string Password
        {
            get
            {
                return mPassword;
            }
            set
            {
                if (mPassword != value)
                {
                    mPassword = value;
                    IsChanged = true;
                }
            }
        }

    }
}
