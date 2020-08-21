using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    public class Database
    {

        private string mName = string.Empty;

        private string mVersion= "0.0.1";

        /// <summary>
        /// 
        /// </summary>
        public Database()
        {
            Setting = new SettingDoc();
            Security = new SecurityDocument() { Name = this.Name };
        }

        /// <summary>
        /// 
        /// </summary>
        public string Name 
        {
            get { return mName; } 
            set
            {
                mName = value; 
                if(Security!=null)
                Security.Name = value;
                if (RealDatabase != null)
                    RealDatabase.Name = value;
                if (HisDatabase != null)
                    HisDatabase.Name = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public string Desc { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Version 
        { 
            get { return mVersion; } 
            set {
                    mVersion = value;
                if (Security != null)
                    Security.Version = value;
                if (RealDatabase != null)
                    RealDatabase.Version = value;
                if (HisDatabase != null)
                    HisDatabase.Version = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public RealDatabase RealDatabase { get; set; } = new RealDatabase();

        /// <summary>
        /// 
        /// </summary>
        public HisDatabase HisDatabase { get; set; } = new HisDatabase();

        /// <summary>
        /// 
        /// </summary>
        public SettingDoc Setting { get; set; } = new SettingDoc();


        /// <summary>
        /// 
        /// </summary>
        public SecurityDocument Security { get; set; } = new SecurityDocument();


        public bool IsDirty
        {
            get
            {
                return RealDatabase.IsDirty || HisDatabase.IsDirty;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="version"></param>
        /// <returns></returns>
        public static Database New(string name,string version="0.0.1")
        {
            Database db = new Database();
            
            db.RealDatabase = new RealDatabase();
            db.HisDatabase = new HisDatabase() { Setting = new HisSettingDoc() };
            db.Name = name;
            db.Version = version;
            return db;
        }

    }
}
