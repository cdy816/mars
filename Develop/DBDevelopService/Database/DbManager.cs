using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DBDevelopService
{
    public class DbManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, Cdy.Tag.Database> mDatabase = new Dictionary<string, Cdy.Tag.Database>();

        public static DbManager Instance = new DbManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool IsLoaded { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Load()
        {
            string databasePath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location) , "Data");
            
            if (System.IO.Directory.Exists(databasePath))
            {
                foreach (var vv in System.IO.Directory.EnumerateDirectories(databasePath))
                {
                    string sname = new System.IO.DirectoryInfo(vv).Name;

                    Cdy.Tag.Database db = new Cdy.Tag.DatabaseSerise().Load(sname);
                    if(!mDatabase.ContainsKey(db.Name))
                    mDatabase.Add(db.Name, db);
                    else
                    {
                        mDatabase[db.Name] = db;
                    }
                }
            }
            IsLoaded = true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void ReLoad(string database)
        {
            string databasePath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Data");

            if (System.IO.Directory.Exists(databasePath))
            {
                foreach (var vv in System.IO.Directory.EnumerateDirectories(databasePath))
                {
                    string sname = new System.IO.DirectoryInfo(vv).Name;

                    if (sname == database)
                    {
                        Cdy.Tag.Database db = new Cdy.Tag.DatabaseSerise().Load(sname);
                        if (!mDatabase.ContainsKey(db.Name))
                            mDatabase.Add(db.Name, db);
                        else
                        {
                            mDatabase[db.Name] = db;
                        }
                        break;
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Reload()
        {
            mDatabase.Clear();
            Load();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void Reload(string database)
        {
            if(mDatabase.ContainsKey(database))
            {
                mDatabase.Remove(database);
            }

            Cdy.Tag.Database db = new Cdy.Tag.DatabaseSerise().Load(database);
            if (!mDatabase.ContainsKey(db.Name))
                mDatabase.Add(db.Name, db);

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void NewDB(string name,string desc)
        {
            if (mDatabase.ContainsKey(name))
            {
                mDatabase[name] = new Cdy.Tag.Database() { Name = name,Desc=desc };
            }
            else
            {
                mDatabase.Add(name, new Cdy.Tag.Database() { Name = name,Desc=desc });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public Cdy.Tag.Database GetDatabase(string name)
        {
            if (mDatabase.ContainsKey(name))
                return mDatabase[name];
            else
                return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        /// <returns></returns>
        public bool Save(Cdy.Tag.Database database)
        {
            try
            {
                new Cdy.Tag.DatabaseSerise() { Dbase = database }.Save();
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string[] ListDatabase()
        {
            return mDatabase.Keys.ToArray();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
