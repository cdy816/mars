//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/12 15:35:17.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Text;
using System.Linq;

namespace HisDataTools
{
    /// <summary>
    /// 
    /// </summary>
    public class HisDataManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static HisDataManager Manager = new HisDataManager();


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...


        /// <summary>
        /// 
        /// </summary>
        public List<string> Databases { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        public IHisQuery GetQueryService(string database)
        {
            return new QuerySerivce(database);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public Dictionary<string, Tuple<int, byte>> GetHisTagIds(string database)
        {
            Dictionary<string, Tuple<int, byte>> re = new Dictionary<string, Tuple<int, byte>>();
            var mDatabase = new DatabaseSerise().Load(database);
            var vtags = mDatabase.RealDatabase.Tags.Select(e=> new KeyValuePair<string,int>(e.Value.Name,e.Key));
            foreach(var vv in mDatabase.HisDatabase.HisTags)
            {
                if(mDatabase.RealDatabase.Tags.ContainsKey((int)vv.Key))
                {
                    string sname = mDatabase.RealDatabase.Tags[(int)vv.Key].Name;
                    if(!re.ContainsKey(sname))
                    {
                        re.Add(sname, new Tuple<int, byte>((int)vv.Key, (byte)vv.Value.TagType));
                    }
                }
            }
            return re;
        }


        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            DataFileSeriserManager.manager.Init();
            CompressUnitManager2.Manager.Init();
            ScanDatabase();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="databaseName"></param>
        public void ScanDatabase(string databaseName)
        {
            string dbpath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Data");
            if (System.IO.Directory.Exists(dbpath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(dbpath).EnumerateDirectories())
                {
                    if (vv.Name == databaseName && !HisQueryManager.Instance.CheckDatabaseIsRegistor(databaseName))
                    {
                        var setting = new HisDatabaseSerise().LoadSettingOnly(vv.Name);
                        if (setting != null)
                        {
                            HisQueryManager.Instance.Registor(vv.Name, setting.HisDataPathPrimary, setting.HisDataPathBack);
                        }
                        else
                        {
                            HisQueryManager.Instance.Registor(vv.Name);
                        }
                    }
                }

                HisQueryManager.Instance.StartMonitor();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void ScanDatabase()
        {
            List<string> bds = new List<string>();
            string dbpath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Data");
            if (System.IO.Directory.Exists(dbpath))
            {
                foreach (var vv in new System.IO.DirectoryInfo(dbpath).EnumerateDirectories())
                {
                    bds.Add(vv.Name);
                    //var setting = new HisDatabaseSerise().LoadSettingOnly(vv.Name);
                    //if (setting != null)
                    //{
                    //    HisQueryManager.Instance.Registor(vv.Name, setting.HisDataPathPrimary, setting.HisDataPathBack);
                    //}
                    //else
                    //{
                    //    HisQueryManager.Instance.Registor(vv.Name);
                    //}
                }
            }
            Databases = bds;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
