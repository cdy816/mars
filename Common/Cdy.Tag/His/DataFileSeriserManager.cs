//==============================================================
//  Copyright (C) 2019  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2019/12/27 18:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class DataFileSeriserManager
    {

        #region ... Variables  ...

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, DataFileSeriserbase> mDataFiles = new Dictionary<string, DataFileSeriserbase>();

        /// <summary>
        /// 
        /// </summary>
        public static DataFileSeriserManager manager = new DataFileSeriserManager();

        /// <summary>
        /// 
        /// </summary>
        public static string mDefaultSeriseName = "LocalFile";

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public DataFileSeriserManager()
        {
            //Init();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        ///// <summary>
        ///// 
        ///// </summary>
        //private void Init()
        //{
        //    LocalFileSeriser s = new LocalFileSeriser();
        //    Registor(s.Name, s);
        //}

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            mDataFiles.Clear();
            string cfgpath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Config", "DataFileSerise.cfg");
            if (System.IO.File.Exists(cfgpath))
            {
                XElement xx = XElement.Load(cfgpath);
                foreach (var vv in xx.Elements())
                {
                    try
                    {
                        string dll = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), vv.Attribute("File").Value);
                        string main = vv.Attribute("MainClass").Value;
                        if (System.IO.File.Exists(dll))
                        {
                            var driver = Assembly.LoadFrom(dll).CreateInstance(main) as DataFileSeriserbase;
                            Registe(driver.Name,driver);
                        }
                        else
                        {
                            LoggerService.Service.Warn("CompressUnitManager", dll + " is not exist.");

                        }
                    }
                    catch (Exception ex)
                    {
                        LoggerService.Service.Erro("DriverManager", ex.StackTrace);
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <param name="datafile"></param>
        private void Registe(string name, DataFileSeriserbase datafile)
        {
            if (!mDataFiles.ContainsKey(name))
            {
                mDataFiles.Add(name, datafile);
            }
            else
            {
                mDataFiles[name] = datafile;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>

        public DataFileSeriserbase GetSeriser(string name)
        {
            if (mDataFiles.ContainsKey(name))
            {
                return mDataFiles[name];
            }
            return null;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public DataFileSeriserbase GetDefaultFileSersie()
        {
            var re = GetSeriser(mDefaultSeriseName);
            return re != null ? re.New() : null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
