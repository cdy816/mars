﻿//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/28 22:15:13.
//  Version 1.0
//  种道洋
//==============================================================

using Cdy.Tag;
using Cdy.Tag.Driver;
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
    public class DriverManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        public static DriverManager Manager = new DriverManager();

        private Dictionary<string,IProducterDriver> mDrivers = new Dictionary<string, IProducterDriver>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, IProducterDriver> Drivers
        {
            get
            {
                return mDrivers;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagDriverService"></param>
        public void Init()
        {
            string cfgpath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location),"Config", "Driver.cfg");
            if(System.IO.File.Exists(cfgpath))
            {
                XElement xx = XElement.Load(cfgpath);
                foreach(var vv in xx.Elements())
                {
                    try
                    {
                        string dll = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location),vv.Attribute("File").Value);
                        string main = vv.Attribute("MainClass").Value;
                        if (System.IO.File.Exists(dll))
                        {
                            var driver = Assembly.LoadFrom(dll).CreateInstance(main) as IProducterDriver;
                            if (!mDrivers.ContainsKey(driver.Name))
                            {
                                mDrivers.Add(driver.Name, driver);
                                driver.Init();
                            }
                        }
                        else
                        {
                            LoggerService.Service.Warn("DriverManager", dll+" is not exist.");

                        }
                    }
                    catch(Exception ex)
                    {
                        LoggerService.Service.Erro("DriverManager", ex.StackTrace);
                    }
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
