//==============================================================
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

        private IRealTagProduct mTagDriverService;
        private ITagHisValueProduct mHisValueDriverService;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagDriverService"></param>
        public void Init(IRealTagProduct tagDriverService,ITagHisValueProduct hisValueService)
        {
            mTagDriverService = tagDriverService;
            mHisValueDriverService = hisValueService;
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

                foreach(var vv in mDrivers)
                {
                    try
                    {
                        vv.Value.Init();
                    }
                    catch
                    {

                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            LoggerService.Service.Info("DriverManager", "开始启动");
            foreach (var vv in mDrivers.Values)
            {
                vv.Start(mTagDriverService, mHisValueDriverService);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            LoggerService.Service.Info("DriverManager", "开始停止");
            foreach (var vv in mDrivers.Values)
            {
                vv.Stop();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Pause()
        {
            LoggerService.Service.Info("DriverManager", "开始暂停");
            foreach (var vv in mDrivers.Values)
            {
                vv.Pause();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Resume()
        {
            LoggerService.Service.Info("DriverManager", "开始恢复");
            foreach (var vv in mDrivers.Values)
            {
                vv.Resume();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addTags"></param>
        /// <param name="changedTags"></param>
        public void NotifyRealTagChanged(Dictionary<int,string> addTags,Dictionary<int,string> changedTags,Dictionary<int,string> removeChanged)
        {
            TagChangedArg tca = new TagChangedArg() { AddedTags = addTags, ChangedTags = changedTags,RemoveTags=removeChanged };
            foreach(var vv in mDrivers)
            {
                try
                {
                    vv.Value.OnRealTagChanged(tca);
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Warn("DriverManager", $"NotifyRealTagChanged { vv.Key } { ex.Message }");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="addTags"></param>
        /// <param name="changedTags"></param>
        public void NotifyHisTagChanged(IEnumerable<int> addTags, IEnumerable<int> changedTags, IEnumerable<int> removedTags)
        {
            HisTagChangedArg tca = new HisTagChangedArg() { AddedTags = addTags, ChangedTags = changedTags,RemoveTags= removedTags };
            foreach (var vv in mDrivers)
            {
                try
                {
                    vv.Value.OnHisTagChanged(tca);
                }
                catch(Exception ex)
                {
                    LoggerService.Service.Warn("DriverManager", $"NotifyHisTagChanged { vv.Key } { ex.Message }");
                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
