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
    public class CompressUnitManager2
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, CompressUnitbase2> mCompressUnit = new Dictionary<int, CompressUnitbase2>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, Queue<CompressUnitbase2>> mPoolCompressUnits = new Dictionary<int, Queue<CompressUnitbase2>>();

        /// <summary>
        /// 
        /// </summary>
        public static CompressUnitManager2 Manager = new CompressUnitManager2();



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
        /// <param name="type"></param>
        /// <returns></returns>
        public CompressUnitbase2 GetCompress(int type)
        {
            lock (mPoolCompressUnits)
            {
                if (mCompressUnit.ContainsKey(type))
                {
                    if (mPoolCompressUnits.ContainsKey(type) && mPoolCompressUnits[type].Count > 0)
                    {
                        return mPoolCompressUnits[type].Dequeue();
                    }
                    else
                    {
                        return mCompressUnit[type].Clone();
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        public CompressUnitbase2 GetCompressQuick(int type)
        {
            return mCompressUnit.ContainsKey(type)?mCompressUnit[type]:null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="compress"></param>
        public void ReleaseCompress(CompressUnitbase2 compress)
        {
            lock (mPoolCompressUnits)
            {
                if (mPoolCompressUnits.ContainsKey(compress.TypeCode))
                {
                    mPoolCompressUnits[compress.TypeCode].Enqueue(compress);
                }
                else
                {
                    var dd = new Queue<CompressUnitbase2>();
                    dd.Enqueue(compress);
                    mPoolCompressUnits.Add(compress.TypeCode, dd);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        private void Registor(CompressUnitbase2 item)
        {
            if(!mCompressUnit.ContainsKey(item.TypeCode))
            {
                mCompressUnit.Add(item.TypeCode, item);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            string cfgpath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "Config", "Compress.cfg");
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
                            var driver = Assembly.LoadFrom(dll).CreateInstance(main) as CompressUnitbase2;
                            if(driver!=null)
                            Registor(driver);
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

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
