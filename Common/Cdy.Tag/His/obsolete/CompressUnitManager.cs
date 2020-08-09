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
    public class CompressUnitManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, CompressUnitbase> mCompressUnit = new Dictionary<int, CompressUnitbase>();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<int, Queue<CompressUnitbase>> mPoolCompressUnits = new Dictionary<int, Queue<CompressUnitbase>>();

        /// <summary>
        /// 
        /// </summary>
        public static CompressUnitManager Manager = new CompressUnitManager();



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
        public CompressUnitbase GetCompress(int type)
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
        public CompressUnitbase GetCompressQuick(int type)
        {
            return mCompressUnit.ContainsKey(type)?mCompressUnit[type]:null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="compress"></param>
        public void ReleaseCompress(CompressUnitbase compress)
        {
            lock (mPoolCompressUnits)
            {
                if (mPoolCompressUnits.ContainsKey(compress.TypeCode))
                {
                    mPoolCompressUnits[compress.TypeCode].Enqueue(compress);
                }
                else
                {
                    var dd = new Queue<CompressUnitbase>();
                    dd.Enqueue(compress);
                    mPoolCompressUnits.Add(compress.TypeCode, dd);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        private void Registor(CompressUnitbase item)
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
                            var driver = Assembly.LoadFrom(dll).CreateInstance(main) as CompressUnitbase;
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

        ///// <summary>
        ///// 
        ///// </summary>
        //private void LoadDefaultUnit()
        //{
        //    Registor(new NoneCompressUnit());
        //}

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
