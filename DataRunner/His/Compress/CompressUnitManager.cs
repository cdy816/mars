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
using System.Text;

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
            return mCompressUnit.ContainsKey(type) ? mCompressUnit[type] : null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="item"></param>
        public void Registor(CompressUnitbase item)
        {
            if(!mCompressUnit.ContainsKey(item.TypeCode))
            {
                mCompressUnit.Add(item.TypeCode, item);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void LoadDefaultUnit()
        {
            Registor(new NoneCompressUnit());
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
