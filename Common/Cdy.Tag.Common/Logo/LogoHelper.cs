//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/26 12:44:42.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace Cdy.Tag
{
    public static class LogoHelper
    {

        #region ... Variables  ...

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
        public static void Print()
        {
            //"Cdy.Tag.Common.Logo.Logo.txt"
            Console.WriteLine(new StreamReader(typeof(LogoHelper).Assembly.GetManifestResourceStream("Cdy.Tag.Common.Logo.Logo.txt")).ReadToEnd());
        }
        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
