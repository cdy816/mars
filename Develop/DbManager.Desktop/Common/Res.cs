﻿//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/3/29 11:05:05.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace DBInStudio.Desktop
{
    public class Res
    {
        public static string Get(string name)
        {
            string str = Properties.Resources.ResourceManager.GetString(name, Thread.CurrentThread.CurrentUICulture);
            return string.IsNullOrEmpty(str) ? name : str;
        }
    }
}
