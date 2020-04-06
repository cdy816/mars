//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/4/4 18:23:59.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace DBInStudio.Desktop
{
    /// <summary>
    /// 
    /// </summary>
    public class SecurityTreeItemViewModel:HasChildrenTreeItemViewModel
    {
        public SecurityTreeItemViewModel()
        {
            Name = Res.Get("Security");
        }
    }
}
