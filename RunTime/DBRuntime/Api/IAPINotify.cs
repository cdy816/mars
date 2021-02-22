//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by chongdaoyang at 2021/2/22 13:37:00.
//  Version 1.0
//  CHONGDAOYANGPC
//==============================================================
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public interface IAPINotify
    {
        void NotifyDatabaseChanged(bool realtag,bool histag,bool security);
    }
}
