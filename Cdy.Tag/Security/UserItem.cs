//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/2/16 11:03:00.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    public class UserItem
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Group { get; set; }

        /// <summary>
        /// 超级用户
        /// </summary>
        public bool SuperUser { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> Permissions { get; set; } = new List<string>();
    }
}
