//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/12 8:52:25.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;

namespace DotNetty.Common
{
    //
    // 摘要:
    //     Represents a type that can create instances of Microsoft.Extensions.Logging.ILogger.
    public interface ILoggerProvider : IDisposable
    {
        //
        // 摘要:
        //     Creates a new Microsoft.Extensions.Logging.ILogger instance.
        //
        // 参数:
        //   categoryName:
        //     The category name for messages produced by the logger.
        ILogger CreateLogger(string categoryName);
    }
}
