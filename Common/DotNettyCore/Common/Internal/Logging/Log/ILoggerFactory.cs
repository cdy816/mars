//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/12 8:59:22.
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
    //     Represents a type used to configure the logging system and create instances of
    //     Microsoft.Extensions.Logging.ILogger from the registered Microsoft.Extensions.Logging.ILoggerProviders.
    public interface ILoggerFactory : IDisposable
    {
        //
        // 摘要:
        //     Adds an Microsoft.Extensions.Logging.ILoggerProvider to the logging system.
        //
        // 参数:
        //   provider:
        //     The Microsoft.Extensions.Logging.ILoggerProvider.
        void AddProvider(ILoggerProvider provider);
        //
        // 摘要:
        //     Creates a new Microsoft.Extensions.Logging.ILogger instance.
        //
        // 参数:
        //   categoryName:
        //     The category name for messages produced by the logger.
        //
        // 返回结果:
        //     The Microsoft.Extensions.Logging.ILogger.
        ILogger CreateLogger(string categoryName);
    }
}
