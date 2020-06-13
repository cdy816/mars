//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/12 8:45:22.
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
    //     Represents a type used to perform logging.
    //
    // 言论：
    //     Aggregates most logging patterns to a single method.
    public interface ILogger
    {
        //
        // 摘要:
        //     Begins a logical operation scope.
        //
        // 参数:
        //   state:
        //     The identifier for the scope.
        //
        // 返回结果:
        //     An IDisposable that ends the logical operation scope on dispose.
        IDisposable BeginScope<TState>(TState state);
        //
        // 摘要:
        //     Checks if the given logLevel is enabled.
        //
        // 参数:
        //   logLevel:
        //     level to be checked.
        //
        // 返回结果:
        //     true if enabled.
        bool IsEnabled(LogLevel logLevel);
        //
        // 摘要:
        //     Writes a log entry.
        //
        // 参数:
        //   logLevel:
        //     Entry will be written on this level.
        //
        //   eventId:
        //     Id of the event.
        //
        //   state:
        //     The entry to be written. Can be also an object.
        //
        //   exception:
        //     The exception related to this entry.
        //
        //   formatter:
        //     Function to create a string message of the state and exception.
        void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception exception, Func<TState, Exception, string> formatter);
    }
}
