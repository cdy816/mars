//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/16 10:25:16.
//  Version 1.0
//  种道洋
//==============================================================

using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public static class ByteBufferExtends
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
        /// <param name="buffer"></param>
        /// <param name="value"></param>
        public static void WriteString(this IByteBuffer buffer, string value)
        {
            var indx = buffer.WriterIndex;
            buffer.WriteInt(0);
            buffer.WriteString(value, Encoding.UTF8);
            int len = buffer.WriterIndex - indx - 4;
            buffer.MarkWriterIndex();
            buffer.SetWriterIndex(indx);
            buffer.WriteInt(len);
            buffer.ResetWriterIndex();


        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public static string ReadString(this IByteBuffer buffer)
        {
            int dsize = buffer.ReadInt();
            return buffer.ReadString(dsize, Encoding.UTF8);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        public static void ReleaseBuffer(this IByteBuffer buffer)
        {
            if (buffer.ReferenceCount == 0) return;
            buffer.Release();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="buffer"></param>
        public static void RetainBuffer(this IByteBuffer buffer)
        {
            if (buffer.ReferenceCount == 0) return;
            buffer.Retain();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
