//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/13 8:48:01.
//  Version 1.0
//  种道洋
//==============================================================

using DotNetty.Buffers;
using DotNetty.Transport.Channels;
using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ServerChannelHandler: ChannelHandlerAdapter
    {
        SocketServer mServer;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        public delegate IByteBuffer DataArrivedDelegate(IChannelHandlerContext context,byte fun, IByteBuffer datas);

        /// <summary>
        /// 
        /// </summary>
        public  DataArrivedDelegate DataArrived { get; set; }

        //private static PooledByteBufferAllocator pooledByteBufAllocator;

        ///// <summary>
        ///// 
        ///// </summary>
        //static ServerChannelHandler()
        //{
        //    pooledByteBufAllocator = new PooledByteBufferAllocator(true);
        //}

        /// <summary>
        /// 
        /// </summary>
        public ServerChannelHandler(SocketServer server)
        {
            mServer = server;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(IChannelHandlerContext mContext, byte[] values,int len,byte fun)
        {
            var buffer = BufferManager.Manager.Allocate(len + 1);
            buffer.WriteByte(fun);
            Marshal.Copy(values, 0, buffer.AddressOfPinnedMemory() + 1, len);
            buffer.SetWriterIndex(len + 1);
            mContext?.WriteAndFlushAsync(buffer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mContext"></param>
        /// <param name="values"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <param name="fun"></param>
        public void Write(IChannelHandlerContext mContext, byte[] values, int offset, int len, byte fun)
        {
            var buffer = BufferManager.Manager.Allocate(len + 1);
            buffer.WriteByte(fun);
            Marshal.Copy(values, offset, buffer.AddressOfPinnedMemory() + 1, len);
            buffer.SetWriterIndex(len + 1);
            mContext?.WriteAndFlushAsync(buffer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mContext"></param>
        /// <param name="value"></param>
        /// <param name="len"></param>
        /// <param name="fun"></param>
        public unsafe void Write(IChannelHandlerContext mContext, IntPtr value, int len, byte fun)
        {
            var buffer = BufferManager.Manager.Allocate(len + 1);
            buffer.WriteByte(fun);
            Buffer.MemoryCopy((void*)(value), (void*)(buffer.AddressOfPinnedMemory() + 1), len, len);
            buffer.SetWriterIndex(len + 1);
            mContext?.WriteAndFlushAsync(buffer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(IChannelHandlerContext mContext, IByteBuffer values)
        {
            mContext?.WriteAndFlushAsync(values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelActive(IChannelHandlerContext context)
        {
            mServer.Registor(context);
            base.ChannelActive(context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelInactive(IChannelHandlerContext context)
        {
            mServer.UnRegistor(context);
            base.ChannelInactive(context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="message"></param>
        public  override void ChannelRead(IChannelHandlerContext context, object message)
        {
            IByteBuffer buffer = message as IByteBuffer;
            if (buffer.IsReadable())
            {
                var fun = buffer.ReadByte();
                if (DataArrived != null)
                {
                    var vdd = DataArrived(context, fun, buffer);
                    if (vdd != null && vdd.ReferenceCount>0)
                    {
                        Write(context, vdd);
                    }
                }
            }
            base.ChannelRead(context, message);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelReadComplete(IChannelHandlerContext context)
        {
            context.Flush();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="exception"></param>
        public override void ExceptionCaught(IChannelHandlerContext context, Exception exception)
        {
            Console.WriteLine("Exception: " + exception);
            context.CloseAsync();
        }

    }
}
