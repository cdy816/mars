//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/13 9:14:21.
//  Version 1.0
//  种道洋
//==============================================================

using DotNetty.Transport.Channels;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.Text;
using System.Runtime.InteropServices;

namespace Cdy.Tag
{
    public class ClientChannelHandler: ChannelHandlerAdapter
    {

        #region ... Variables  ...
        private  SocketClient mParent;
        private IChannelHandlerContext mContext;
        private static PooledByteBufferAllocator pooledByteBufAllocator;

        public delegate void DataArrivedDelegate(byte fun, IByteBuffer datas);

        public event DataArrivedDelegate DataArrivedEvent;


        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        static ClientChannelHandler()
        {
            pooledByteBufAllocator = new PooledByteBufferAllocator(true);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="parent"></param>
        public ClientChannelHandler(SocketClient parent)
        {
            mParent = parent;
        }
        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="fun"></param>
        public void Write(byte[] values,byte fun)
        {
            var buffer = pooledByteBufAllocator.DirectBuffer(values.Length+1);
            buffer.WriteByte(fun);
            Marshal.Copy(values, 0, buffer.AddressOfPinnedMemory()+1, values.Length);
            buffer.SetWriterIndex(values.Length + 1);
            mContext?.WriteAndFlushAsync(buffer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <param name="fun"></param>
        public void Write(byte[] values,int offset,int len, byte fun)
        {
            var buffer = pooledByteBufAllocator.DirectBuffer(len+1);
            buffer.WriteByte(fun);
            Marshal.Copy(values, offset, buffer.AddressOfPinnedMemory()+1,len);
            buffer.SetWriterIndex(len + 1);
            mContext?.WriteAndFlushAsync(buffer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="len"></param>
        /// <param name="fun"></param>
        public unsafe void Write(IntPtr value,int len, byte fun)
        {
            var buffer = pooledByteBufAllocator.DirectBuffer(len+1);
            buffer.WriteByte(fun);
            Buffer.MemoryCopy((void*)(value), (void*)(buffer.AddressOfPinnedMemory()+1), len, len);
            buffer.SetWriterIndex(len + 1);
            mContext?.WriteAndFlushAsync(buffer);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        public void Write(IByteBuffer values)
        {
            mContext?.WriteAndFlushAsync(values);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelActive(IChannelHandlerContext context)
        {
            mContext = context;
            mParent.IsConnected = true;
            base.ChannelActive(context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelInactive(IChannelHandlerContext context)
        {
            mParent.IsConnected = false;
            base.ChannelInactive(context);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        /// <param name="message"></param>
        public override void ChannelRead(IChannelHandlerContext context, object message)
        {
            IByteBuffer buffer = message as IByteBuffer;
            if (buffer.IsReadable() && buffer.Capacity>1)
            {
                var fun = buffer.ReadByte();
                DataArrivedEvent?.Invoke(fun, buffer);
            }
            base.ChannelRead(context, message);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="context"></param>
        public override void ChannelReadComplete(IChannelHandlerContext context) => context.Flush();

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

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
