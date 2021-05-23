//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/13 9:07:48.
//  Version 1.0
//  种道洋
//==============================================================

using DotNetty.Buffers;
using DotNetty.Codecs;
using DotNetty.Handlers.Tls;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Channels.Sockets;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Net;
using System.Net.Security;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class SocketClient:INotifyPropertyChanged
    {

        #region ... Variables  ...
        private IChannel clientChannel;
        private MultithreadEventLoopGroup group;
        private ClientChannelHandler mChannelHandler;
        private bool mIsConnected;

        public event PropertyChangedEventHandler PropertyChanged;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 是否使用异步通信库
        /// </summary>
        public bool UseLibuv { get; set; } = true;

        /// <summary>
        /// 是否使用Certificate
        /// </summary>
        private bool IsSsl { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        public bool IsConnected
        {
            get { return mIsConnected; }
            set
            {
                if (mIsConnected != value)
                {
                    mIsConnected = value;
                    if (PropertyChanged != null)
                    {
                        Task.Run(() => { PropertyChanged(this, new PropertyChangedEventArgs("IsConnected")); OnConnectChanged(value); });
                    }
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool NeedReConnected { get; set; } = true;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        protected virtual void OnConnectChanged(bool value)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public async void Connect(string ip,int port)
        {
            group = new MultithreadEventLoopGroup();

            X509Certificate2 cert = null;
            string targetHost = null;
            if (IsSsl)
            {
               // cert = new X509Certificate2(Path.Combine(ExampleHelper.ProcessDirectory, "dotnetty.com.pfx"), "password");
                targetHost = cert.GetNameInfo(X509NameType.DnsName, false);
            }
            try
            {
                var bootstrap = new Bootstrap();
                bootstrap
                    .Group(group)
                    .Channel<TcpSocketChannel>()
                    .Option(ChannelOption.TcpNodelay, true)
                    .Handler(new ActionChannelInitializer<ISocketChannel>(channel =>
                    {
                        IChannelPipeline pipeline = channel.Pipeline;

                        if (cert != null)
                        {
                            pipeline.AddLast("tls", new TlsHandler(stream => new SslStream(stream, true, (sender, certificate, chain, errors) => true), new ClientTlsSettings(targetHost)));
                        }
                        pipeline.AddLast("framing-enc", new LengthFieldPrepender(4));
                        pipeline.AddLast("framing-dec", new LengthFieldBasedFrameDecoder(Int32.MaxValue, 0, 4, 0, 4));
                        mChannelHandler = new ClientChannelHandler(this);
                        mChannelHandler.DataArrivedEvent += ProcessData;
                        pipeline.AddLast(mChannelHandler);
                    }));

                clientChannel = await bootstrap.ConnectAsync(new IPEndPoint(IPAddress.Parse(ip), port));
            }
            catch
            {
                Close();
                NeedReConnected = true;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        protected virtual void ProcessData(byte fun, IByteBuffer datas)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <returns></returns>
        public IByteBuffer GetBuffer(byte fun,int size)
        {
            return BufferManager.Manager.Allocate(fun, size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public bool Send(byte fun,byte[] values,int len)
        {
            mChannelHandler.Write(values,0,len, fun);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool Send(IByteBuffer values)
        {
            mChannelHandler.Write(values);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public bool Send(byte fun, byte[] values,int offset, int len)
        {
            mChannelHandler.Write(values, offset, len, fun);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public bool Send(byte fun, IntPtr values,int len)
        {
            mChannelHandler.Write(values,len, fun);
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        public async void Close()
        {
            try
            {
                if (clientChannel != null)
                    await clientChannel.CloseAsync();
                await group.ShutdownGracefullyAsync(TimeSpan.FromMilliseconds(100), TimeSpan.FromSeconds(1));
            }
            catch
            {

            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
