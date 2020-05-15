using System;
using System.Net;
using System.Collections;
using System.Collections.Generic;
using System.Threading;
using System.Runtime.InteropServices;
using System.Runtime;
using DotNetty.Transport.Channels;
using DotNetty.Transport.Libuv;
using DotNetty.Transport.Bootstrapping;
using DotNetty.Transport.Channels.Sockets;
using DotNetty.Handlers.Tls;
using System.Security.Cryptography.X509Certificates;
using DotNetty.Codecs;
using DotNetty.Buffers;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class SocketServer
    {

        #region ... Variables  ...
        private string mIp;

        private Dictionary<string, IChannelHandlerContext> mClients = new Dictionary<string, IChannelHandlerContext>();

        private Dictionary<byte, FunCallBack> mFuns = new Dictionary<byte, FunCallBack>();

        private IEventLoopGroup bossGroup;

        private IEventLoopGroup workGroup;

        private ServerChannelHandler mProcessHandle;

        private IChannel boundChannel;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public delegate IByteBuffer FunCallBack(string clientId,IByteBuffer memory);

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 是否使用异步通信库
        /// </summary>
        public bool UseLibuv { get; set; } = false;

        /// <summary>
        /// 是否使用Certificate
        /// </summary>
        private bool IsSsl { get; set; } = false;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <returns></returns>
        public IByteBuffer GetBuffer(byte fun, int size)
        {
            return BufferManager.Manager.Allocate(fun, size);
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public  void Start(string ip,int port)
        {
            mIp = ip;
            StartInner(port);
        }

        /// <summary>
        /// 
        /// </summary>
        public  void Start(int port)
        {
          
            StartInner(port);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="port"></param>
        protected virtual async void StartInner(int port)
        {
            if (!RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                GCSettings.LatencyMode = GCLatencyMode.SustainedLowLatency;
            }
           

            if (UseLibuv)
            {
                var dispatcher = new DispatcherEventLoopGroup();
                bossGroup = dispatcher;
                workGroup = new WorkerEventLoopGroup(dispatcher);
            }
            else
            {
                bossGroup = new MultithreadEventLoopGroup(1);
                workGroup = new MultithreadEventLoopGroup();
            }

            X509Certificate2 tlsCertificate = null;
            if (IsSsl)
            {
               // tlsCertificate = new X509Certificate2(Path.Combine(ExampleHelper.ProcessDirectory, "dotnetty.com.pfx"), "password");
            }

            var bootstrap = new ServerBootstrap();
            bootstrap.Group(bossGroup, workGroup);

            if (UseLibuv)
            {
                bootstrap.Channel<TcpServerChannel>();
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)
                    || RuntimeInformation.IsOSPlatform(OSPlatform.OSX))
                {
                    bootstrap
                        .Option(ChannelOption.SoReuseport, true)
                        .ChildOption(ChannelOption.SoReuseaddr, true);
                }
            }
            else
            {
                bootstrap.Channel<TcpServerSocketChannel>();
            }

            bootstrap
                    .Option(ChannelOption.SoBacklog, 8192)
                    .ChildHandler(new ActionChannelInitializer<IChannel>(channel =>
                    {
                        IChannelPipeline pipeline = channel.Pipeline;
                        if (tlsCertificate != null)
                        {
                            pipeline.AddLast(TlsHandler.Server(tlsCertificate));
                        }
                        pipeline.AddLast("framing-enc", new LengthFieldPrepender(2));
                        pipeline.AddLast("framing-dec", new LengthFieldBasedFrameDecoder(ushort.MaxValue, 0, 2, 0, 2));
                        mProcessHandle = new ServerChannelHandler(this);
                        mProcessHandle.DataArrived = new ServerChannelHandler.DataArrivedDelegate((context,fun, data) => { return ExecuteCallBack(context,fun,data); });
                        pipeline.AddLast(mProcessHandle);
                    }));

            if (string.IsNullOrEmpty(mIp))
            {
                boundChannel = await bootstrap.BindAsync(port);
            }
            else
            {
                boundChannel = await bootstrap.BindAsync(IPAddress.Parse(mIp),port);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual async void Stop()
        {
            await boundChannel.CloseAsync();
            workGroup.ShutdownGracefullyAsync().Wait();
            bossGroup.ShutdownGracefullyAsync().Wait();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        /// <returns></returns>
        private IByteBuffer ExecuteCallBack(IChannelHandlerContext context,byte fun,IByteBuffer datas)
        {
            if(mFuns.ContainsKey(fun))
            {
                return mFuns[fun](GetClientId(context), datas);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="callback"></param>
        public void RegistorFunCallBack(byte fun, FunCallBack callback)
        {
            if(!mFuns.ContainsKey(fun))
            {
                mFuns.Add(fun, callback);
            }
            else
            {
                mFuns[fun] = callback;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="values"></param>
        public void SendData(string id,byte fun,byte[] values,int len)
        {
            if(mClients.ContainsKey(id))
            {
                mProcessHandle.Write(mClients[id], values,len,fun);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="data"></param>
        public void SendData(string id, IByteBuffer data)
        {
            if (mClients.ContainsKey(id))
            {
                mProcessHandle.Write(mClients[id], data);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="len"></param>
        public void SendData(string id, byte fun, IntPtr values,int len)
        {
            if (mClients.ContainsKey(id))
            {
                mProcessHandle.Write(mClients[id], values,len, fun);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        public void SendData(byte fun, IntPtr values, int len)
        {
            foreach(var vv in mClients)
            {
                mProcessHandle.Write(vv.Value, values,len, fun);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="channel"></param>
        /// <returns></returns>
        private string GetClientId(IChannelHandlerContext channel)
        {
            string sname = channel.Name;
            if (channel.Channel is TcpSocketChannel)
            {
                sname = (channel.Channel as TcpSocketChannel).RemoteAddress.ToString();
            }
            else if (channel.Channel is SocketDatagramChannel)
            {
                sname = (channel.Channel as SocketDatagramChannel).RemoteAddress.ToString();
            }
            else if (channel.Channel is TcpServerSocketChannel)
            {
                sname = (channel.Channel as TcpServerSocketChannel).RemoteAddress.ToString();
            }
            return sname;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        protected virtual void OnClientConnected(string id)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        protected virtual void OnClientDisConnected(string id)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="channel"></param>
        internal void Registor(IChannelHandlerContext channel)
        {
            string sname = GetClientId(channel);
            

            if (!mClients.ContainsKey(sname))
            {
                mClients[sname] = channel;
            }
            else
            {
                mClients.Add(sname, channel);
            }
            OnClientConnected(sname);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="channel"></param>
        internal void UnRegistor(IChannelHandlerContext channel)
        {
            string sname = GetClientId(channel);
            if (mClients.ContainsKey(sname))
            {
                mClients.Remove(sname);
            }
            OnClientDisConnected(sname);
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
