using System;
using System.Net;
using System.Collections;
using System.Collections.Generic;
using System.Threading;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class SocketServer
    {

        #region ... Variables  ...
        private string mIp;
        private System.Net.Sockets.Socket mServerSocket;

        private Dictionary<EndPoint, ServerProcessItem> mClients = new Dictionary<EndPoint, ServerProcessItem>();

        private Thread mAcceptThread;

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
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public void Start(string ip,int port)
        {
            mIp = ip;
            StartInner(port);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start(int port)
        {
            mIp = "0.0.0.0";
            StartInner(port);
        }

        public void StartIpv6(string ip,int port)
        {
            mIp = ip;
            StartInnerV6(port);
        }

        private void StartInner(int port)
        {
            mServerSocket = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetwork, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            mServerSocket.Bind(new IPEndPoint(IPAddress.Parse(mIp), port));
            mServerSocket.Listen(1024);
            StartScan();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="port"></param>
        private void StartInnerV6(int port)
        {
            mServerSocket = new System.Net.Sockets.Socket(System.Net.Sockets.AddressFamily.InterNetworkV6, System.Net.Sockets.SocketType.Stream, System.Net.Sockets.ProtocolType.Tcp);
            mServerSocket.Bind(new IPEndPoint(IPAddress.Parse(mIp), port));
            mServerSocket.Listen(1024);
            StartScan();
        }

        /// <summary>
        /// 
        /// </summary>
        private void StartScan()
        {
            mAcceptThread = new Thread(AcceptThreadPro);
            mAcceptThread.IsBackground = true;
            mAcceptThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        private void AcceptThreadPro()
        {
            while(true)
            {
                var socket = mServerSocket.Accept();
                mClients.Add(socket.RemoteEndPoint, new ServerProcessItem(socket));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            foreach(var vv in mClients)
            {
                vv.Value.Close();
            }
            mAcceptThread.Abort();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
