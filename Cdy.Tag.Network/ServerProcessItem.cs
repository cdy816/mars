//==============================================================
//  Copyright (C) 2020  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2020/5/11 11:34:18.
//  Version 1.0
//  种道洋
//==============================================================

using System;
using System.Collections.Generic;
using System.Text;
using System.Net.Sockets;

namespace Cdy.Tag
{
    public class ServerProcessItem
    {

        #region ... Variables  ...
        private System.Net.Sockets.Socket mSocket;
        private SocketBuffer mSendBuffer;
        private SocketBuffer mReceiveBuffer;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="socket"></param>
        public ServerProcessItem(Socket socket)
        {
            mSocket = socket;

        }

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        private void Init()
        {
            mSendBuffer = new SocketBuffer(1024 * 10);
            mReceiveBuffer = new SocketBuffer(1024 * 10);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Receive()
        {
           int count = mSocket.Receive(mReceiveBuffer.Buffers, mReceiveBuffer.ReceiveAddr, mSocket.Available, SocketFlags.None);
            mReceiveBuffer.ReceiveAddr += count;
            
        }

        /// <summary>
        /// 
        /// </summary>
        public void Send()
        {

        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            mSocket.Close();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
