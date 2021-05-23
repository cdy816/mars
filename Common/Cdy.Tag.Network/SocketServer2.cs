using Cheetah;
using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public abstract class SocketServer2:Cheetah.TcpSocket.TcpSocketServer
    {

        #region ... Variables  ...
        bool isRunning = false;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="memory"></param>
        /// <returns></returns>
        public delegate Cheetah.ByteBuffer FunCallBack(string clientId, Cheetah.ByteBuffer memory);


        private Dictionary<byte, FunCallBack> mFuns = new Dictionary<byte, FunCallBack>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public abstract string Name { get; }

        #endregion ...Properties...

        #region ... Methods    ...


        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public Cheetah.ByteBuffer Allocate(int size)
        {

            return Alloc(size);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="preValue"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        public Cheetah.ByteBuffer Allocate(byte preValue, int size)
        {
            return Allocate(size + 1).Write(preValue);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <returns></returns>
        public Cheetah.ByteBuffer GetBuffer(byte fun, int size)
        {
            return Allocate(fun, size);
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public void Start(string ip, int port)
        {
            if (!isRunning)
            {
                isRunning = true;

                try
                {
                    Open(ip,port);
                    LoggerService.Service.Info("SocketServer", "Server " + this.Name + " start  at port " + port, ConsoleColor.Cyan);
                }
                catch (System.Net.Sockets.SocketException se)
                {
                    if (se.SocketErrorCode == System.Net.Sockets.SocketError.AccessDenied)
                    {
                        LoggerService.Service.Erro("SocketServer", "启动服务失败,端口 " + port + " 被占用");
                    }
                    else
                    {
                        LoggerService.Service.Erro("SocketServer", "在端口 " + port + " 启动服务失败, " + se.Message);
                    }
                }
                catch (Exception ex)
                {
                    LoggerService.Service.Erro("SocketServer", "在端口 " + port + " 启动服务失败, " + ex.Message + ex.StackTrace);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Start(int port)
        {
            if (!isRunning)
            {
                isRunning = true;

                try
                {
                    Open(port);
                    LoggerService.Service.Info("SocketServer", "Server " + this.Name + " start  at port " + port, ConsoleColor.Cyan);
                }
                catch (System.Net.Sockets.SocketException se)
                {
                    if (se.SocketErrorCode == System.Net.Sockets.SocketError.AccessDenied)
                    {
                        LoggerService.Service.Erro("SocketServer", "启动服务失败,端口 " + port + " 被占用");
                    }
                    else
                    {
                        LoggerService.Service.Erro("SocketServer", "在端口 " + port + " 启动服务失败, " + se.Message);
                    }
                }
                catch (Exception ex)
                {
                    LoggerService.Service.Erro("SocketServer", "在端口 " + port + " 启动服务失败, " + ex.Message + ex.StackTrace);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public virtual void Stop()
        {
            if (isRunning)
            {
                isRunning = false;
                Close();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="callback"></param>
        public void RegistorFunCallBack(byte fun, FunCallBack callback)
        {
            if (!mFuns.ContainsKey(fun))
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
        /// <param name="data"></param>
        /// <returns></returns>
        public override ByteBuffer OnReceiveData(string id, ByteBuffer data)
        {
            if (data != null && data.WriteIndex > data.ReadIndex)
            {
                byte fun = data.ReadByte();

                if (mFuns.ContainsKey(fun))
                {
                    return mFuns[fun](id, data);
                }
                else
                {
                    LoggerService.Service.Warn("socket server:", "invailed data:" + fun.ToString());
                }
            }
            return base.OnReceiveData(id, data);
        }
    

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="len"></param>
        public void SendData(string id, byte fun, IntPtr values, int len)
        {
            SendDataToClient(id, GetBuffer(fun, len).Write(values, len));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="len"></param>
        public void SendData(string id, byte fun, byte[] values, int len)
        {
            SendDataToClient(id, GetBuffer(fun, len).Write(values,0, len));
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        public void SendData(string id, byte fun, byte[] values)
        {
            SendDataToClient(id, GetBuffer(fun, values.Length).Write(values, 0, values.Length));
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="offset"></param>
        /// <param name="len"></param>
        public void SendData(string id, byte fun, byte[] values,int offset,int len)
        {
            SendDataToClient(id, GetBuffer(fun, len).Write(values, offset, len));
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
