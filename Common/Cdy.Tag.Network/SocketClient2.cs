using Cheetah;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class SocketClient2:Cheetah.TcpSocket.TcpSocketClient,INotifyPropertyChanged
    {
        public event PropertyChangedEventHandler PropertyChanged;

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        public override void OnConnected(bool isConnected)
        {
            base.OnConnected(isConnected);
            OnPropertyChanged("IsConnected");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="size"></param>
        /// <returns></returns>
        public Cheetah.ByteBuffer Allocate(int size)
        {
            return this.MemoryPool.Alloc(size);
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
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public bool Send(byte fun, byte[] values, int len)
        {
            this.SendData(GetBuffer(fun, len).Write(values, 0, len));
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <returns></returns>
        public bool Send(byte fun, byte[] values)
        {
            return Send(fun, values, values.Length);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="values"></param>
        /// <param name="len"></param>
        /// <returns></returns>
        public unsafe bool Send(byte fun,IntPtr values,int len)
        {
            this.SendData(GetBuffer(fun, len).Write(values, len));
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        public override void OnReceiveData(ByteBuffer data)
        {
            if(data.WriteIndex>data.ReadIndex)
            {
                byte fun = data.ReadByte();
                ProcessData(fun, data);
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        protected virtual void ProcessData(byte fun, Cheetah.ByteBuffer datas)
        {

        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        protected void OnPropertyChanged(string name)
        {
            if(PropertyChanged!=null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(name));
            }

        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
