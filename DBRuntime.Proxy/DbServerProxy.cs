using Cdy.Tag;
using DBRunTime.ServiceApi;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DBRuntime.Proxy
{
    public class DbServerProxy:INotifyPropertyChanged
    {

        #region ... Variables  ...

        public static DbServerProxy Proxy = new DbServerProxy();

        ApiClient dbClient;

        private bool mIsConnected;

        private ManualResetEvent resetEvent;

        private Thread mScanThread;

        private string mIp;
        private int mPort = 0;

        public event PropertyChangedEventHandler PropertyChanged;

        private bool mIsClosed = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public DbServerProxy()
        {
            Init();
            resetEvent = new ManualResetEvent(false);
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string UserName { get; set; } = "Admin";

        /// <summary>
        /// 
        /// </summary>
        public string Password { get; set; } = "Admin";

        /// <summary>
        /// 
        /// </summary>
        public bool IsConnected
        {
            get
            {
                return mIsConnected;
            }
            set
            {
                mIsConnected = value;
                OnPropertyChanged("IsConnected");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ApiClient NetworkClient
        {
            get
            {
                return dbClient;
            }
        }


        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void ConnectProcess()
        {
            Thread.Sleep(1000);
            if(dbClient.IsConnected)
            {
               IsConnected = dbClient.Login(UserName, Password);
            }
            else
            {
                dbClient.Connect(mIp, mPort);
            }
            resetEvent.Set();
            while (!mIsClosed)
            {
                resetEvent.WaitOne();
                if (mIsClosed) break;
                if (!mIsConnected)
                {
                    if (dbClient.IsConnected)
                    {
                        IsConnected = dbClient.Login(UserName, Password);
                    }
                    else if(dbClient.NeedReConnected)
                    {
                        dbClient.Connect(mIp, mPort);
                    }
                    Thread.Sleep(1000);
                }
                else
                {
                    resetEvent.Reset();
                }
              
            }
        }

        /// <summary>
        /// 
        /// </summary>
        private void Init()
        {
            dbClient = new ApiClient();
            dbClient.PropertyChanged += DbClient_PropertyChanged;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void DbClient_PropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            if(e.PropertyName== "IsConnected")
            {
                if (!dbClient.IsConnected)
                    resetEvent.Set();
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="port"></param>
        public void Connect(string ip,int port)
        {
            mIp = ip;
            mPort = port;
            dbClient.Connect(ip, port);
            mScanThread = new Thread(ConnectProcess);
            mScanThread.IsBackground = true;
            mScanThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            mIsClosed = true;
            resetEvent.Set();
            dbClient.PropertyChanged -= DbClient_PropertyChanged;
            dbClient.Close();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetRunnerDatabase()
        {
            return dbClient.GetRunnerDatabase(dbClient.LoginId);
        }

        private unsafe HisQueryResult<T> ProcessHisResult<T>(IByteBuffer data)
        {
            int size = data.Capacity - data.ReaderIndex;
            HisQueryResult<T> re = new HisQueryResult<T>(size);
            Buffer.MemoryCopy((void*)(data.AddressOfPinnedMemory() + data.ReaderIndex), (void*)re.Address, size, size);
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <returns></returns>
        public HisQueryResult<T> QueryAllHisData<T>(int id,DateTime stime,DateTime etime)
        {
            if(IsConnected)
            {
                var res = dbClient.QueryAllHisValue(id, stime, etime);
                TagType tp = (TagType) res.ReadByte();
                switch (tp)
                {
                    case Cdy.Tag.TagType.Bool:
                        return ProcessHisResult<bool>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Byte:
                        return ProcessHisResult<byte>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.DateTime:
                        return ProcessHisResult<DateTime>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Double:
                        return ProcessHisResult<double>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Float:
                        return ProcessHisResult<float>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Int:
                        return ProcessHisResult<int>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Long:
                        return ProcessHisResult<long>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Short:
                        return ProcessHisResult<short>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.String:
                        return ProcessHisResult<string>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UInt:
                        return ProcessHisResult<uint>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULong:
                        return ProcessHisResult<ulong>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UShort:
                        return ProcessHisResult<ushort>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint:
                        return ProcessHisResult<int>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint:
                        return ProcessHisResult<uint>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint3:
                        return ProcessHisResult<IntPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint3:
                        return ProcessHisResult<UIntPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint:
                        return ProcessHisResult<LongPointData>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint:
                        return ProcessHisResult<ULongPointTag>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint3:
                        return ProcessHisResult<LongPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint3:
                        return ProcessHisResult<ULongPoint3Data>(res) as HisQueryResult<T>;
                }

            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <param name="span"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisData<T>(int id, DateTime stime, DateTime etime,TimeSpan span,Cdy.Tag.QueryValueMatchType type)
        {
            if (IsConnected)
            {
                var res = dbClient.QueryHisValueForTimeSpan(id, stime, etime,span,type);
                TagType tp = (TagType)res.ReadByte();
                switch (tp)
                {
                    case Cdy.Tag.TagType.Bool:
                        return ProcessHisResult<bool>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Byte:
                        return ProcessHisResult<byte>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.DateTime:
                        return ProcessHisResult<DateTime>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Double:
                        return ProcessHisResult<double>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Float:
                        return ProcessHisResult<float>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Int:
                        return ProcessHisResult<int>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Long:
                        return ProcessHisResult<long>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Short:
                        return ProcessHisResult<short>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.String:
                        return ProcessHisResult<string>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UInt:
                        return ProcessHisResult<uint>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULong:
                        return ProcessHisResult<ulong>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UShort:
                        return ProcessHisResult<ushort>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint:
                        return ProcessHisResult<int>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint:
                        return ProcessHisResult<uint>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint3:
                        return ProcessHisResult<IntPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint3:
                        return ProcessHisResult<UIntPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint:
                        return ProcessHisResult<LongPointData>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint:
                        return ProcessHisResult<ULongPointTag>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint3:
                        return ProcessHisResult<LongPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint3:
                        return ProcessHisResult<ULongPoint3Data>(res) as HisQueryResult<T>;
                }

            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public HisQueryResult<T> QueryHisData<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType type)
        {
            if (IsConnected)
            {
                var res = dbClient.QueryHisValueAtTimes(id, times, type);
                TagType tp = (TagType)res.ReadByte();
                switch (tp)
                {
                    case Cdy.Tag.TagType.Bool:
                        return ProcessHisResult<bool>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Byte:
                        return ProcessHisResult<byte>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.DateTime:
                        return ProcessHisResult<DateTime>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Double:
                        return ProcessHisResult<double>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Float:
                        return ProcessHisResult<float>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Int:
                        return ProcessHisResult<int>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Long:
                        return ProcessHisResult<long>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Short:
                        return ProcessHisResult<short>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.String:
                        return ProcessHisResult<string>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UInt:
                        return ProcessHisResult<uint>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULong:
                        return ProcessHisResult<ulong>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UShort:
                        return ProcessHisResult<ushort>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint:
                        return ProcessHisResult<int>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint:
                        return ProcessHisResult<uint>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint3:
                        return ProcessHisResult<IntPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint3:
                        return ProcessHisResult<UIntPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint:
                        return ProcessHisResult<LongPointData>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint:
                        return ProcessHisResult<ULongPointTag>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint3:
                        return ProcessHisResult<LongPoint3Data>(res) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint3:
                        return ProcessHisResult<ULongPoint3Data>(res) as HisQueryResult<T>;
                }

            }
            return null;
        }

        #endregion ...Methods...

        #region ... Interfaces ...
        protected void OnPropertyChanged(string name)
        {
            if(PropertyChanged!=null)
            {
                PropertyChanged(this, new PropertyChangedEventArgs(name) );
            }
        }
        #endregion ...Interfaces...
    }
}
