using Cdy.Tag;
using DBRunTime.ServiceApi;
using DotNetty.Buffers;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;

namespace DBRuntime.Proxy
{
    public class DbServerProxy:INotifyPropertyChanged
    {

        #region ... Variables  ...

        public static DbServerProxy Proxy = new DbServerProxy();

        ApiClient dbClient;

        ApiClient mHisClient;

        private bool mIsConnected=false;

        private ManualResetEvent resetEvent;

        private Thread mScanThread;

        private string mIp;

        private int mPort = 0;

        public event PropertyChangedEventHandler PropertyChanged;

        //public event EventHandler ValueUpdateEvent;

        private bool mIsClosed = false;

        ApiClient mUsedHisClient;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public DbServerProxy()
        {
           
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
                if (mIsConnected != value)
                {
                    mIsConnected = value;
                    OnPropertyChanged("IsConnected");
                }
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

        public bool IsUseStandardHisDataServer { get; set; } = false;

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
            if (IsUseStandardHisDataServer)
            {
                if (!mHisClient.IsConnected)
                {
                    mHisClient.Connect(mIp, mPort + 1);
                }
                else
                {
                    mHisClient.Login(UserName, Password);
                }
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

                    if (IsUseStandardHisDataServer)
                    {
                        if (mHisClient.IsConnected)
                        {
                            mHisClient.Login(UserName, Password);
                        }
                        else if (mHisClient.NeedReConnected)
                        {
                            mHisClient.Connect(mIp, mPort + 1);
                        }
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

            if (IsUseStandardHisDataServer)
            {
                mHisClient = new ApiClient();
                mHisClient.PropertyChanged += MHisClient_PropertyChanged;
                mUsedHisClient = mHisClient;
            }
            else
            {
                mUsedHisClient = dbClient;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MHisClient_PropertyChanged(object sender, PropertyChangedEventArgs e)
        {
            if (e.PropertyName == "IsConnected")
            {
                if (!mHisClient.IsConnected)
                    resetEvent.Set();
            }
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
                {
                    IsConnected = false;
                    resetEvent.Set();
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="port"></param>
        public void Connect(string ip,int port)
        {
            Init();
            mIp = ip;
            mPort = port;
            dbClient.Connect(ip, port);
            if (IsUseStandardHisDataServer)
            {
                mHisClient.Connect(ip, port + 1);

            }
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
            if (IsUseStandardHisDataServer)
            {
                mHisClient.PropertyChanged -= MHisClient_PropertyChanged;
                mHisClient.Close();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public string GetRunnerDatabase()
        {
            return dbClient.GetRunnerDatabase();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public RealDatabase LoadRealDatabase()
        {
            string sfile = dbClient.GetRealdatabase();
            if(System.IO.File.Exists(sfile))
            {
                return new RealDatabaseSerise().Load(sfile);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public SecurityDocument LoadSecurity()
        {
            string sfile = dbClient.GetSecuritySetting();
            if (System.IO.File.Exists(sfile))
            {
                return new SecuritySerise().Load(sfile);
            }
            return null;
        }

        //private unsafe Dictionary<DateTime,Tuple<object,byte>> ProcessHisResult<T>(IByteBuffer data, TagType tp)
        //{
        //    Dictionary<DateTime, Tuple<object, byte>> re = new Dictionary<DateTime, Tuple<object, byte>>();
        //    int count = data.ReadInt();
        //    DateTime time;
        //    byte qu = 0;

        //    switch (tp)
        //    {
        //        case Cdy.Tag.TagType.Bool:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadBoolean();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.Byte:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadByte();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.DateTime:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = new DateTime(data.ReadLong());
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.Double:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadDouble();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.Float:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadFloat();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.Int:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadInt();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.Long:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadLong();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.Short:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadShort();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.String:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = data.ReadString();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.UInt:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = (uint)data.ReadUnsignedInt();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.ULong:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = (ulong)data.ReadLong();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.UShort:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var val = (ulong)data.ReadUnsignedShort();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(val, qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.IntPoint:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = data.ReadInt();
        //                var y = data.ReadInt();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new IntPointData(x,y), qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.UIntPoint:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = data.ReadUnsignedInt();
        //                var y = data.ReadUnsignedInt();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new UIntPointData(x, y), qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.IntPoint3:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = data.ReadInt();
        //                var y = data.ReadInt();
        //                var z = data.ReadInt();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new IntPoint3Data(x, y,z), qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.UIntPoint3:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = data.ReadUnsignedInt();
        //                var y = data.ReadUnsignedInt();
        //                var z = data.ReadUnsignedInt();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new UIntPoint3Data(x, y,z), qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.LongPoint:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = data.ReadLong();
        //                var y = data.ReadLong();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new LongPointData(x, y), qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.ULongPoint:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = (ulong)data.ReadLong();
        //                var y = (ulong)data.ReadLong();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new ULongPointData(x, y), qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.LongPoint3:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = data.ReadLong();
        //                var y = data.ReadLong();
        //                var z = data.ReadLong();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new LongPoint3Data(x, y,z), qu));
        //            }
        //            break;
        //        case Cdy.Tag.TagType.ULongPoint3:
        //            for (int i = 0; i < count; i++)
        //            {
        //                var x = (ulong)data.ReadLong();
        //                var y = (ulong)data.ReadLong();
        //                var z = (ulong)data.ReadLong();
        //                time = new DateTime(data.ReadLong());
        //                qu = data.ReadByte();
        //                re.Add(time, new Tuple<object, byte>(new ULongPoint3Data(x, y, z), qu));
        //            }
        //            break;
        //    }

            

        //    return re;
        //}

        private unsafe HisQueryResult<T> ProcessHisResultByMemory<T>(IByteBuffer data, TagType tp)
        {
           
            int count = data.ReadInt();
            HisQueryResult<T> re = new HisQueryResult<T>(count);
            Marshal.Copy(data.Array, data.ArrayOffset + data.ReaderIndex, re.Address, data.ReadableBytes);
            re.Count = count;
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <returns></returns>
        public IByteBuffer QueryAllHisValue(int id, DateTime stime, DateTime etime)
        {
            if (IsConnected)
            {
               return this.mUsedHisClient.QueryAllHisValue(id, stime, etime);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="stime"></param>
        /// <param name="etime"></param>
        /// <param name="span"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public IByteBuffer QueryHisData(int id, DateTime stime, DateTime etime, TimeSpan span, Cdy.Tag.QueryValueMatchType type)
        {
            if (IsConnected)
            {
                return mUsedHisClient.QueryHisValueForTimeSpan(id, stime, etime, span, type);
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public IByteBuffer QueryHisData(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType type)
        {
            if (IsConnected)
            {
                return mUsedHisClient.QueryHisValueAtTimes(id, times, type);
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
        /// <returns></returns>
        public HisQueryResult<T> QueryAllHisData<T>(int id, DateTime stime, DateTime etime)
        {
            if (IsConnected)
            {
                var res = this.mUsedHisClient.QueryAllHisValue(id, stime, etime);
                if (res == null || res.ReferenceCount == 0) return null;
                TagType tp = (TagType)res.ReadByte();
                switch (tp)
                {
                    case Cdy.Tag.TagType.Bool:
                        return ProcessHisResultByMemory<bool>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Byte:
                        return ProcessHisResultByMemory<byte>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.DateTime:
                        return ProcessHisResultByMemory<DateTime>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Double:
                        return ProcessHisResultByMemory<double>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Float:
                        return ProcessHisResultByMemory<float>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Int:
                        return ProcessHisResultByMemory<int>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Long:
                        return ProcessHisResultByMemory<long>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Short:
                        return ProcessHisResultByMemory<short>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.String:
                        return ProcessHisResultByMemory<string>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UInt:
                        return ProcessHisResultByMemory<uint>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULong:
                        return ProcessHisResultByMemory<ulong>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UShort:
                        return ProcessHisResultByMemory<ushort>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint:
                        return ProcessHisResultByMemory<int>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint:
                        return ProcessHisResultByMemory<uint>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint3:
                        return ProcessHisResultByMemory<IntPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint3:
                        return ProcessHisResultByMemory<UIntPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint:
                        return ProcessHisResultByMemory<LongPointData>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint:
                        return ProcessHisResultByMemory<ULongPointTag>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint3:
                        return ProcessHisResultByMemory<LongPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint3:
                        return ProcessHisResultByMemory<ULongPoint3Data>(res, tp) as HisQueryResult<T>;
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
        public HisQueryResult<T> QueryHisData<T>(int id, DateTime stime, DateTime etime, TimeSpan span, Cdy.Tag.QueryValueMatchType type)
        {
            if (IsConnected)
            {
                var res = mUsedHisClient.QueryHisValueForTimeSpan(id, stime, etime, span, type);
                if (res == null) return null;
                TagType tp = (TagType)res.ReadByte();
                switch (tp)
                {
                    case Cdy.Tag.TagType.Bool:
                        return ProcessHisResultByMemory<bool>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Byte:
                        return ProcessHisResultByMemory<byte>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.DateTime:
                        return ProcessHisResultByMemory<DateTime>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Double:
                        return ProcessHisResultByMemory<double>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Float:
                        return ProcessHisResultByMemory<float>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Int:
                        return ProcessHisResultByMemory<int>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Long:
                        return ProcessHisResultByMemory<long>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Short:
                        return ProcessHisResultByMemory<short>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.String:
                        return ProcessHisResultByMemory<string>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UInt:
                        return ProcessHisResultByMemory<uint>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULong:
                        return ProcessHisResultByMemory<ulong>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UShort:
                        return ProcessHisResultByMemory<ushort>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint:
                        return ProcessHisResultByMemory<int>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint:
                        return ProcessHisResultByMemory<uint>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint3:
                        return ProcessHisResultByMemory<IntPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint3:
                        return ProcessHisResultByMemory<UIntPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint:
                        return ProcessHisResultByMemory<LongPointData>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint:
                        return ProcessHisResultByMemory<ULongPointTag>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint3:
                        return ProcessHisResultByMemory<LongPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint3:
                        return ProcessHisResultByMemory<ULongPoint3Data>(res, tp) as HisQueryResult<T>;
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
                var res = mUsedHisClient.QueryHisValueAtTimes(id, times, type);
                if (res == null) return null;
                TagType tp = (TagType)res.ReadByte();
                switch (tp)
                {
                    case Cdy.Tag.TagType.Bool:
                        return ProcessHisResultByMemory<bool>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Byte:
                        return ProcessHisResultByMemory<byte>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.DateTime:
                        return ProcessHisResultByMemory<DateTime>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Double:
                        return ProcessHisResultByMemory<double>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Float:
                        return ProcessHisResultByMemory<float>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Int:
                        return ProcessHisResultByMemory<int>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Long:
                        return ProcessHisResultByMemory<long>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.Short:
                        return ProcessHisResultByMemory<short>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.String:
                        return ProcessHisResultByMemory<string>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UInt:
                        return ProcessHisResultByMemory<uint>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULong:
                        return ProcessHisResultByMemory<ulong>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UShort:
                        return ProcessHisResultByMemory<ushort>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint:
                        return ProcessHisResultByMemory<int>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint:
                        return ProcessHisResultByMemory<uint>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.IntPoint3:
                        return ProcessHisResultByMemory<IntPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.UIntPoint3:
                        return ProcessHisResultByMemory<UIntPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint:
                        return ProcessHisResultByMemory<LongPointData>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint:
                        return ProcessHisResultByMemory<ULongPointTag>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.LongPoint3:
                        return ProcessHisResultByMemory<LongPoint3Data>(res, tp) as HisQueryResult<T>;
                    case Cdy.Tag.TagType.ULongPoint3:
                        return ProcessHisResultByMemory<ULongPoint3Data>(res, tp) as HisQueryResult<T>;
                }

            }
            return null;
        }

        #region obslute QueryHisValue
        ///// <summary>
        ///// 
        ///// </summary>
        ///// <param name="id"></param>
        ///// <param name="stime"></param>
        ///// <param name="etime"></param>
        ///// <returns></returns>
        //public Dictionary<DateTime, Tuple<object, byte>> QueryAllHisData<T>(int id,DateTime stime,DateTime etime)
        //{
        //    if(IsConnected)
        //    {
        //        var res = dbClient.QueryAllHisValue(id, stime, etime);
        //        if (res == null || res.ReadableBytes==0) return null;
        //        TagType tp = (TagType) res.ReadByte();
        //        switch (tp)
        //        {
        //            case Cdy.Tag.TagType.Bool:
        //                return ProcessHisResult<bool>(res,tp);
        //            case Cdy.Tag.TagType.Byte:
        //                return ProcessHisResult<byte>(res,tp);
        //            case Cdy.Tag.TagType.DateTime:
        //                return ProcessHisResult<DateTime>(res,tp);
        //            case Cdy.Tag.TagType.Double:
        //                return ProcessHisResult<double>(res,tp);
        //            case Cdy.Tag.TagType.Float:
        //                return ProcessHisResult<float>(res,tp);
        //            case Cdy.Tag.TagType.Int:
        //                return ProcessHisResult<int>(res,tp);
        //            case Cdy.Tag.TagType.Long:
        //                return ProcessHisResult<long>(res,tp);
        //            case Cdy.Tag.TagType.Short:
        //                return ProcessHisResult<short>(res,tp);
        //            case Cdy.Tag.TagType.String:
        //                return ProcessHisResult<string>(res,tp);
        //            case Cdy.Tag.TagType.UInt:
        //                return ProcessHisResult<uint>(res,tp);
        //            case Cdy.Tag.TagType.ULong:
        //                return ProcessHisResult<ulong>(res,tp);
        //            case Cdy.Tag.TagType.UShort:
        //                return ProcessHisResult<ushort>(res,tp);
        //            case Cdy.Tag.TagType.IntPoint:
        //                return ProcessHisResult<int>(res,tp);
        //            case Cdy.Tag.TagType.UIntPoint:
        //                return ProcessHisResult<uint>(res,tp);
        //            case Cdy.Tag.TagType.IntPoint3:
        //                return ProcessHisResult<IntPoint3Data>(res,tp);
        //            case Cdy.Tag.TagType.UIntPoint3:
        //                return ProcessHisResult<UIntPoint3Data>(res,tp);
        //            case Cdy.Tag.TagType.LongPoint:
        //                return ProcessHisResult<LongPointData>(res,tp);
        //            case Cdy.Tag.TagType.ULongPoint:
        //                return ProcessHisResult<ULongPointTag>(res,tp);
        //            case Cdy.Tag.TagType.LongPoint3:
        //                return ProcessHisResult<LongPoint3Data>(res,tp);
        //            case Cdy.Tag.TagType.ULongPoint3:
        //                return ProcessHisResult<ULongPoint3Data>(res,tp);
        //        }

        //    }
        //    return null;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="stime"></param>
        ///// <param name="etime"></param>
        ///// <param name="span"></param>
        ///// <param name="type"></param>
        ///// <returns></returns>
        //public Dictionary<DateTime, Tuple<object, byte>> QueryHisData<T>(int id, DateTime stime, DateTime etime,TimeSpan span,Cdy.Tag.QueryValueMatchType type)
        //{
        //    if (IsConnected)
        //    {
        //        var res = dbClient.QueryHisValueForTimeSpan(id, stime, etime,span,type);
        //        if (res == null) return null;
        //        TagType tp = (TagType)res.ReadByte();
        //        switch (tp)
        //        {
        //            case Cdy.Tag.TagType.Bool:
        //                return ProcessHisResult<bool>(res,tp) ;
        //            case Cdy.Tag.TagType.Byte:
        //                return ProcessHisResult<byte>(res,tp) ;
        //            case Cdy.Tag.TagType.DateTime:
        //                return ProcessHisResult<DateTime>(res,tp) ;
        //            case Cdy.Tag.TagType.Double:
        //                return ProcessHisResult<double>(res,tp) ;
        //            case Cdy.Tag.TagType.Float:
        //                return ProcessHisResult<float>(res,tp) ;
        //            case Cdy.Tag.TagType.Int:
        //                return ProcessHisResult<int>(res,tp) ;
        //            case Cdy.Tag.TagType.Long:
        //                return ProcessHisResult<long>(res,tp) ;
        //            case Cdy.Tag.TagType.Short:
        //                return ProcessHisResult<short>(res,tp) ;
        //            case Cdy.Tag.TagType.String:
        //                return ProcessHisResult<string>(res,tp) ;
        //            case Cdy.Tag.TagType.UInt:
        //                return ProcessHisResult<uint>(res,tp) ;
        //            case Cdy.Tag.TagType.ULong:
        //                return ProcessHisResult<ulong>(res,tp) ;
        //            case Cdy.Tag.TagType.UShort:
        //                return ProcessHisResult<ushort>(res,tp) ;
        //            case Cdy.Tag.TagType.IntPoint:
        //                return ProcessHisResult<int>(res,tp) ;
        //            case Cdy.Tag.TagType.UIntPoint:
        //                return ProcessHisResult<uint>(res,tp) ;
        //            case Cdy.Tag.TagType.IntPoint3:
        //                return ProcessHisResult<IntPoint3Data>(res,tp) ;
        //            case Cdy.Tag.TagType.UIntPoint3:
        //                return ProcessHisResult<UIntPoint3Data>(res,tp) ;
        //            case Cdy.Tag.TagType.LongPoint:
        //                return ProcessHisResult<LongPointData>(res,tp) ;
        //            case Cdy.Tag.TagType.ULongPoint:
        //                return ProcessHisResult<ULongPointTag>(res,tp) ;
        //            case Cdy.Tag.TagType.LongPoint3:
        //                return ProcessHisResult<LongPoint3Data>(res,tp) ;
        //            case Cdy.Tag.TagType.ULongPoint3:
        //                return ProcessHisResult<ULongPoint3Data>(res,tp) ;
        //        }

        //    }
        //    return null;
        //}

        ///// <summary>
        ///// 
        ///// </summary>
        ///// <typeparam name="T"></typeparam>
        ///// <param name="id"></param>
        ///// <param name="times"></param>
        ///// <param name="type"></param>
        ///// <returns></returns>
        //public Dictionary<DateTime, Tuple<object, byte>> QueryHisData<T>(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType type)
        //{
        //    if (IsConnected)
        //    {
        //        var res = dbClient.QueryHisValueAtTimes(id, times, type);
        //        if (res == null) return null;
        //        TagType tp = (TagType)res.ReadByte();
        //        switch (tp)
        //        {
        //            case Cdy.Tag.TagType.Bool:
        //                return ProcessHisResult<bool>(res,tp) ;
        //            case Cdy.Tag.TagType.Byte:
        //                return ProcessHisResult<byte>(res,tp) ;
        //            case Cdy.Tag.TagType.DateTime:
        //                return ProcessHisResult<DateTime>(res,tp) ;
        //            case Cdy.Tag.TagType.Double:
        //                return ProcessHisResult<double>(res,tp) ;
        //            case Cdy.Tag.TagType.Float:
        //                return ProcessHisResult<float>(res,tp) ;
        //            case Cdy.Tag.TagType.Int:
        //                return ProcessHisResult<int>(res,tp) ;
        //            case Cdy.Tag.TagType.Long:
        //                return ProcessHisResult<long>(res,tp) ;
        //            case Cdy.Tag.TagType.Short:
        //                return ProcessHisResult<short>(res,tp) ;
        //            case Cdy.Tag.TagType.String:
        //                return ProcessHisResult<string>(res,tp) ;
        //            case Cdy.Tag.TagType.UInt:
        //                return ProcessHisResult<uint>(res,tp) ;
        //            case Cdy.Tag.TagType.ULong:
        //                return ProcessHisResult<ulong>(res,tp) ;
        //            case Cdy.Tag.TagType.UShort:
        //                return ProcessHisResult<ushort>(res,tp) ;
        //            case Cdy.Tag.TagType.IntPoint:
        //                return ProcessHisResult<int>(res,tp) ;
        //            case Cdy.Tag.TagType.UIntPoint:
        //                return ProcessHisResult<uint>(res,tp) ;
        //            case Cdy.Tag.TagType.IntPoint3:
        //                return ProcessHisResult<IntPoint3Data>(res,tp) ;
        //            case Cdy.Tag.TagType.UIntPoint3:
        //                return ProcessHisResult<UIntPoint3Data>(res,tp) ;
        //            case Cdy.Tag.TagType.LongPoint:
        //                return ProcessHisResult<LongPointData>(res,tp) ;
        //            case Cdy.Tag.TagType.ULongPoint:
        //                return ProcessHisResult<ULongPointTag>(res,tp) ;
        //            case Cdy.Tag.TagType.LongPoint3:
        //                return ProcessHisResult<LongPoint3Data>(res,tp) ;
        //            case Cdy.Tag.TagType.ULongPoint3:
        //                return ProcessHisResult<ULongPoint3Data>(res,tp) ;
        //        }

        //    }
        //    return null;
        //}
        #endregion

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
