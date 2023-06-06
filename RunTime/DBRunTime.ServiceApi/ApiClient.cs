using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cdy.Tag;
using Cheetah;

namespace DBRunTime.ServiceApi
{
    public class ApiFunConst
    {
        /// <summary>
        /// 
        /// </summary>
        public const byte TagInfoRequest = 1;

        public const byte GetTagIdByNameFun = 0;

        public const byte Login = 1;

        public const byte RegistorValueCallback = 2;


        public const byte GetRunnerDatabase = 3;

        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataRequestFun = 10;

        public const byte RealDataSetFun = 11;

        /// <summary>
        /// 获取实时值
        /// </summary>
        public const byte RequestRealData = 0;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealData2 = 10;

        /// <summary>
        /// 请求所有数据
        /// </summary>
        public const byte RealMemorySync = 13;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealDataByMemoryCopy = 11;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestRealData2ByMemoryCopy = 12;

        /// <summary>
        /// 设置实时值
        /// </summary>
        public const byte SetDataValue = 1;

        /// <summary>
        /// 值改变通知
        /// </summary>
        public const byte ValueChangeNotify = 2;

        /// <summary>
        /// 清空值改变通知
        /// </summary>
        public const byte ResetValueChangeNotify = 3;


        /// <summary>
        /// 块改变通知
        /// </summary>
        public const byte BlockValueChangeNotify = 4;


        /// <summary>
        /// 
        /// </summary>
        public const byte RealDataPushFun = 12;


        /// <summary>
        /// 
        /// </summary>
        public const byte HisDataRequestFun = 20;


        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePoint = 0;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestAllHisData = 1;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDataByTimeSpan = 2;

        /// <summary>
        /// 读取数据的统计值
        /// </summary>
        public const byte RequestNumberStatistics = 3;

        //值统计
        public const byte RequestValueStatistics = 5;

        /// <summary>
        /// 读取某个时间点的统计值
        /// </summary>
        public const byte RequestNumberStatisticsByTimePoint = 4;


        public const byte SyncRealTagConfig = 30;

        public const byte SyncHisTagConfig = 31;

        public const byte SyncSecuritySetting = 32;

        public const byte RealTagMemoryInfo = 33;

        /// <summary>
        /// 
        /// </summary>
        public const byte TagInfoNotify = 100;

        public const byte DatabaseChangedNotify = 1;


        public const byte AysncReturn = byte.MaxValue;


        public const byte Hart = byte.MaxValue;


        /// <summary>
        /// 修改历史数据
        /// </summary>
        public const byte ModifyHisData = 6;

        /// <summary>
        /// 删除历史数据
        /// </summary>
        public const byte DeleteHisData = 7;

        public const byte SetRealDataToLastData = 120;

        public const byte RequestComplexTagRealValue = 14;


        /// <summary>
        /// 根据时间间隔查询数据的历史记录，忽略系统退出的影响
        /// </summary>
        public const byte RequestHisDataByTimeSpanByIgnorClosedQuality = 11;

        /// <summary>
        /// 
        /// </summary>
        public const byte RequestHisDatasByTimePointByIgnorClosedQuality = 10;

        /// <summary>
        /// 请求变量状态数据
        /// </summary>
        public const byte RequestStateData = 6;

        /// <summary>
        /// 请求变量扩展字段
        /// </summary>
        public const byte RequestExtendField2 = 7;

        /// <summary>
        /// 设置状态数据
        /// </summary>
        public const byte SetStateData = 8;

        /// <summary>
        /// 设置扩展数据
        /// </summary>
        public const byte SetExtendField2Data = 9;

        /// <summary>
        /// 通过SQL查询历史
        /// </summary>
        public const byte ReadHisValueBySQL = 140;
    }

    public class ApiClient : SocketClient2
    {

        #region ... Variables  ...

        private ManualResetEvent infoRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent SyncDataEvent = new ManualResetEvent(false);

        private ByteBuffer mInfoRequreData;

        private ByteBuffer mRealSyncData;

        //private ManualResetEvent hisRequreEvent = new ManualResetEvent(false);

        //private ByteBuffer mHisRequreData;

        private ManualResetEvent realRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent realSetRequreEvent = new ManualResetEvent(false);

        private ManualResetEvent realResetRequreEvent = new ManualResetEvent(false);

        private ByteBuffer mRealRequreData;

        private ByteBuffer mRealSetResponseData;

        private ByteBuffer mRealResetResponseData;

        private object mHisDataLock = new object();

        private object mRealDataLock = new object();

        private object mRealSetDataLock = new object();

        private object mRealResetDataLock = new object();

        private object mLoginLock = new object();

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<byte, ByteBuffer> mReceivedDatas = new Dictionary<byte, ByteBuffer>();

        public delegate void ProcessDataPushDelegate(ByteBuffer datas);

        private int mHisRequreCount = 0;

        private int mRealRequerCount = 0;

        private object mRealDataLocker = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public bool IsLogin { get { return LoginId > 0; } }

        /// <summary>
        /// 
        /// </summary>
        public long LoginId { get; set; }

        public ProcessDataPushDelegate ProcessDataPush { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Action<bool, bool, bool> DatabaseChangedAction { get; set; }

        private string mUser;
        private string mPass;

        public Dictionary<int, Action<ByteBuffer>> mHisDataCallBack = new Dictionary<int, Action<ByteBuffer>>();

        public Dictionary<int, Action<ByteBuffer>> mRealDataCallBack = new Dictionary<int,Action<ByteBuffer>>();

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="isConnected"></param>
        public override void OnConnected(bool isConnected)
        {
            base.OnConnected(isConnected);
            if (!isConnected)
            {
                LoginId = 0;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="fun"></param>
        /// <param name="datas"></param>
        protected override void ProcessData(byte fun, Cheetah.ByteBuffer datas)
        {
            //LoggerService.Service.Warn("ApiClient", $"收到服务器数据 {fun} 数据大小:{datas.ReadableCount}");
            if (fun == ApiFunConst.RealDataPushFun)
            {
                //Stopwatch sw = Stopwatch.StartNew();
                //sw.Start();
                ProcessDataPush?.Invoke(datas);
                //sw.Stop();
                
                //if(sw.ElapsedMilliseconds > 100)
                //{
                //    LoggerService.Service.Warn("ApiClient", $"数据推送处理耗时{sw.ElapsedMilliseconds}");
                //}
            }
            else if (fun == ApiFunConst.AysncReturn)
            {
                //收到异步请求回调数据
                datas.UnlockAndReturn();
                return;
            }
            else
            {
                //datas.IncRef();
                switch (fun)
                {
                    case ApiFunConst.TagInfoRequest:
                        mInfoRequreData?.UnlockAndReturn();
                        mInfoRequreData = datas;
                        infoRequreEvent.Set();
                        //LoggerService.Service.Info("",$" 收到Info数据ID {mInfoRequreData.Id}");
                        break;
                    case ApiFunConst.RealDataRequestFun:
                    case ApiFunConst.RequestComplexTagRealValue:
                        var rid = datas.ReadInt();
                        ProcessRealDataCallBack(rid, datas);
                        break;
                    case ApiFunConst.RealDataSetFun:
                        mRealSetResponseData?.UnlockAndReturn();
                        mRealSetResponseData = datas;
                        this.realSetRequreEvent.Set();
                        break;
                    case ApiFunConst.SetRealDataToLastData:
                        mRealResetResponseData?.UnlockAndReturn();
                        mRealResetResponseData = datas;
                        realResetRequreEvent.Set();
                        break;
                    case ApiFunConst.HisDataRequestFun:
                        //mHisRequreData?.UnlockAndReturn();
                        //mHisRequreData = datas;

                        var cid = datas.ReadInt();
                        ProcessHisDataCallBack(cid, datas);
                        //hisRequreEvent.Set();
                        break;
                    case ApiFunConst.RealTagMemoryInfo:
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = datas;
                        SyncDataEvent.Set();
                        break;
                    case ApiFunConst.SyncRealTagConfig:
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = datas;
                        SyncDataEvent.Set();
                        break;
                    case ApiFunConst.SyncSecuritySetting:
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = datas;
                        SyncDataEvent.Set();
                        break;
                    case ApiFunConst.SyncHisTagConfig:
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = datas;
                        SyncDataEvent.Set();
                        break;
                    case ApiFunConst.TagInfoNotify:
                        ProcessTagInfoNotify(datas);
                        break;
                    default:
                        datas?.UnlockAndReturn();
                        Debug.Print("DbClient ProcessData Invailed data");
                        break;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="mHisRequreData"></param>
        private void ProcessHisDataCallBack(int id, ByteBuffer mHisRequreData)
        {
            lock (mHisDataCallBack)
            {
                if (mHisDataCallBack.ContainsKey(id))
                {
                    mHisDataCallBack[id].Invoke(mHisRequreData);
                    mHisDataCallBack.Remove(id);
                }
                else
                {
                    mHisRequreData.UnlockAndReturn();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="mHisRequreData"></param>
        private void ProcessRealDataCallBack(int id, ByteBuffer mHisRequreData)
        {
            Stopwatch sw = Stopwatch.StartNew();
            sw.Start();
            lock (mRealDataCallBack)
            {
                if (mRealDataCallBack.ContainsKey(id))
                {
                    mRealDataCallBack[id].Invoke(mHisRequreData);
                    mRealDataCallBack.Remove(id);
                }
                else
                {
                    mHisRequreData.UnlockAndReturn();
                }
            }
            sw.Stop();

            if (sw.ElapsedMilliseconds > 100)
            {
                LoggerService.Service.Warn("ApiClient", $"ProcessRealDataCallBack 处理耗时： {sw.ElapsedMilliseconds}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="data"></param>
        private void ProcessTagInfoNotify(ByteBuffer data)
        {
            byte cmd = data.ReadByte();
            switch (cmd)
            {
                case ApiFunConst.DatabaseChangedNotify:
                    var type = data.ReadByte();
                    Task.Run(() => {
                        DatabaseChangedAction((type & 0x01) > 0, (type & 0x02) > 0, (type & 0x04) > 0);
                    });
                    data.UnlockAndReturn();
                    break;
                default:
                    data.UnlockAndReturn();
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        public bool Login(string username, string password, int timeount = 5000)
        {

            lock (mLoginLock)
            {
                LoggerService.Service.Info("Api Client", "Login");

                if (IsLogin) return true;
                if(mInfoRequreData!=null)
                {
                    mInfoRequreData.UnlockAndReturn();
                    mInfoRequreData = null;
                }
                mUser = username;
                mPass = password;
                int size = username.Length*2 + password.Length*2 + 9;
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, size);
                mb.Write(ApiFunConst.Login);
                mb.Write(username);
                mb.Write(password);

                infoRequreEvent.Reset();
                SendData(mb);

                if (infoRequreEvent.WaitOne(timeount))
                {
                    if (mInfoRequreData != null && (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex) > 4)
                    {
                        try
                        {
                            LoginId = mInfoRequreData.ReadLong();
                            return IsLogin;
                        }
                        finally
                        {
                            mInfoRequreData?.UnlockAndReturn();
                            mInfoRequreData = null;
                        }
                    }
                    else
                    {
                        LoggerService.Service.Info("ApiClient", $"登录数据无效!");
                    }
                }
                else
                {
                    LoggerService.Service.Info("ApiClient", $"登录超时!");
                }
                //mInfoRequreData?.Release();
                LoginId = -1;
                return IsLogin;
            }
        }

        /// <summary>
        /// 心跳处理
        /// </summary>
        public void Hart()
        {
            try
            {
                //LoggerService.Service.Info("Api Client", "Hart");
                int size = 2;
                var mb = GetBuffer(ApiFunConst.TagInfoRequest, size);
                mb.Write(ApiFunConst.Hart);
                mb.Write(byte.MaxValue);
                SendData(mb);
            }
            catch
            {

            }
        }

        private object mSyncLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string GetRealdatabase(int timeout = 50000)
        {
            string filename = string.Empty;
            LoggerService.Service.Info("Api Client", "GetRealdatabase");
            lock (mSyncLocker)
            {
                if(mRealSyncData != null)
                {
                    mRealSyncData.UnlockAndReturn();
                    mRealSyncData = null;
                }

                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                mb.Write(ApiFunConst.SyncRealTagConfig);
                mb.Write(LoginId);
                this.SyncDataEvent.Reset();
                SendData(mb);

                if (SyncDataEvent.WaitOne(timeout))
                {
                    try
                    {
                        if ((this.mRealSyncData.WriteIndex - mRealSyncData.ReadIndex) > 0)
                        {
                            try
                            {
                                System.IO.MemoryStream ms = new System.IO.MemoryStream();

                                ms.Write(this.mRealSyncData.ReadBytes((int)(this.mRealSyncData.WriteIndex - mRealSyncData.ReadIndex)));
                                ms.Position = 0;
                                System.IO.Compression.GZipStream gzip = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
                                filename = System.IO.Path.GetTempFileName();
                                var sfile = System.IO.File.Open(filename, System.IO.FileMode.OpenOrCreate);
                                gzip.CopyTo(sfile);
                                sfile.Close();

                                ms.Dispose();
                                gzip.Dispose();
                                return filename;
                            }
                            catch (Exception ex)
                            {
                                LoggerService.Service.Warn("ApiClient", "GetRealdatabase: " + ex.Message + "  " + ex.StackTrace);
                            }

                        }
                    }
                    finally
                    {
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = null;
                    }
                }
            }

            return filename;
        }

        /// <summary>
        /// 获取历史数据库配置
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string GetHisdatabase(int timeout = 50000)
        {
            string filename = string.Empty;
            LoggerService.Service.Info("Api Client", "GetHisdatabase");
            lock (mSyncLocker)
            {
                if (mRealSyncData != null)
                {
                    mRealSyncData.UnlockAndReturn();
                    mRealSyncData = null;
                }

                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                mb.Write(ApiFunConst.SyncHisTagConfig);
                mb.Write(LoginId);
                this.SyncDataEvent.Reset();
                SendData(mb);

                if (SyncDataEvent.WaitOne(timeout))
                {
                    try
                    {
                        if ((this.mRealSyncData.WriteIndex - mRealSyncData.ReadIndex) > 0)
                        {
                            try
                            {
                                System.IO.MemoryStream ms = new System.IO.MemoryStream();

                                ms.Write(this.mRealSyncData.ReadBytes((int)(this.mRealSyncData.WriteIndex - mRealSyncData.ReadIndex)));
                                ms.Position = 0;
                                System.IO.Compression.GZipStream gzip = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
                                filename = System.IO.Path.GetTempFileName();
                                var sfile = System.IO.File.Open(filename, System.IO.FileMode.OpenOrCreate);
                                gzip.CopyTo(sfile);
                                sfile.Close();

                                ms.Dispose();
                                gzip.Dispose();
                                return filename;
                            }
                            catch(Exception ex)
                            {
                                LoggerService.Service.Warn("ApiClient", "GetHisdatabase: " + ex.Message+"  "+ex.StackTrace);
                            }

                        }
                    }
                    finally
                    {
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = null;
                    }
                }
            }

            return filename;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string GetSecuritySetting(int timeout = 50000)
        {
            string filename = string.Empty;
            LoggerService.Service.Info("Api Client", "GetSecuritySetting");
            lock (mSyncLocker)
            {
                if (mRealSyncData != null)
                {
                    mRealSyncData.UnlockAndReturn();
                    mRealSyncData = null;
                }

                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                mb.Write(ApiFunConst.SyncSecuritySetting);
                mb.Write(LoginId);
                this.SyncDataEvent.Reset();
                SendData(mb);

                if (SyncDataEvent.WaitOne(timeout))
                {
                    try
                    {
                        if (this.mRealSyncData.WriteIndex - mRealSyncData.ReadIndex > 0)
                        {
                            try
                            {
                                System.IO.MemoryStream ms = new System.IO.MemoryStream();

                                ms.Write(this.mRealSyncData.ReadBytes((int)(this.mRealSyncData.WriteIndex - mRealSyncData.ReadIndex)));
                                ms.Position = 0;
                                System.IO.Compression.GZipStream gzip = new System.IO.Compression.GZipStream(ms, System.IO.Compression.CompressionMode.Decompress);
                                filename = System.IO.Path.GetTempFileName();
                                var sfile = System.IO.File.Open(filename, System.IO.FileMode.OpenOrCreate);
                                gzip.CopyTo(sfile);
                                sfile.Close();

                                ms.Dispose();
                                gzip.Dispose();
                                return filename;
                            }
                            catch (Exception ex)
                            {
                                LoggerService.Service.Warn("ApiClient", "GetSecuritySetting: " + ex.Message + "  " + ex.StackTrace);
                            }
                        }
                    }
                    finally
                    {
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = null;
                    }

                }
            }
            return filename;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagNames"></param>
        /// <returns></returns>
        public Dictionary<string, int> GetTagIds(List<string> tagNames, int timeout = 5000)
        {
            Dictionary<string, int> re = new Dictionary<string, int>();
            LoggerService.Service.Info("Api Client", "GetTagIds");
            lock (mLoginLock)
            {
                if (mInfoRequreData != null)
                {
                    mInfoRequreData.UnlockAndReturn();
                    mInfoRequreData = null;
                }

                var mb = GetBuffer(ApiFunConst.TagInfoRequest, tagNames.Count * 24 + 1 + 9);
                mb.Write(ApiFunConst.GetTagIdByNameFun);
                mb.Write(LoginId);
                mb.Write(tagNames.Count);
                foreach (var vv in tagNames)
                {
                    mb.Write(vv);
                }
                infoRequreEvent.Reset();
                SendData(mb);

                if (infoRequreEvent.WaitOne(timeout))
                {
                    try
                    {
                        for (int i = 0; i < tagNames.Count; i++)
                        {
                            if (mInfoRequreData.WriteIndex - mInfoRequreData.ReadIndex > 0)
                                re.Add(tagNames[i], mInfoRequreData.ReadInt());
                        }
                    }
                    finally
                    {
                        mInfoRequreData?.UnlockAndReturn();
                        mInfoRequreData = null;
                    }
                }
            }

            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="loginid"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string GetRunnerDatabase(int timeout = 5000)
        {
            LoggerService.Service.Info("Api Client", "GetRunnerDatabase");

            //if (!CheckLogin()) return String.Empty;
            lock (mLoginLock)
            {
                if (mInfoRequreData != null)
                {
                    mInfoRequreData.UnlockAndReturn();
                    mInfoRequreData = null;
                }

                //if (IsLogin)
                {
                    var mb = GetBuffer(ApiFunConst.TagInfoRequest, 8 + 1);
                    mb.Write(ApiFunConst.GetRunnerDatabase);
                    mb.Write(LoginId);
                    infoRequreEvent.Reset();
                    SendData(mb);

                    if (infoRequreEvent.WaitOne(timeout))
                    {
                        try
                        {
                            return mInfoRequreData.ReadString();
                        }
                        catch(Exception ex)
                        {
                            LoggerService.Service.Info("Api Client", $"GetRunnerDatabase {mInfoRequreData.Id} {ex.Message}");
                        }
                        finally
                        {
                            mInfoRequreData.UnlockAndReturn();
                            mInfoRequreData = null;
                        }

                    }
                }
            }
            return string.Empty;
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="MemoryLen">内存大小</param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<int,long> GetRealTagMemoryInfo(out int MemoryLen, int timeout = 50000)
        {
            string filename = string.Empty;
            LoggerService.Service.Info("Api Client", "GetRealTagMemoryInfo");
            lock (mSyncLocker)
            {
                if (mRealSyncData != null)
                {
                    mRealSyncData.UnlockAndReturn();
                    mRealSyncData = null;
                }

                var mb = GetBuffer(ApiFunConst.TagInfoRequest, 1 + 9);
                mb.Write(ApiFunConst.RealTagMemoryInfo);
                mb.Write(LoginId);
                this.SyncDataEvent.Reset();
                SendData(mb);

                if (SyncDataEvent.WaitOne(timeout))
                {
                    try
                    {
                        if ((this.mRealSyncData.WriteIndex - mRealSyncData.ReadIndex) > 0)
                        {
                            try
                            {
                                MemoryLen = this.mRealSyncData.ReadInt();
                                int count = mRealSyncData.ReadInt();
                                Dictionary<int, long> re = new Dictionary<int, long>(count);
                                for(int i=0;i<count; i++)
                                {
                                    re.Add(mRealSyncData.ReadInt(), mRealSyncData.ReadLong());
                                }
                                return re;
                            }
                            catch (Exception ex)
                            {
                                LoggerService.Service.Warn("ApiClient", "GetRealdatabase: " + ex.Message + "  " + ex.StackTrace);
                            }

                        }
                    }
                    finally
                    {
                        mRealSyncData?.UnlockAndReturn();
                        mRealSyncData = null;
                    }
                }
            }
            MemoryLen = 0;
            return null;
        }

        #region RealData



        /// <summary>
        /// 
        /// </summary>
        /// <param name="loginid"></param>
        /// <param name="minid"></param>
        /// <param name="maxid"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool RegistorTagValueCallBack(int minid, int maxid, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8);
                mb.Write(ApiFunConst.RegistorValueCallback);
                mb.Write(this.LoginId);
                mb.Write(minid);
                mb.Write(maxid);
                
                this.realRequreEvent.Reset();
                SendData(mb);

                if (realRequreEvent.WaitOne(timeout))
                {
                    try
                    {
                        return mRealRequreData.ReadByte() > 0;
                    }
                    finally
                    {
                        mRealRequreData?.UnlockAndReturn();
                        mRealRequreData = null;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private int GetRealRequreId()
        {
            lock (mRealDataLock)
            {
                var vid = mHisRequreCount;
                mHisRequreCount++;
                return vid;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="callback"></param>
        private void RegistorRealDataCallback(int id,Action<ByteBuffer> callback)
        {
            lock (mRealDataLock)
            {
                mRealDataCallBack.Add(id, callback);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="callback"></param>
        private void UnRegistorRealDataCallback(int id)
        {
            lock (mRealDataLock)
            {
                if(mRealDataCallBack.ContainsKey(id))
                {
                    mRealDataCallBack.Remove(id);
                }
            }
        }

        //private ManualResetEvent mRegistorTagBlockValueCallBackEvent = new ManualResetEvent(false);

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool RegistorTagBlockValueCallBack(int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return false;

                var vid = GetRealRequreId();
                //ByteBuffer re=null;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8+4);
                mb.Write(ApiFunConst.BlockValueChangeNotify);
                mb.Write(this.LoginId);
                mb.Write(vid);

                var buffer = SendAndWait(vid,mb,timeout);

                if (buffer != null)
                {
                    try
                    {
                        return buffer.ReadByte() > 0;
                    }
                    finally
                    {
                        buffer.UnlockAndReturn();
                    }
                }

                //mRegistorTagBlockValueCallBackEvent.Reset();
                //RegistorRealDataCallback(vid, (data) => { 
                //    re= data;
                //    mRegistorTagBlockValueCallBackEvent.Set();
                //});
                ////this.realRequreEvent.Reset();

                //SendData(mb);

                //if (mRegistorTagBlockValueCallBackEvent.WaitOne(timeout))
                //{
                //    try
                //    {
                //        return re?.ReadByte() > 0;
                //    }
                //    finally
                //    {
                //        re?.UnlockAndReturn();
                //        re = null;
                //    }

                //}
            }

            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool ClearRegistorTagValueCallBack(int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return false;

                var vid = GetRealRequreId();
                ByteBuffer re = null;

                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8+4);
                mb.Write(ApiFunConst.ResetValueChangeNotify);
                mb.Write(this.LoginId);
                mb.Write(vid);

                var buffer = SendAndWait(vid, mb, timeout);

                if (buffer != null)
                {
                    try
                    {
                        return buffer.ReadByte() > 0;
                    }
                    finally
                    {
                        buffer.UnlockAndReturn();
                    }
                }

                //mRegistorTagBlockValueCallBackEvent.Reset();
                //RegistorRealDataCallback(vid, (data) => {
                //    re = data;
                //    mRegistorTagBlockValueCallBackEvent.Set();
                //});

                ////realRequreEvent.Reset();
                //SendData(mb);

                //if (mRegistorTagBlockValueCallBackEvent.WaitOne(timeout))
                //{
                //    try
                //    {
                //        return re?.ReadByte() > 0;
                //    }
                //    finally
                //    {
                //        re?.UnlockAndReturn();
                //        re = null;
                //    }
                //}
            }

            return false;
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ByteBuffer GetRealData(List<int> ids, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;

                var vid = GetRealRequreId();

                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4+4);
                mb.Write(ApiFunConst.RequestRealData);
                mb.Write(this.LoginId);
                mb.Write(vid);

                mb.Write(ids.Count);
             
                for (int i = 0; i < ids.Count; i++)
                {
                    mb.Write(ids[i]);
                }
                return SendAndWait(vid, mb, timeout);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<short> GetStateData(List<int> ids, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;

                var vid = GetRealRequreId();

                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4 + 4);
                mb.Write(ApiFunConst.RequestStateData);
                mb.Write(this.LoginId);
                mb.Write(vid);

                mb.Write(ids.Count);

                for (int i = 0; i < ids.Count; i++)
                {
                    mb.Write(ids[i]);
                }
                var re = SendAndWait(vid, mb, timeout);
                if(re!=null)
                {
                    List<short> rd = new List<short>();
                    int count = re.ReadInt();
                    for(int i=0;i<count;i++)
                    {
                        rd.Add(re.ReadShort());
                    }
                    return rd;
                }
                else
                {
                    return null;
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public List<long> GetExtendField2Data(List<int> ids, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;

                var vid = GetRealRequreId();

                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4 + 4);
                mb.Write(ApiFunConst.RequestExtendField2);
                mb.Write(this.LoginId);
                mb.Write(vid);

                mb.Write(ids.Count);

                for (int i = 0; i < ids.Count; i++)
                {
                    mb.Write(ids[i]);
                }
                var re = SendAndWait(vid, mb, timeout);
                if (re != null)
                {
                    List<long> rd = new List<long>();
                    int count = re.ReadInt();
                    for (int i = 0; i < count; i++)
                    {
                        rd.Add(re.ReadLong());
                    }
                    return rd;
                }
                else
                {
                    return null;
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer GetRealDataByMemoryCopy(List<int> ids, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;
                var vid = GetRealRequreId();

                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + ids.Count * 4 + 4);
                mb.Write(ApiFunConst.RequestRealDataByMemoryCopy);
                mb.Write(this.LoginId);
                mb.Write(vid);
                mb.Write(ids.Count);

                for (int i = 0; i < ids.Count; i++)
                {
                    mb.Write(ids[i]);
                }
                return SendAndWait(vid, mb, timeout);
            }

        }

        private ByteBuffer SendAndWait(int vid,ByteBuffer mb,int timeout)
        {
            ByteBuffer re=null;
            ManualResetEvent mGetRealDataCallBackEvent = new ManualResetEvent(false);
            try
            {
                RegistorRealDataCallback(vid, (data) =>
                {
                    re = data;
                    if(!mGetRealDataCallBackEvent.SafeWaitHandle.IsClosed)
                    mGetRealDataCallBackEvent.Set();
                    else
                    {
                        data?.UnlockAndReturn();
                    }
                });
                SendData(mb);

                if (mGetRealDataCallBackEvent.WaitOne(timeout))
                {
                    return re;
                }
                else
                {
                    re?.UnlockAndReturn();
                }
            }
            finally
            {
                UnRegistorRealDataCallback(vid);
                mGetRealDataCallBackEvent.Dispose();
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer GetRealData(int ids, int ide, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;

                var vid = GetRealRequreId();
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8+4);
                mb.Write(ApiFunConst.RequestRealData2);
                mb.Write(this.LoginId);
                mb.Write(vid);
                mb.Write(ids);
                mb.Write(ide);

               return SendAndWait(vid,mb,timeout);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer GetComplexTagRealData(int ids, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;

                var vid = GetRealRequreId();
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 8 + 4);
                mb.Write(ApiFunConst.RequestComplexTagRealValue);
                mb.Write(this.LoginId);
                mb.Write(vid);
                mb.Write(ids);

                return SendAndWait(vid, mb, timeout);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="minid"></param>
        /// <param name="maxid"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer SyncRealMemory(int startaddress, int endaddress, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;
                var vid = GetRealRequreId();

                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 4);
                mb.Write(ApiFunConst.RealMemorySync);
                mb.Write(this.LoginId);
                mb.Write(vid);
                mb.Write((endaddress - startaddress));
                mb.Write(startaddress);

                return SendAndWait(vid, mb, timeout);
                //realRequreEvent.Reset();
                //SendData(mb);
                //if (realRequreEvent.WaitOne(timeout))
                //{
                //    if (mRealRequreData.WriteIndex - mRealRequreData.ReadIndex == (endaddress - startaddress))
                //    {
                //        try
                //        {
                //            lock (mRealDataLocker)
                //            {
                //                var vre = mRealRequreData;
                //                vre.IncRef();
                //                mRealRequreData = null;
                //                return vre;
                //            }
                //        }
                //        catch
                //        {

                //        }
                //    }
                //   return null;
                //}
                //else
                //{
                //    return null;
                //}
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ids"></param>
        /// <param name="ide"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer GetRealDataByMemoryCopy(int ids, int ide, int timeout = 5000)
        {
            lock (mRealDataLock)
            {
                if (!CheckLogin()) return null;
                var vid = GetRealRequreId();

                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + (ide - ids) * 4);
                mb.Write(ApiFunConst.RequestRealData2ByMemoryCopy);
                mb.Write(this.LoginId);
                mb.Write(vid);

                mb.Write(ids);
                mb.Write(ide);
                return SendAndWait(vid,mb, timeout);
                //realRequreEvent.Reset();
                //SendData(mb);

                //if (realRequreEvent.WaitOne(timeout))
                //{
                //    lock (mRealDataLocker)
                //    {
                //        var vre = mRealRequreData;
                //        vre.IncRef();
                //        mRealRequreData = null;
                //        return vre;
                //    }
                //}
            }
            //mRealRequreData?.ReleaseBuffer();

            //return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="valueType"></param>
        /// <param name="value"></param>
        public bool SetTagValue(int id, byte valueType, object value, int timeout = 5000)
        {
            lock (mRealSetDataLock)
            {
                if (!CheckLogin()) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 30);
                mb.Write(ApiFunConst.SetDataValue);
                mb.Write(this.LoginId);
                mb.Write(1);
                mb.Write(id);
                mb.WriteByte(valueType);
                switch (valueType)
                {
                    case (byte)TagType.Bool:
                        mb.Write((byte)value);
                        break;
                    case (byte)TagType.Byte:
                        mb.Write((byte)value);
                        break;
                    case (byte)TagType.Short:
                        mb.Write((short)value);
                        break;
                    case (byte)TagType.UShort:
                        mb.Write((ushort)value);
                        break;
                    case (byte)TagType.Int:
                        mb.Write((int)value);
                        break;
                    case (byte)TagType.UInt:
                        mb.Write((int)value);
                        break;
                    case (byte)TagType.Long:
                    case (byte)TagType.ULong:
                        mb.Write((long)value);
                        break;
                    case (byte)TagType.Float:
                        mb.Write((float)value);
                        break;
                    case (byte)TagType.Double:
                        mb.Write((double)value);
                        break;
                    case (byte)TagType.String:
                        string sval = value.ToString();
                        //mb.Write(sval.Length);
                        mb.Write(sval, Encoding.Unicode);
                        break;
                    case (byte)TagType.DateTime:
                        mb.Write(((DateTime)value).Ticks);
                        break;
                    case (byte)TagType.IntPoint:
                        mb.Write(((IntPointData)value).X);
                        mb.Write(((IntPointData)value).Y);
                        break;
                    case (byte)TagType.UIntPoint:
                        mb.Write((int)((UIntPointData)value).X);
                        mb.Write((int)((UIntPointData)value).Y);
                        break;
                    case (byte)TagType.IntPoint3:
                        mb.Write(((IntPoint3Data)value).X);
                        mb.Write(((IntPoint3Data)value).Y);
                        mb.Write(((IntPoint3Data)value).Z);
                        break;
                    case (byte)TagType.UIntPoint3:
                        mb.Write((int)((UIntPoint3Data)value).X);
                        mb.Write((int)((UIntPoint3Data)value).Y);
                        mb.Write((int)((UIntPoint3Data)value).Z);
                        break;
                    case (byte)TagType.LongPoint:
                        mb.Write(((LongPointData)value).X);
                        mb.Write(((LongPointData)value).Y);
                        break;
                    case (byte)TagType.ULongPoint:
                        mb.Write((long)((ULongPointData)value).X);
                        mb.Write((long)((ULongPointData)value).Y);
                        break;
                    case (byte)TagType.LongPoint3:
                        mb.Write(((LongPoint3Data)value).X);
                        mb.Write(((LongPoint3Data)value).Y);
                        mb.Write(((LongPoint3Data)value).Z);
                        break;
                    case (byte)TagType.ULongPoint3:
                        mb.Write((long)((ULongPoint3Data)value).X);
                        mb.Write((long)((ULongPoint3Data)value).Y);
                        mb.Write((long)((ULongPoint3Data)value).Z);
                        break;
                }
                realSetRequreEvent.Reset();
                SendData(mb);

                if (realSetRequreEvent.WaitOne(timeout))
                {
                    if (this.mRealSetResponseData.WriteIndex - mRealSetResponseData.ReadIndex > 0)
                    {
                        try
                        {
                            return mRealSetResponseData.ReadByte() > 0;
                        }
                        finally
                        {
                            mRealSetResponseData?.UnlockAndReturn();
                            mRealSetResponseData = null;
                        }
                    }
                    else
                    {
                        return true;
                    }
                }
            }
            return false;
        }
        /// <summary>
        /// 设置变量的状态
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagStateValue(Dictionary<int,short> values, int timeout = 5000)
        {
            lock (mRealSetDataLock)
            {
                if (!CheckLogin()) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, values.Count*6+ 8 + 30);
                mb.Write(ApiFunConst.SetStateData);
                mb.Write(this.LoginId);
                mb.Write(values.Count);
                foreach(var value in values)
                {
                    mb.Write(value.Key);
                    mb.Write(value.Value);
                }
                realSetRequreEvent.Reset();
                SendData(mb);

                if (realSetRequreEvent.WaitOne(timeout))
                {
                    if (this.mRealSetResponseData.WriteIndex - mRealSetResponseData.ReadIndex > 0)
                    {
                        try
                        {
                            return mRealSetResponseData.ReadByte() > 0;
                        }
                        finally
                        {
                            mRealSetResponseData?.UnlockAndReturn();
                            mRealSetResponseData = null;
                        }
                    }
                    else
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// 设置变量的扩展属性2
        /// </summary>
        /// <param name="values"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagExtendField2Value(Dictionary<int, long> values, int timeout = 5000)
        {
            lock (mRealSetDataLock)
            {
                if (!CheckLogin()) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, values.Count * 12 + 8 + 30);
                mb.Write(ApiFunConst.SetExtendField2Data);
                mb.Write(this.LoginId);
                mb.Write(values.Count);
                foreach (var value in values)
                {
                    mb.Write(value.Key);
                    mb.Write(value.Value);
                }
                realSetRequreEvent.Reset();
                SendData(mb);

                if (realSetRequreEvent.WaitOne(timeout))
                {
                    if (this.mRealSetResponseData.WriteIndex - mRealSetResponseData.ReadIndex > 0)
                    {
                        try
                        {
                            return mRealSetResponseData.ReadByte() > 0;
                        }
                        finally
                        {
                            mRealSetResponseData?.UnlockAndReturn();
                            mRealSetResponseData = null;
                        }
                    }
                    else
                    {
                        return true;
                    }
                }
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="valueType"></param>
        /// <param name="value"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool SetTagValue(List<int> id, List<byte> valueType, List<object> value, int timeout = 5000)
        {
            lock (mRealSetDataLock)
            {
                if (!CheckLogin()) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 30);
                mb.Write(ApiFunConst.SetDataValue);
                mb.Write(this.LoginId);
                mb.Write(id.Count);
                for (int i = 0; i < id.Count; i++)
                {
                    mb.Write(id[i]);
                    mb.WriteByte(valueType[i]);
                    switch (valueType[i])
                    {
                        case (byte)TagType.Bool:
                            mb.Write((byte)value[i]);
                            break;
                        case (byte)TagType.Byte:
                            mb.Write((byte)value[i]);
                            break;
                        case (byte)TagType.Short:
                            mb.Write((short)value[i]);
                            break;
                        case (byte)TagType.UShort:
                            mb.Write((ushort)value[i]);
                            break;
                        case (byte)TagType.Int:
                            mb.Write((int)value[i]);
                            break;
                        case (byte)TagType.UInt:
                            mb.Write((int)value[i]);
                            break;
                        case (byte)TagType.Long:
                        case (byte)TagType.ULong:
                            mb.Write((long)value[i]);
                            break;
                        case (byte)TagType.Float:
                            mb.Write((float)value[i]);
                            break;
                        case (byte)TagType.Double:
                            mb.Write((double)value[i]);
                            break;
                        case (byte)TagType.String:
                            string sval = value[i].ToString();
                            //mb.Write(sval.Length);
                            mb.Write(sval, Encoding.Unicode);
                            break;
                        case (byte)TagType.DateTime:
                            mb.Write(((DateTime)value[i]).Ticks);
                            break;
                        case (byte)TagType.IntPoint:
                            mb.Write(((IntPointData)value[i]).X);
                            mb.Write(((IntPointData)value[i]).Y);
                            break;
                        case (byte)TagType.UIntPoint:
                            mb.Write((int)((UIntPointData)value[i]).X);
                            mb.Write((int)((UIntPointData)value[i]).Y);
                            break;
                        case (byte)TagType.IntPoint3:
                            mb.Write(((IntPoint3Data)value[i]).X);
                            mb.Write(((IntPoint3Data)value[i]).Y);
                            mb.Write(((IntPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.UIntPoint3:
                            mb.Write((int)((UIntPoint3Data)value[i]).X);
                            mb.Write((int)((UIntPoint3Data)value[i]).Y);
                            mb.Write((int)((UIntPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.LongPoint:
                            mb.Write(((LongPointData)value[i]).X);
                            mb.Write(((LongPointData)value[i]).Y);
                            break;
                        case (byte)TagType.ULongPoint:
                            mb.Write((long)((ULongPointData)value[i]).X);
                            mb.Write((long)((ULongPointData)value[i]).Y);
                            break;
                        case (byte)TagType.LongPoint3:
                            mb.Write(((LongPoint3Data)value[i]).X);
                            mb.Write(((LongPoint3Data)value[i]).Y);
                            mb.Write(((LongPoint3Data)value[i]).Z);
                            break;
                        case (byte)TagType.ULongPoint3:
                            mb.Write((long)((ULongPoint3Data)value[i]).X);
                            mb.Write((long)((ULongPoint3Data)value[i]).Y);
                            mb.Write((long)((ULongPoint3Data)value[i]).Z);
                            break;
                    }
                }
                realSetRequreEvent.Reset();
                SendData(mb);
                try
                {
                    if (realSetRequreEvent.WaitOne(timeout))
                    {
                        return mRealSetResponseData.ReadByte() > 0;
                    }
                }
                finally
                {
                    mRealSetResponseData?.UnlockAndReturn();
                    mRealSetResponseData = null;
                }
                return false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="valueType"></param>
        /// <param name="value"></param>
        public bool ResetTagToLastValue(int id, int timeout = 5000)
        {
            lock (mRealResetDataLock)
            {
                if (!CheckLogin()) return false;
                var mb = GetBuffer(ApiFunConst.RealDataRequestFun, 8 + 30);
                mb.Write(ApiFunConst.SetRealDataToLastData);
                mb.Write(this.LoginId);
                mb.Write(id);
                realResetRequreEvent.Reset();
                SendData(mb);

                if (realResetRequreEvent.WaitOne(timeout))
                {
                    if (this.mRealResetResponseData.WriteIndex - mRealResetResponseData.ReadIndex > 0)
                    {
                        try
                        {
                            return mRealResetResponseData.ReadByte() > 0;
                        }
                        finally
                        {
                            mRealResetResponseData?.UnlockAndReturn();
                            mRealResetResponseData = null;
                        }
                    }
                    else
                    {
                        return true;
                    }
                }
            }
            return false;
        }


        #endregion


        private bool CheckLogin()
        {
            if (LoginId <= 0)
            {
                Login(mUser, mPass);
            }
            return IsLogin;
        }



        #region HisData

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public ByteBuffer QueryAllHisValue(int id, DateTime startTime, DateTime endTime, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }
            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestAllHisData);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) => {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }

            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }

                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 通过SQL语句查询历史数据
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="timeout"></param>
        /// <returns>HisQueryTableResult或者List<double> </returns>
        public object QueryValueBySql(string sql, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }
            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + sql.Length + 4);
            mb.Write(ApiFunConst.ReadHisValueBySQL);
            mb.Write(this.LoginId);
            mb.Write(sql);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) => {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }

            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    var byt = re.ReadByte();
                    if (ApiFunConst.ReadHisValueBySQL == byt)
                    {
                        var typ = re.ReadByte();
                        if (typ == 0)
                        {
                            Cdy.Tag.HisQueryTableResult rel = new HisQueryTableResult();
                            string smeta = re.ReadString();
                            rel.FromStringToMeta(smeta);
                            int dsize = re.ReadInt();
                            rel.Init2(dsize);
                            re.CopyTo(rel.Address, re.ReadIndex, 0, dsize);
                            re.UnlockAndReturn();
                            return rel;
                        }
                        else if (typ == 1) 
                        {
                            List<double> ltmp = new List<double>();
                            int dsize = re.ReadInt();
                            for (int i = 0; i < dsize; i++)
                            {
                                ltmp.Add(re.ReadDouble());
                            }
                            re.UnlockAndReturn();
                            return ltmp;
                        }
                        else if (typ == 2)
                        {
                            //实时值
                            return re;
                        }
                    }
                    else
                    {
                        re.UnlockAndReturn();
                    }
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                    }

                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer QueryStatisitcsValue(int id, DateTime startTime, DateTime endTime, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestNumberStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        public ByteBuffer QueryHisValueAtTimes(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5 + 4);
            mb.Write(ApiFunConst.RequestHisDatasByTimePoint);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(times.Count);
            for (int i = 0; i < times.Count; i++)
            {
                mb.Write(times[i].Ticks);
            }
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        public ByteBuffer QueryHisValueAtTimesByIgnorSystemExit(int id, List<DateTime> times, Cdy.Tag.QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5 + 4);
            mb.Write(ApiFunConst.RequestHisDatasByTimePointByIgnorClosedQuality);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(times.Count);
            for (int i = 0; i < times.Count; i++)
            {
                mb.Write(times[i].Ticks);
            }
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="times"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer QueryStatisticsHisValueAtTimes(int id, List<DateTime> times, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + times.Count * 8 + 5 + 4);
            mb.Write(ApiFunConst.RequestNumberStatisticsByTimePoint);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(times.Count);
            for (int i = 0; i < times.Count; i++)
            {
                mb.Write(times[i].Ticks);
            }
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }

            //ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            //hisRequreEvent.Reset();

            //SendData(mb);
            //if (hisRequreEvent.WaitOne(timeout) && re!=null && re.WriteIndex - re.ReadIndex > 1)
            //{
            //    try
            //    {
            //        return mHisRequreData;
            //    }
            //    finally
            //    {
            //        mHisRequreData = null;
            //    }
            //}
            //return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="span"></param>
        /// <param name="matchType"></param>
        /// <returns></returns>
        public ByteBuffer QueryHisValueForTimeSpan(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 24 + 5 + 4);
            mb.Write(ApiFunConst.RequestHisDataByTimeSpan);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(span.Ticks);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="span"></param>
        /// <param name="matchType"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public ByteBuffer QueryHisValueForTimeSpanByIgnorSystemExit(int id, DateTime startTime, DateTime endTime, TimeSpan span, QueryValueMatchType matchType, int timeout = 5000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return null;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 24 + 5 + 4);
            mb.Write(ApiFunConst.RequestHisDataByTimeSpanByIgnorClosedQuality);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write((byte)matchType);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(span.Ticks);
            mb.Write(vid);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();
            try
            {
                lock (mHisDataCallBack)
                {
                    mHisDataCallBack.Add(vid, (data) =>
                    {
                        re = data;
                        try
                        {
                            hisRequreEvent.Set();
                        }
                        catch
                        {
                            data?.UnlockAndReturn();
                        }
                    });
                }
                SendData(mb);
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re;
                        }
                    }
                }
                return null;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Tuple<DateTime,object> FindNumberTagValue(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval, int timeout = 60000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return new Tuple<DateTime, object>(DateTime.MinValue, 0);
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)0);
            mb.Write(((byte)type).ToString() + "|" + value.ToString() +"|"+interval);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    var dt = re.ReadDateTime();
                    var dd = re.ReadDouble();
                    return new Tuple<DateTime, object>(dt, dd);
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            var dt = re.ReadDateTime();
                            var dd = re.ReadDouble();
                            return new Tuple<DateTime, object>(dt, dd);
                        }
                    }
                }
                return new Tuple<DateTime, object>(DateTime.MinValue,0);
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Dictionary<DateTime,object> FindNumberTagValues(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval, int timeout = 60000)
        {
            Dictionary<DateTime,object> red = new Dictionary<DateTime,object>();
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return red;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)1);
            mb.Write(((byte)type).ToString() + "|" + value.ToString() + "|" + interval);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    var cc = re.ReadInt();
                    for(int i=0;i<cc;i++)
                    {
                        red.Add(re.ReadDateTime(),re.ReadDouble());
                    }
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            var cc = re.ReadInt();
                            for (int i = 0; i < cc; i++)
                            {
                                red.Add(re.ReadDateTime(),re.ReadDouble());
                            }
                        }
                    }
                }
                return red;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public double FindNumberTagValueDuration(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval, int timeout = 60000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return double.MinValue;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)2);
            mb.Write(((byte)type).ToString() + "|" + value.ToString() + "|" + interval);

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re.ReadDouble();
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re.ReadDouble();
                        }
                    }
                }
                return double.MinValue;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Tuple<double,List<DateTime>> FindNumberTagMaxMinValue(int id, DateTime startTime, DateTime endTime, NumberStatisticsType type,int timeout = 60000)
        {
            List<DateTime> red = new List<DateTime>();
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return new Tuple<double, List<DateTime>>(double.MinValue, red);
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)3);
            mb.Write(((byte)type).ToString());

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                double dval = double.MinValue;
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    dval = re.ReadDouble();
                    var cc = re.ReadInt();
                    for (int i = 0; i < cc; i++)
                    {
                        red.Add(re.ReadDateTime());
                    }
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            dval = re.ReadDouble();
                            var cc = re.ReadInt();
                            for (int i = 0; i < cc; i++)
                            {
                                red.Add(re.ReadDateTime());
                            }
                        }
                    }
                }
                return new Tuple<double, List<DateTime>>(dval,red);
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public double FindNumberTagAvgValue(int id, DateTime startTime, DateTime endTime, int timeout = 60000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return double.MinValue;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)4);
            mb.Write("4");

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re.ReadDouble();
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re.ReadDouble();
                        }
                    }
                }
                return double.MinValue;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }



        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public DateTime FindNoNumberTagValue(int id, DateTime startTime, DateTime endTime, object value, int timeout = 60000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return DateTime.MinValue;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)10);
            if (value is bool)
            {
                mb.Write(Convert.ToByte(value).ToString());
            }
            else if (value is DateTime)
            {
                mb.Write((Convert.ToDateTime(value).Ticks).ToString());
            }
            else
            {
                mb.Write(value.ToString());
            }


            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re.ReadDateTime();
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re.ReadDateTime();
                        }
                    }
                }
                return DateTime.MinValue;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public List<DateTime> FindNoNumberTagValues(int id, DateTime startTime, DateTime endTime, object value, int timeout = 60000)
        {
            List<DateTime> red = new List<DateTime>();
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return red;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)11);
            if (value is bool)
            {
                mb.Write(Convert.ToByte(value).ToString());
            }
            else if (value is DateTime)
            {
                mb.Write((Convert.ToDateTime(value).Ticks).ToString());
            }
            else
            {
                mb.Write(value.ToString());
            }

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    var cc = re.ReadInt();
                    for (int i = 0; i < cc; i++)
                    {
                        red.Add(re.ReadDateTime());
                    }
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            var cc = re.ReadInt();
                            for (int i = 0; i < cc; i++)
                            {
                                red.Add(re.ReadDateTime());
                            }
                        }
                    }
                }
                return red;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public double FindNoNumberTagValueDuration(int id, DateTime startTime, DateTime endTime, object value, int timeout = 60000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return double.MinValue;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 8 + 20 + 4);
            mb.Write(ApiFunConst.RequestValueStatistics);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(startTime.Ticks);
            mb.Write(endTime.Ticks);
            mb.Write(vid);
            mb.WriteByte((byte)12);
            if (value is bool)
            {
                mb.Write(Convert.ToByte(value).ToString());
            }
            else if (value is DateTime)
            {
                mb.Write((Convert.ToDateTime(value).Ticks).ToString());
            }
            else
            {
                mb.Write(value.ToString());
            }

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re.ReadDouble();
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re.ReadDouble();
                        }
                    }
                }
                return double.MinValue;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool ModifyHisValue(int id,TagType type,string user,string msg ,IEnumerable<TagHisValue<object>> values, int timeout = 60000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return false;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 13+user.Length*2+2+msg.Length*2+2+values.Count()*34+4+4+1);
            mb.Write(ApiFunConst.ModifyHisData);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(user);
            mb.Write(msg);
            mb.Write((byte)type);
            mb.Write(values.Count());
            mb.Write(vid);

            foreach (var vv in values)
            {
                mb.Write(vv.Time);
                switch (type)
                {
                    case TagType.Bool:
                        mb.Write(Convert.ToByte(Convert.ToBoolean(vv.Value)));
                        break;
                    case TagType.Byte:
                        mb.Write(Convert.ToByte(vv.Value));
                        break;
                    case TagType.UShort:
                        mb.Write(Convert.ToUInt16(vv.Value));
                        break;
                    case TagType.Short:
                        mb.Write(Convert.ToInt16(vv.Value));
                        break;
                    case TagType.Int:
                        mb.Write(Convert.ToInt32(vv.Value));
                        break;
                    case TagType.UInt:
                        mb.Write(Convert.ToUInt32(vv.Value));
                        break;
                    case TagType.Long:
                        mb.Write(Convert.ToInt64(vv.Value));
                        break;
                    case TagType.ULong:
                        mb.Write(Convert.ToUInt64(vv.Value));
                        break;
                    case TagType.Double:
                        mb.Write(Convert.ToDouble(vv.Value));
                        break;
                    case TagType.Float:
                        mb.Write(Convert.ToSingle(vv.Value));
                        break;
                    case TagType.String:
                        mb.Write(Convert.ToString(vv.Value));
                        break;
                    case TagType.DateTime:
                        mb.Write(Convert.ToDateTime(vv.Value).Ticks);
                        break;
                    case TagType.IntPoint:
                        IntPointData ip = (IntPointData)vv.Value;
                        mb.Write(ip.X);
                        mb.Write(ip.Y);
                        break;
                    case TagType.UIntPoint:
                        UIntPointData uip = (UIntPointData)vv.Value;
                        mb.Write(uip.X);
                        mb.Write(uip.Y);
                        break;
                    case TagType.IntPoint3:
                        IntPoint3Data ip3 = (IntPoint3Data)vv.Value;
                        mb.Write(ip3.X);
                        mb.Write(ip3.Y);
                        mb.Write(ip3.Z);
                        break;
                    case TagType.UIntPoint3:
                        UIntPoint3Data uip3 = (UIntPoint3Data)vv.Value;
                        mb.Write(uip3.X);
                        mb.Write(uip3.Y);
                        mb.Write(uip3.Z);
                        break;
                    case TagType.LongPoint:
                        LongPointData lip = (LongPointData)vv.Value;
                        mb.Write(lip.X);
                        mb.Write(lip.Y);
                        break;
                    case TagType.LongPoint3:
                        LongPoint3Data lip3 = (LongPoint3Data)vv.Value;
                        mb.Write(lip3.X);
                        mb.Write(lip3.Y);
                        mb.Write(lip3.Z);
                        break;
                    case TagType.ULongPoint:
                        LongPointData ulip = (LongPointData)vv.Value;
                        mb.Write(ulip.X);
                        mb.Write(ulip.Y);
                        break;
                    case TagType.ULongPoint3:
                        LongPoint3Data ulip3 = (LongPoint3Data)vv.Value;
                        mb.Write(ulip3.X);
                        mb.Write(ulip3.Y);
                        mb.Write(ulip3.Z);
                        break;
                }
                mb.WriteByte(vv.Quality);

            }

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re.ReadByte()>0;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re.ReadByte()>0;
                        }
                    }
                }
                return false;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool DeleteHisValue(int id,  string user, string msg, DateTime stime,DateTime etime, int timeout = 60000)
        {
            var vid = 0;
            ByteBuffer re = null;
            lock (mHisDataLock)
            {
                vid = mHisRequreCount;
                mHisRequreCount++;
                if (!CheckLogin()) return false;
            }

            var mb = GetBuffer(ApiFunConst.HisDataRequestFun, 13 + user.Length * 2 + 2 + msg.Length * 2 + 2 + 16+ 4 + 4 + 1);
            mb.Write(ApiFunConst.DeleteHisData);
            mb.Write(this.LoginId);
            mb.Write(id);
            mb.Write(user);
            mb.Write(msg);
            mb.Write(stime);
            mb.Write(etime);
            mb.Write(vid);


          

            ManualResetEvent hisRequreEvent = new ManualResetEvent(false);
            hisRequreEvent.Reset();

            lock (mHisDataCallBack)
            {
                mHisDataCallBack.Add(vid, (data) =>
                {
                    re = data;
                    try
                    {
                        hisRequreEvent.Set();
                    }
                    catch
                    {
                        data?.UnlockAndReturn();
                    }
                });
            }
            SendData(mb);
            try
            {
                if (hisRequreEvent.WaitOne(timeout) && re != null && re.WriteIndex - re.ReadIndex > 1)
                {
                    return re.ReadByte()>0;
                }
                else
                {
                    lock (mHisDataCallBack)
                    {
                        if (mHisDataCallBack.ContainsKey(vid))
                        {
                            mHisDataCallBack.Remove(vid);
                        }
                        else
                        {
                            return re.ReadByte()>0;
                        }
                    }
                }
                return false;
            }
            finally
            {
                hisRequreEvent.Dispose();
            }
        }
        #endregion

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
