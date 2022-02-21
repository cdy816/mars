using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;
using System.Linq;

namespace DirectAccess
{
    /// <summary>
    /// 
    /// </summary>
    public class Client:IDisposable
    {

        #region ... Variables  ...

        private string mLoginId = string.Empty;

        
        private DirectAccess.Data.DataClient mDataClient;


        private Grpc.Net.Client.GrpcChannel grpcChannel;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...



        /// <summary>
        /// 
        /// </summary>
        /// <param name="ip"></param>
        /// <param name="port"></param>
        public Client(string ip,int port)
        {
            Ip = ip;
            Port = port;
            Init();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 使用TLS加密
        /// </summary>
        public bool UseTls { get; set; } = true;

        public string Ip { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public bool IsLogined
        {
            get
            {
                return !string.IsNullOrEmpty(mLoginId);
            }
        }

        /// <summary>
        /// 超时时间
        /// </summary>
        public int TimeOut { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime LoginTime { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public Client Init()
        {
            try
            {
                var httpClientHandler = new HttpClientHandler();
                httpClientHandler.ServerCertificateCustomValidationCallback =
                    HttpClientHandler.DangerousAcceptAnyServerCertificateValidator;
                var httpClient = new HttpClient(httpClientHandler);
                if(UseTls)
                grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"https://" + Ip + ":"+ Port, new GrpcChannelOptions { HttpClient = httpClient });
                else
                {
                    grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"http://" + Ip + ":" + Port, new GrpcChannelOptions { HttpClient = httpClient });
                }
                mDataClient = new DirectAccess.Data.DataClient(grpcChannel);
            }
            catch (Exception ex)
            {
                //LoggerService.Service.Erro("DevelopService", ex.Message);
            }
            return this;
        }


        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="username">用户名</param>
        /// <param name="password">密码</param>
        /// <returns></returns>
        public bool Login(string username,string password)
        {
            if (mDataClient != null)
            {
                try
                {
                    var re = mDataClient.Login(new LoginRequest() { Name = username, Password = password });
                    TimeOut = re.Timeout;
                    LoginTime = DateTime.FromBinary(re.Time).ToLocalTime();
                    mLoginId = re.Token;
                }
                catch
                {
                    return false;
                }
            }
            return false;
        }

        /// <summary>
        /// 登出
        /// </summary>
        public void Logout()
        {
            if(mDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    mDataClient.Logout(new LogoutRequest() { Token = mLoginId });
                    mLoginId = string.Empty;
                }
                catch
                {

                }
            }
        }

        /// <summary>
        /// 定时心跳，维持登录
        /// </summary>
        /// <returns></returns>
        public bool Hart()
        {
            try
            {
                if(mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mDataClient.Hart(new HartRequest() { Token = mLoginId }).Result;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 更新实时数据
        /// </summary>
        /// <param name="tag">变量</param>
        /// <param name="value">值</param>
        /// <param name="quality">质量</param>
        /// <returns></returns>
        public bool UpdateTagValue(string tag,object value,byte quality)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new DataRequest() { Token = mLoginId };
                    dr.Value.Add(new TagValue() { Tag = tag, Value = value.ToString(), Quality = quality });
                    return mDataClient.UpdateValue(dr).Result;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 更新实时数据
        /// </summary>
        /// <param name="values">值</param>
        /// <returns></returns>
        public bool UpdateTagValue(Dictionary<string,Tuple<object,byte>> values)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new DataRequest() { Token = mLoginId };
                    foreach(var vv in values)
                    dr.Value.Add(new TagValue() { Tag = vv.Key, Value = vv.Value.Item1.ToString(), Quality = vv.Value.Item2 });
                    return mDataClient.UpdateValue(dr).Result;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 更新实时数据
        /// </summary>
        /// <param name="values">值</param>
        /// <returns></returns>
        public bool UpdateTagValue(Dictionary<string, RealValuePoint> values)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new DataRequest() { Token = mLoginId };
                    foreach (var vv in values)
                        dr.Value.Add(new TagValue() { Tag = vv.Key, Value = vv.Value.Value.ToString(), Quality = vv.Value.Quality });
                    return mDataClient.UpdateValue(dr).Result;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 更新历史数据
        /// </summary>
        /// <param name="values">值</param>
        /// <returns></returns>
        public bool UpdateTagHisValue(Dictionary<string,List<Tuple<DateTime,object,byte>>> values)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new HisDataRequest() { Token = mLoginId };
                    foreach(var vv in values)
                    {
                        var vval = new TagHisDataValue();
                        vval.Tag = vv.Key;
                        vval.Value.AddRange(vv.Value.Select(e => new HisValue() { Time = e.Item1.ToBinary(), Value = e.Item2.ToString(), Quality = e.Item3 }));
                        dr.Value.Add(vval);
                    }
                    return mDataClient.UpdateHisValue(dr).Result;
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 更新历史数据
        /// </summary>
        /// <param name="values">值</param>
        /// <returns></returns>
        public bool UpdateTagHisValue(List<HisDataValueCollection> values)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new HisDataRequest() { Token = mLoginId };
                    foreach (var vv in values)
                    {
                        var vval = new TagHisDataValue();
                        vval.Tag = vv.Tag;
                        foreach (var vvv in vv)
                        {
                            vval.Value.AddRange(vv.Select(e => new HisValue() { Time = e.Time.ToBinary(), Value = e.Value.ToString(), Quality = e.Quality }));
                        }
                    }
                    return mDataClient.UpdateHisValue(dr).Result;
                }
            }
            catch
            {

            }
            return false;
        }
        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            mLoginId = string.Empty;
            grpcChannel.Dispose();
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class HisDataValueCollection: List<HisDataValuePoint>
    {

        #region ... Variables  ...

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public string Tag { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
    /// <summary>
    /// 
    /// </summary>
    public struct HisDataValuePoint
    {
        /// <summary>
        /// 
        /// </summary>
        public object Value { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public DateTime Time { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public byte Quality { get; set; }
    }


    public struct RealValuePoint
    {
        /// <summary>
        /// 
        /// </summary>
        public object Value { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public byte Quality { get; set; }
    }

}
