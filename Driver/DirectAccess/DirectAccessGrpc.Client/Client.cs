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
        /// 获取某个时间段内记录的所有值
        /// </summary>
        /// <param name="tag">变量集合/param>
        /// <param name="stime">开始时间</param>
        /// <param name="etime">结束时间</param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> GetAllHisValue(IEnumerable<string> tags, DateTime stime,DateTime etime)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new QueryAllHisDataRequest() { Token = mLoginId,StartTime=stime.Ticks,EndTime=etime.Ticks };
                    dr.Tags.AddRange(tags);
                    Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                     var res = mDataClient.GetAllHisValue(dr);
                    if(res != null && res.Result)
                    {
                        foreach(var vv in res.Values)
                        {
                            HisDataValueCollection hvc = new HisDataValueCollection();
                            hvc.AddRange(vv.Values.Select(e => new HisDataValuePoint() { Quality = (byte)e.Quality, Time = DateTime.FromBinary(e.Time), Value = e.Value }));
                            re.Add(vv.Tag, hvc);
                        }
                    }
                    return re;
                }
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 在某个时间段，查询一些列时间间隔上时间对应的值
        /// </summary>
        /// <param name="tag">变量集合</param>
        /// <param name="stime">开始时间</param>
        /// <param name="etime">结束时间</param>
        /// <param name="timespan">时间间隔（毫秒）</param>
        /// <param name="queryType">拟合类型(0：取前一个值，1： 取后一个值，2：取较近的值 ，3：线性插值</param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> GetHisValue(IEnumerable<string> tags, DateTime stime, DateTime etime,int timespan,byte queryType)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new QueryHisDataRequest() { Token = mLoginId, StartTime = stime.Ticks, EndTime = etime.Ticks,Duration = timespan,QueryType=queryType  };
                    dr.Tags.AddRange(tags);
                    Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                    var res = mDataClient.GetHisValue(dr);
                    if (res != null && res.Result)
                    {
                        foreach (var vv in res.Values)
                        {
                            HisDataValueCollection hvc = new HisDataValueCollection();
                            hvc.AddRange(vv.Values.Select(e => new HisDataValuePoint() { Quality = (byte)e.Quality, Time = DateTime.FromBinary(e.Time), Value = e.Value }));
                            re.Add(vv.Tag, hvc);
                        }
                    }
                    return re;
                }
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 在某个时间段，查询一些列时间间隔上时间对应的值，数据拟合时忽略系统退出时的影响
        /// </summary>
        /// <param name="tag">变量集合</param>
        /// <param name="stime">开始时间</param>
        /// <param name="etime">结束时间</param>
        /// <param name="timespan">时间间隔（毫秒）</param>
        /// <param name="queryType">拟合类型(0：取前一个值，1： 取后一个值，2：取较近的值 ，3：线性插值</param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> GetHisValueByIgnorSystemExit(IEnumerable<string> tags, DateTime stime, DateTime etime, int timespan, byte queryType)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new QueryHisDataRequest() { Token = mLoginId, StartTime = stime.Ticks, EndTime = etime.Ticks, Duration = timespan, QueryType = queryType };
                    dr.Tags.AddRange(tags);
                    Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                    var res = mDataClient.GetHisValueIgnorSystemExit(dr);
                    if (res != null && res.Result)
                    {
                        foreach (var vv in res.Values)
                        {
                            HisDataValueCollection hvc = new HisDataValueCollection();
                            hvc.AddRange(vv.Values.Select(e => new HisDataValuePoint() { Quality = (byte)e.Quality, Time = DateTime.FromBinary(e.Time), Value = e.Value }));
                            re.Add(vv.Tag, hvc);
                        }
                    }
                    return re;
                }
            }
            catch
            {

            }
            return null;
        }


        /// <summary>
        /// 些列时间点上时间对应的值
        /// </summary>
        /// <param name="tag">变量集合</param>
        /// <param name="times">时间集合</param>
        /// <param name="queryType">拟合类型(0：取前一个值，1： 取后一个值，2：取较近的值 ，3：线性插值</param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> GetHisValueAtTimePoint(IEnumerable<string> tags, IEnumerable<DateTime> times, byte queryType)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new QueryHisDataAtTimesRequest() { Token = mLoginId,  QueryType = queryType };
                    dr.Times.AddRange(times.Select(e => e.Ticks));
                    dr.Tags.AddRange(tags);
                    Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                    var res = mDataClient.GetHisValueAtTimes(dr);
                    if (res != null && res.Result)
                    {
                        foreach (var vv in res.Values)
                        {
                            HisDataValueCollection hvc = new HisDataValueCollection();
                            hvc.AddRange(vv.Values.Select(e => new HisDataValuePoint() { Quality = (byte)e.Quality, Time = DateTime.FromBinary(e.Time), Value = e.Value }));
                            re.Add(vv.Tag, hvc);
                        }
                    }
                    return re;
                }
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 些列时间点上时间对应的值,数据拟合时忽略系统退出时的影响
        /// </summary>
        /// <param name="tag">变量集合</param>
        /// <param name="times">时间集合</param>
        /// <param name="queryType">拟合类型(0：取前一个值，1： 取后一个值，2：取较近的值 ，3：线性插值</param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> GetHisValueAtTimePointByIgnorSystemExit(IEnumerable<string> tags, IEnumerable<DateTime> times, byte queryType)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new QueryHisDataAtTimesRequest() { Token = mLoginId, QueryType = queryType };
                    dr.Times.AddRange(times.Select(e => e.Ticks));
                    dr.Tags.AddRange(tags);
                    Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                    var res = mDataClient.GetHisValueAtTimesIgnorSystemExit(dr);
                    if (res != null && res.Result)
                    {
                        foreach (var vv in res.Values)
                        {
                            HisDataValueCollection hvc = new HisDataValueCollection();
                            hvc.AddRange(vv.Values.Select(e => new HisDataValuePoint() { Quality = (byte)e.Quality, Time = DateTime.FromBinary(e.Time), Value = e.Value }));
                            re.Add(vv.Tag, hvc);
                        }
                    }
                    return re;
                }
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 获取变量的实时值
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        public Dictionary<string, RealValuePoint> GetTagRealValue(List<string> tags)
        {
            try
            {
                if (mDataClient != null && !string.IsNullOrEmpty(mLoginId))
                {
                    var dr = new GetRealValueRequest() { Token = mLoginId };
                    dr.TagNames.AddRange(tags);
                    Dictionary<string, RealValuePoint> re = new Dictionary<string, RealValuePoint>();
                    var res = mDataClient.GetRealValue(dr);
                    if (res != null && res.Result)
                    {
                        foreach (var vv in res.Values)
                        {
                            re.Add(tags[vv.Id], new RealValuePoint() { Value = ConvertValue((byte)vv.ValueType, vv.Value), Quality = (byte)vv.Quality });
                        }
                    }
                    return re;
                }
            }
            catch
            {

            }
            return null;
        }

        private object ConvertValue(byte type,string value)
        {
            switch (type)
            {
                case 0:
                    return bool.Parse(value);
                case 1:
                    return Convert.ToByte(value);
                case 2:
                    return Convert.ToInt16(value);
                case 3:
                    return Convert.ToUInt16(value);
                case 4:
                    return Convert.ToInt32(value);
                case 5:
                    return Convert.ToUInt32(value);
                case 6:
                    return Convert.ToInt64(value);
                case 7:
                    return Convert.ToUInt64(value);
                case 8:
                    return Convert.ToDouble(value);
                case 9:
                    return Convert.ToSingle(value);
                case 10:
                    return DateTime.Parse(value);
                case 11:
                    return value;
                default:
                    return value;
                
            }
            return value;
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
