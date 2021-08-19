using Cdy.Tag;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Reflection;

namespace DBGrpcApi
{
    /// <summary>
    /// 
    /// </summary>
    public class Client:IDisposable
    {

        #region ... Variables  ...

        private string mLoginId = string.Empty;

        
        private RealData.RealDataClient mRealDataClient;

        private HislData.HislDataClient mHisDataClient;

        private Security.SecurityClient mSecurityClient;

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
                mRealDataClient = new RealData.RealDataClient(grpcChannel);

                mHisDataClient = new HislData.HislDataClient(grpcChannel);

                mSecurityClient = new Security.SecurityClient(grpcChannel);

            }
            catch (Exception ex)
            {
                LoggerService.Service.Erro("DevelopService", ex.Message);
            }
            return this;
        }


        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool Login(string username,string password)
        {
            if (mSecurityClient != null)
            {
                try
                {
                    var re = mSecurityClient.Login(new LoginRequest() { Name = username, Password = password });
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
            if(mSecurityClient!=null && !string.IsNullOrEmpty(mLoginId))
            {
                try
                {
                    mSecurityClient.Logout(new LogoutRequest() { Token = mLoginId });
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
                if(mSecurityClient!=null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mSecurityClient.Hart(new HartRequest() { Token = mLoginId }).Result;
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
        /// <param name="tag"></param>
        /// <returns></returns>
        private string GetGroupName(string tag)
        {
            if (tag.LastIndexOf(".") > 0)
            {
                return tag.Substring(0, tag.LastIndexOf("."));
            }
            return string.Empty;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        private Dictionary<string, List<string>> GetGroupTags(List<string> tags)
        {
            Dictionary<string, List<string>> groupedtags = new Dictionary<string, List<string>>();
            foreach (var vv in tags)
            {
                string grp = GetGroupName(vv);
                string tag = string.IsNullOrEmpty(grp) ? vv : vv.Substring(grp.Length+1);
                if (groupedtags.ContainsKey(grp))
                {
                    groupedtags[grp].Add(tag);
                }
                else
                {
                    groupedtags.Add(grp, new List<string>() { tag });
                }
            }
            return groupedtags;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        private object ConvertToValue(string value,int type)
        {
            var tp = (Cdy.Tag.TagType)type;
            switch (tp)
            {
                case TagType.Bool:
                    return Convert.ToBoolean(value);
                case TagType.Byte:
                    return Convert.ToByte(value);
                case TagType.DateTime:
                    return DateTime.Parse(value);
                case TagType.Double:
                    return double.Parse(value);
                case TagType.Float:
                    return float.Parse(value);
                case TagType.Int:
                    return Int32.Parse(value);
                case TagType.IntPoint:
                    string[] ss = value.Split(new char[] { ',' });
                    return new IntPointData(int.Parse(ss[0]), int.Parse(ss[1]));
                case TagType.IntPoint3:
                     ss = value.Split(new char[] { ',' });
                    return new IntPoint3Data(int.Parse(ss[0]), int.Parse(ss[1]), int.Parse(ss[2]));
                case TagType.Long:
                    return long.Parse(value);
                case TagType.LongPoint:
                     ss = value.Split(new char[] { ',' });
                    return new LongPointData(long.Parse(ss[0]), long.Parse(ss[1]));
                case TagType.LongPoint3:
                    ss = value.Split(new char[] { ',' });
                    return new LongPoint3Data(long.Parse(ss[0]), long.Parse(ss[1]), long.Parse(ss[2]));
                case TagType.Short:
                    return short.Parse(value);
                case TagType.String:
                    return value;
                case TagType.UInt:
                    return UInt32.Parse(value);
                case TagType.UIntPoint:
                     ss = value.Split(new char[] { ',' });
                    return new UIntPointData(uint.Parse(ss[0]), uint.Parse(ss[1]));
                case TagType.UIntPoint3:
                    ss = value.Split(new char[] { ',' });
                    return new UIntPoint3Data(uint.Parse(ss[0]), uint.Parse(ss[1]), uint.Parse(ss[2]));
                case TagType.ULong:
                    return ulong.Parse(value);
                case TagType.ULongPoint:
                    ss = value.Split(new char[] { ',' });
                    return new ULongPointData(ulong.Parse(ss[0]), ulong.Parse(ss[1]));
                case TagType.ULongPoint3:
                    ss = value.Split(new char[] { ',' });
                    return new ULongPoint3Data(ulong.Parse(ss[0]), ulong.Parse(ss[1]), ulong.Parse(ss[2]));
                case TagType.UShort:
                    return ushort.Parse(value);
            }
            return null;
        }

        /// <summary>
        /// 读取实时值
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        public Dictionary<string,object> ReadRealValue(List<string> tags)
        {
            try
            {
                Dictionary<string, object> re = new Dictionary<string, object>();
                var gtags = GetGroupTags(tags);
                foreach(var vv in gtags)
                {
                    var vvv = new GetRealValueRequest() { Group = vv.Key, Token = mLoginId };
                    vvv.TagNames.AddRange(vv.Value);
                    var res = mRealDataClient.GetRealValueOnly(vvv);
                    if(res.Result)
                    {
                        foreach(var val in res.Values)
                        {
                            string sname = vv.Value[val.Id];
                            if(!string.IsNullOrEmpty(vv.Key))
                            {
                                sname = vv.Key + "." + sname;
                            }
                            re.Add(sname, ConvertToValue(val.Value, val.ValueType));
                        }
                    }
                }
                return re;
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagIds"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public Dictionary<int, object> ReadRealValueById(List<int> tagIds,string group)
        {
            try
            {
                Dictionary<int, object> re = new Dictionary<int, object>();
                var gtags = group;
                var vvv = new GetRealValueByIdRequest() { Group = group, Token = mLoginId };
                vvv.Ids.AddRange(tagIds);
                var res = mRealDataClient.GetRealValueOnlyById(vvv);
                if (res.Result)
                {
                    foreach (var val in res.Values)
                    {
                        re.Add(val.Id, ConvertToValue(val.Value, val.ValueType));
                    }
                }
                return re;
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 读取实时值，质量戳
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        public Dictionary<string, Tuple<int, object>> ReadRealValueAndQuality(List<string> tags)
        {
            try
            {
                Dictionary<string, Tuple<int, object>> re = new Dictionary<string, Tuple<int, object>>();
                var gtags = GetGroupTags(tags);
                foreach (var vv in gtags)
                {
                    var vvv = new GetRealValueRequest() { Group = vv.Key, Token = mLoginId };
                    vvv.TagNames.AddRange(vv.Value);
                    var res = mRealDataClient.GetRealValueAndQuality(vvv);
                    if (res.Result)
                    {
                        foreach (var val in res.Values)
                        {
                            string sname = vv.Value[val.Id];
                            if (!string.IsNullOrEmpty(vv.Key))
                            {
                                sname = vv.Key + "." + sname;
                            }
                            re.Add(sname, new Tuple<int, object>(val.Quality, ConvertToValue(val.Value, val.ValueType)));
                        }
                    }
                }
                return re;
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagIds"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<int, object>> ReadRealValueAndQualityById(List<int> tagIds, string group)
        {
            try
            {
                Dictionary<int, Tuple<int, object>> re = new Dictionary<int, Tuple<int, object>>();
                var gtags = group;
                var vvv = new GetRealValueByIdRequest() { Group = group, Token = mLoginId };
                vvv.Ids.AddRange(tagIds);
                var res = mRealDataClient.GetRealValueAndQualityById(vvv);
                if (res.Result)
                {
                    foreach (var val in res.Values)
                    {
                        re.Add(val.Id, new Tuple<int, object>(val.Quality, ConvertToValue(val.Value, val.ValueType)));
                    }
                }
                return re;
            }
            catch
            {

            }
            return null;
        }


        /// <summary>
        /// 读取实时值，质量戳，时间
        /// </summary>
        /// <param name="tags"></param>
        /// <returns></returns>
        public Dictionary<string, Tuple<int,DateTime, object>> ReadRealValueAndQualityTime(List<string> tags)
        {
            try
            {
                Dictionary<string, Tuple<int, DateTime, object>> re = new Dictionary<string, Tuple<int, DateTime, object>>();
                var gtags = GetGroupTags(tags);
                foreach (var vv in gtags)
                {
                    var vvv = new GetRealValueRequest() { Group = vv.Key, Token = mLoginId };
                    vvv.TagNames.AddRange(vv.Value);
                    var res = mRealDataClient.GetRealValue(vvv);
                    if (res.Result)
                    {
                        foreach (var val in res.Values)
                        {
                            string sname = vv.Value[val.Id];
                            if (!string.IsNullOrEmpty(vv.Key))
                            {
                                sname = vv.Key + "." + sname;
                            }
                            re.Add(sname, new Tuple<int, DateTime, object>(val.Quality,DateTime.FromBinary(val.Time), ConvertToValue(val.Value, val.ValueType)));
                        }
                    }
                }
                return re;
            }
            catch
            {

            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tagIds"></param>
        /// <param name="group"></param>
        /// <returns></returns>
        public Dictionary<int, Tuple<int, DateTime, object>> ReadRealValueAndQualityTimeById(List<int> tagIds, string group)
        {
            try
            {
                Dictionary<int, Tuple<int, DateTime, object>> re = new Dictionary<int, Tuple<int, DateTime, object>>();
                var gtags = group;
                var vvv = new GetRealValueByIdRequest() { Group = group, Token = mLoginId };
                vvv.Ids.AddRange(tagIds);
                var res = mRealDataClient.GetRealValueById(vvv);
                if (res.Result)
                {
                    foreach (var val in res.Values)
                    {
                        re.Add(val.Id, new Tuple<int, DateTime,object>(val.Quality, DateTime.FromBinary(val.Time), ConvertToValue(val.Value, val.ValueType)));
                    }
                }
                return re;
            }
            catch
            {

            }
            return null;
        }
        
        /// <summary>
        /// 读取历史记录
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="starttime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> ReadHisValue(List<string> tags,DateTime starttime,DateTime endTime,TimeSpan duration,Cdy.Tag.QueryValueMatchType type)
        {
            if(mHisDataClient!=null && !string.IsNullOrEmpty(mLoginId))
            {
                Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                var req = new HisDataRequest() { StartTime = starttime.ToBinary(), EndTime = endTime.ToBinary(), Duration = (int)duration.TotalMilliseconds, QueryType = (int)type, Token = mLoginId };
                req.Tags.AddRange(tags);
                var res = mHisDataClient.GetHisValue(req);
                if(res.Result)
                {
                    foreach(var vv in res.Values)
                    {
                        HisDataValueCollection hvd = new HisDataValueCollection() { ValueType = vv.ValueType };
                        foreach(var vvv in vv.Values)
                        {
                            hvd.Add(new HisDataValuePoint() { Time = DateTime.FromBinary(vvv.Time), Value = ConvertToValue(vvv.Value, vv.ValueType) });
                        }
                        re.Add(vv.Tag, hvd);
                    }
                }
                return re;
            }
            return null;
        }


        /// <summary>
        /// 读取记录的所有值
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="starttime"></param>
        /// <param name="endTime"></param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> ReadAllHisValue(List<string> tags, DateTime starttime, DateTime endTime)
        {
            if (mHisDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                var req = new AllHisDataRequest() { StartTime = starttime.ToBinary(), EndTime = endTime.ToBinary(), Token = mLoginId };
                req.Tags.AddRange(tags);
                var res = mHisDataClient.GetAllHisValue(req);
                if (res.Result)
                {
                    foreach (var vv in res.Values)
                    {
                        HisDataValueCollection hvd = new HisDataValueCollection() { ValueType = vv.ValueType };
                        foreach (var vvv in vv.Values)
                        {
                            hvd.Add(new HisDataValuePoint() { Time = DateTime.FromBinary(vvv.Time), Value = ConvertToValue(vvv.Value, vv.ValueType) });
                        }
                        re.Add(vv.Tag, hvd);
                    }
                }
                return re;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tags"></param>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <returns></returns>
        public Dictionary<string,StatisticsValueCollection > ReadStatisticsValue(List<string> tags,DateTime starttime,DateTime endtime)
        {
            if (mHisDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                Dictionary<string, StatisticsValueCollection> re = new Dictionary<string, StatisticsValueCollection>();
                var req = new NumberValueStatisticsDataRequest() { StartTime = starttime.ToBinary(), EndTime = endtime.ToBinary(), Token = mLoginId };
                req.Tags.AddRange(tags);
                var res = mHisDataClient.GetNumberValueStatisticsData(req);
                if (res.Result)
                {
                    foreach (var vv in res.Values)
                    {
                        StatisticsValueCollection hvd = new StatisticsValueCollection();
                        foreach (var vvv in vv.Values)
                        {
                            hvd.Add(new StatisticsValue() { Time = DateTime.FromBinary(vvv.Time),AvgValue=vvv.AvgValue,MaxValue=vvv.MaxValue,MaxValueTime=DateTime.FromBinary(vvv.MaxTime),MinValue=vvv.MinValue,MinValueTime=DateTime.FromBinary(vvv.MinTime) });
                        }
                        re.Add(vv.Tag, hvd);
                    }
                }
                return re;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public bool SetTagValue(string tag,object value)
        {
            if(mRealDataClient!=null && !string.IsNullOrEmpty(mLoginId))
            {
                SetRealValueRequest svr = new SetRealValueRequest() { Token = mLoginId };
                svr.Values.Add(new SetRealValueItem() { TagName = tag, Value = value.ToString() });
                var res = mRealDataClient.SetRealValue(svr);
                return res.Result;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            mLoginId = string.Empty;
            mHisDataClient = null;
            mRealDataClient = null;
            mSecurityClient = null;
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
        public int ValueType { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public class StatisticsValueCollection : List<StatisticsValue>
    {
    
    }

    /// <summary>
    /// 
    /// </summary>
    public struct StatisticsValue
    {
        public DateTime Time { get; set; }
        public double AvgValue { get; set; }
        public DateTime MaxValueTime { get; set; }
        public double MaxValue { get; set; }

        public DateTime MinValueTime { get; set; }

        public double MinValue { get; set; }
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
    }

}
