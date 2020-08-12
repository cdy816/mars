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
    public class Client
    {

        #region ... Variables  ...

        private string mLoginId = string.Empty;

        
        private RealData.RealDataClient mRealDataClient;

        private HislData.HislDataClient mHisDataClient;

        private Security.SecurityClient mSecurityClient;

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

        public string Ip { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int Port { get; set; }

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

                Grpc.Net.Client.GrpcChannel grpcChannel = Grpc.Net.Client.GrpcChannel.ForAddress(@"https://" + Ip + ":"+ Port, new GrpcChannelOptions { HttpClient = httpClient });

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
                    mLoginId = mSecurityClient.Login(new LoginRequest() { Name = username, Password = password }).Token;
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


        private string GetGroupName(string tag)
        {
            if (tag.LastIndexOf(".") > 0)
            {
                return tag.Substring(0, tag.LastIndexOf(".") - 1);
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
                string tag = string.IsNullOrEmpty(grp) ? vv : vv.Substring(grp.Length);
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
