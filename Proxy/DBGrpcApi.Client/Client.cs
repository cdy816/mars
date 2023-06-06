using Cdy.Tag;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Reflection;
using System.Xml.Linq;

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

        private TagInfo.TagInfoClient mTagInfoClient;

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
                mRealDataClient = new RealData.RealDataClient(grpcChannel);

                mHisDataClient = new HislData.HislDataClient(grpcChannel);

                mSecurityClient = new Security.SecurityClient(grpcChannel);

                mTagInfoClient = new TagInfo.TagInfoClient(grpcChannel);

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
        /// <param name="username">用户名</param>
        /// <param name="password">密码</param>
        /// <returns></returns>
        public bool Login(string username,string password)
        {
            if (mSecurityClient != null)
            {
                try
                {
                    var re = mSecurityClient.Login(new LoginRequest() { Name = username, Password = password,ApplicationCode=Cdy.Tag.Common.Common.Md5Helper.CalSha1() });
                    TimeOut = re.Timeout;
                    LoginTime = DateTime.FromBinary(re.Time).ToLocalTime();
                    mLoginId = re.Token;
                    return true;
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
        public bool Heart()
        {
            try
            {
                if(mSecurityClient!=null && !string.IsNullOrEmpty(mLoginId))
                {
                    return mSecurityClient.Heart(new HeartRequest() { Token = mLoginId,Time = DateTime.UtcNow.Ticks }).Result;
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
        /// 
        /// </summary>
        /// <param name="vals"></param>
        /// <returns></returns>
        private ComplexValues ParseComplexValues(string vals)
        {
            ComplexValues re = new ComplexValues();
            foreach (var vv in vals.Split(";"))
            {
                if (string.IsNullOrEmpty(vv)) continue;
                var vvv = vv.Split(":");
                int id = int.Parse(vvv[0]);
                byte valType = byte.Parse(vvv[1]);
                var val = ConvertToValue(vvv[2], valType);
                var qua = byte.Parse(vvv[3]);
                var tim = DateTime.FromBinary(long.Parse(vvv[4]));
                re.Add(id,new RealTagValueWithTimer() { Id = id, Quality = qua, Value = val, ValueType = valType,Time=tim });
            }
            return re;
        }

        /// <summary>
        /// 读取实时值
        /// </summary>
        /// <param name="tags">变量名称集合</param>
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
                            if (val.ValueType == (byte)TagType.Complex)
                            {
                                re.Add(sname, ParseComplexValues(val.Value));
                            }
                            else
                            {
                                re.Add(sname, ConvertToValue(val.Value, val.ValueType));
                            }
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
        /// 读取变量的状态
        /// </summary>
        /// <param name="tags">变量名称集合</param>
        /// <returns></returns>
        public Dictionary<string, short> ReadTagState(List<string> tags)
        {
            try
            {
                Dictionary<string, short> re = new Dictionary<string, short>();
                var gtags = GetGroupTags(tags);
                foreach (var vv in gtags)
                {
                    var vvv = new GetTagStateRequest() { Group = vv.Key, Token = mLoginId };
                    vvv.TagNames.AddRange(vv.Value);
                    var res = mRealDataClient.GetTagState(vvv);
                    if (res.Result)
                    {
                        foreach (var val in res.Values)
                        {
                            string sname = val.TagName;
                            if (!string.IsNullOrEmpty(vv.Key)&&sname.IndexOf(".")<0)
                            {
                                sname = vv.Key + "." + sname;
                            }
                            re.Add(sname, (short)val.Value);
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
        /// 读取变量的扩展属性2
        /// </summary>
        /// <param name="tags">变量名称集合</param>
        /// <returns></returns>
        public Dictionary<string, long> ReadTagExtendField2(List<string> tags)
        {
            try
            {
                Dictionary<string, long> re = new Dictionary<string, long>();
                var gtags = GetGroupTags(tags);
                foreach (var vv in gtags)
                {
                    var vvv = new GetTagStateRequest() { Group = vv.Key, Token = mLoginId };
                    vvv.TagNames.AddRange(vv.Value);
                    var res = mRealDataClient.GetTagExtendField2(vvv);
                    if (res.Result)
                    {
                        foreach (var val in res.Values)
                        {
                            string sname = val.TagName;
                            if (!string.IsNullOrEmpty(vv.Key) && sname.IndexOf(".") < 0)
                            {
                                sname = vv.Key + "." + sname;
                            }
                            re.Add(sname, val.Value);
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
        /// 读取实时值
        /// </summary>
        /// <param name="tagIds">变量ID集合</param>
        /// <param name="group">请求变量所在的组名</param>
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
                        if (val.ValueType == (byte)TagType.Complex)
                        {
                            re.Add(val.Id, ParseComplexValues(val.Value));
                        }
                        else
                        {
                            re.Add(val.Id, ConvertToValue(val.Value, val.ValueType));
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
        /// <param name="tags">变量名称集合</param>
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
                            if (val.ValueType == (byte)TagType.Complex)
                            {
                                re.Add(sname, new Tuple<int, object>(val.Quality, ParseComplexValues(val.Value)));
                            }
                            else
                            {
                                re.Add(sname, new Tuple<int, object>(val.Quality, ConvertToValue(val.Value, val.ValueType)));
                            }
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
        /// <param name="tagIds">变量ID集合</param>
        /// <param name="group">请求变量所在的组名</param>
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
                        if (val.ValueType == (byte)TagType.Complex)
                        {
                            re.Add(val.Id, new Tuple<int, object>(val.Quality, ParseComplexValues(val.Value)));
                        }
                        else
                        {
                            re.Add(val.Id, new Tuple<int, object>(val.Quality, ConvertToValue(val.Value, val.ValueType)));
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
        /// <param name="tags">变量名称集合</param>
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
                            if (val.ValueType == (byte)TagType.Complex)
                            {
                                re.Add(sname, new Tuple<int,DateTime, object>(val.Quality, DateTime.FromBinary(val.Time), ParseComplexValues(val.Value)));
                            }
                            else
                            {
                                re.Add(sname, new Tuple<int, DateTime, object>(val.Quality, DateTime.FromBinary(val.Time).ToLocalTime(), ConvertToValue(val.Value, val.ValueType)));
                            }
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
        /// <param name="tagIds">变量ID集合</param>
        /// <param name="group">请求变量所在的组名</param>
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
                        if (val.ValueType == (byte)TagType.Complex)
                        {
                            re.Add(val.Id, new Tuple<int, DateTime, object>(val.Quality, DateTime.FromBinary(val.Time), ParseComplexValues(val.Value)));
                        }
                        else
                            re.Add(val.Id, new Tuple<int, DateTime,object>(val.Quality, DateTime.FromBinary(val.Time).ToLocalTime(), ConvertToValue(val.Value, val.ValueType)));
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
        /// <param name="tags">变量名称集合</param>
        /// <param name="starttime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="duration">时间间隔</param>
        /// <param name="type">值拟合类型<see cref="QueryValueMatchType"/></param>
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
        /// 读取历史记录,忽略系统退出对数据拟合的影响
        /// </summary>
        /// <param name="tags">变量名称集合</param>
        /// <param name="starttime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="duration">时间间隔</param>
        /// <param name="type">值拟合类型<see cref="QueryValueMatchType"/></param>
        /// <returns></returns>
        public Dictionary<string, HisDataValueCollection> ReadHisValueByIgnorSystemExit(List<string> tags, DateTime starttime, DateTime endTime, TimeSpan duration, Cdy.Tag.QueryValueMatchType type)
        {
            if (mHisDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                Dictionary<string, HisDataValueCollection> re = new Dictionary<string, HisDataValueCollection>();
                var req = new HisDataRequest() { StartTime = starttime.ToBinary(), EndTime = endTime.ToBinary(), Duration = (int)duration.TotalMilliseconds, QueryType = (int)type, Token = mLoginId };
                req.Tags.AddRange(tags);
                var res = mHisDataClient.GetHisValueIgnorSystemExit(req);
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
        /// 根据SQL语句查询变量的值
        /// </summary>
        /// <param name="sql">Sql 语句</param>
        /// <returns>历史数据表List<List<string>>、统计结果string[]、实时数据值List<RealTagValueWithTimer2> </returns>
        public object ReadValueBySql(string sql)
        {
            if (mHisDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                var res = mHisDataClient.QueryDataBySql(new QueryBySqlRequest() { Sql = sql, Token = mLoginId });
                if(res!=null)
                {
                    if(res.ValueType==0)
                    {
                        //表格形式,第一行数据表格头
                        List<List<string>> vals = new List<List<string>>();
                        foreach(var vv in res.Value.Rows)
                        {
                            vals.Add(vv.Columns.ToList());
                        }
                        return vals;
                    }
                    else if(res.ValueType==1)
                    {
                        //列表形式
                        if(res.Value.Rows.Count>0)
                        {
                            return res.Value.Rows[0].Columns.ToArray();
                        }
                        else
                        {
                            return null;
                        }
                    }
                    else if(res.ValueType==2)
                    {
                        //实时数据
                        List<RealTagValueWithTimer2> vals = new List<RealTagValueWithTimer2>();
                        foreach (var vv in res.Value.Rows)
                        {
                            RealTagValueWithTimer2 vvv = new RealTagValueWithTimer2() { TagName = vv.Columns[0] };
                            vvv.Value = vv.Columns[1];
                            vvv.Quality = byte.Parse(vv.Columns[2]);
                            vvv.Time = DateTime.Parse(vv.Columns[3]);
                            vals.Add(vvv);
                        }
                        return vals;
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// 读取记录的所有值
        /// </summary>
        /// <param name="tags">变量名称集合</param>
        /// <param name="starttime">开始时间</param>
        /// <param name="endTime">结束时间</param>
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
        /// 请求统计值
        /// </summary>
        /// <param name="tags">变量名称集合</param>
        /// <param name="starttime">开始时间</param>
        /// <param name="endtime">结束时间</param>
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
        /// 设置变量的值
        /// </summary>
        /// <param name="tag">变量名称</param>
        /// <param name="value">值</param>
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
        /// 设置变量的值
        /// </summary>
        /// <param name="value">变量名称和值的集合</param>
        /// <returns></returns>
        public bool SetTagValue(Dictionary<string,string> values)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                SetRealValueRequest svr = new SetRealValueRequest() { Token = mLoginId };
                foreach(var vv in values)
                svr.Values.Add(new SetRealValueItem() { TagName = vv.Key, Value = vv.Value });
                var res = mRealDataClient.SetRealValue(svr);
                return res.Result;
            }
            return false;
        }

        /// <summary>
        /// 设置变量的状态
        /// </summary>
        /// <param name="value">变量名称和值的集合</param>
        /// <returns></returns>
        public bool SetTagState(Dictionary<string, short> values)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                SetTagStateRequest svr = new SetTagStateRequest() { Token = mLoginId };
                foreach (var vv in values)
                    svr.Values.Add(new IntValueItem() { TagName = vv.Key, Value = vv.Value });
                var res = mRealDataClient.SetTagState(svr);
                return res.Result;
            }
            return false;
        }


        /// <summary>
        /// 设置变量的扩展属性2
        /// </summary>
        /// <param name="value">变量名称和值的集合</param>
        /// <returns></returns>
        public bool SetTagExtendField2(Dictionary<string, long> values)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                SetTagExtendField2Request svr = new SetTagExtendField2Request() { Token = mLoginId };
                foreach (var vv in values)
                    svr.Values.Add(new Int64ValueItem() { TagName = vv.Key, Value = vv.Value });
                var res = mRealDataClient.SetTagExtendField2(svr);
                return res.Result;
            }
            return false;
        }

        /// <summary>
        /// 查找某个值对应的时间
        /// </summary>
        /// <param name="tag">变量名称</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type">数值型变量值比较方式<see cref="NumberStatisticsType"/></param>
        /// <param name="value">值</param>
        /// <param name="interval">偏差,默认:0</param>
        /// <returns></returns>
        public Tuple<DateTime, object> FindTagValue(string tag, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, double interval=0)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                FindTagValueRequest req = new FindTagValueRequest() { Token = mLoginId, CompareType = (byte)type, StartTime = startTime.Ticks, EndTime = endTime.Ticks, Interval = interval.ToString(), Tag = tag, Value = value.ToString() };
                var res = mHisDataClient.FindTagValue(req);
                if(res!=null)
                {
                    if(res.Value.Count>0)
                    return new Tuple<DateTime, object>( DateTime.FromBinary(res.Time[0]), res.Value[0]);
                    else
                    {
                        return new Tuple<DateTime, object>(DateTime.FromBinary(res.Time[0]), value);
                    }
                }
            }
            return null;
        }

        /// <summary>
        /// 查找某个值保持的时间
        /// </summary>
        /// <param name="tag">变量名称</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type">数值型变量值比较方式<see cref="NumberStatisticsType"/></param>
        /// <param name="value">值</param>
        /// <param name="interval">偏差,默认:0</param>
        /// <returns>累计时间(单位：秒)</returns>
        public double? CalTagValueKeepTime(string tag, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                FindTagValueRequest req = new FindTagValueRequest() { Token = mLoginId, CompareType = (byte)type, StartTime = startTime.Ticks, EndTime = endTime.Ticks, Interval = interval.ToString(), Tag = tag, Value = value.ToString() };
                var res = mHisDataClient.CalTagValueKeepTime(req);
                if (res != null)
                {
                    return res.Values;
                }
            }
            return null;
        }

        /// <summary>
        /// 查找某个值对应的时间
        /// </summary>
        /// <param name="tag">变量名称</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <param name="type">数值型变量值比较方式<see cref="NumberStatisticsType"/></param>
        /// <param name="value">值</param>
        /// <param name="interval">偏差,默认:0</param>
        /// <returns></returns>
        public Dictionary<DateTime, object> FindTagValues(string tag, DateTime startTime, DateTime endTime, NumberStatisticsType type, object value, object interval)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                Dictionary<DateTime, object> re = new Dictionary<DateTime, object>();
                FindTagValueRequest req = new FindTagValueRequest() { Token = mLoginId, CompareType = (byte)type, StartTime = startTime.Ticks, EndTime = endTime.Ticks, Interval = interval.ToString(), Tag = tag, Value = value.ToString() };
                var res = mHisDataClient.FindTagValues(req);
                if (res != null)
                {
                    if (res.Value.Count > 0)
                    {
                        for (int i = 0; i < res.Value.Count; i++)
                            re.Add(DateTime.FromBinary(res.Time[i]), res.Value[i]);
                    }
                    else
                    {
                        for (int i = 0; i < res.Value.Count; i++)
                            re.Add(DateTime.FromBinary(res.Time[i]), value);
                    }
                }
                return re;
            }
            return null;
        }

        /// <summary>
        /// 查找最大值
        /// </summary>
        /// <param name="tag">变量名称</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <returns></returns>
        public Tuple<double,List<DateTime>> FindNumberTagMaxValue(string tag,DateTime startTime,DateTime endTime)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                FindTagValueRequest req = new FindTagValueRequest() { Token = mLoginId, CompareType = (byte)NumberStatisticsType.Max, StartTime = startTime.Ticks, EndTime = endTime.Ticks, Interval = "0", Tag = tag, Value = "0" };
                var res = mHisDataClient.FindNumberTagMaxValue(req);
                if (res != null)
                {
                    List<DateTime> dt = new List<DateTime>();
                    foreach(var vv in res.Times)
                    {
                        dt.Add(DateTime.FromBinary(vv));
                    }
                    return new Tuple<double, List<DateTime>>(res.Values, dt);
                }
            }
            return null;
        }

        /// <summary>
        /// 查找最小值
        /// </summary>
        /// <param name="tag">变量名称</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <returns></returns>
        public Tuple<double, List<DateTime>> FindNumberTagMinValue(string tag, DateTime startTime, DateTime endTime)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                FindTagValueRequest req = new FindTagValueRequest() { Token = mLoginId, CompareType = (byte)NumberStatisticsType.Min, StartTime = startTime.Ticks, EndTime = endTime.Ticks, Interval = "0", Tag = tag, Value = "0" };
                var res = mHisDataClient.FindNumberTagMaxValue(req);
                if (res != null)
                {
                    List<DateTime> dt = new List<DateTime>();
                    foreach (var vv in res.Times)
                    {
                        dt.Add(DateTime.FromBinary(vv));
                    }
                    return new Tuple<double, List<DateTime>>(res.Values, dt);
                }
            }
            return null;
        }

        /// <summary>
        /// 计算平均值
        /// </summary>
        /// <param name="tag">变量名称</param>
        /// <param name="startTime">开始时间</param>
        /// <param name="endTime">结束时间</param>
        /// <returns></returns>
        public double FindNumberTagAvgValue(string tag, DateTime startTime, DateTime endTime)
        {
            if (mRealDataClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                FindTagValueRequest req = new FindTagValueRequest() { Token = mLoginId, CompareType = (byte)NumberStatisticsType.Min, StartTime = startTime.Ticks, EndTime = endTime.Ticks, Interval = "0", Tag = tag, Value = "0" };
                var res = mHisDataClient.CalNumberTagAvgValue(req);
                if (res != null)
                {
                    return res.Values;
                }
            }
            return double.MinValue;
        }

        /// <summary>
        /// 枚举所有变量组
        /// </summary>
        /// <returns></returns>
        public List<string> ListTagGroups()
        {
            if(mTagInfoClient!=null&&!string.IsNullOrEmpty(mLoginId))
            {
                List<string> rre = new List<string>();
                var re = mTagInfoClient.ListTagGroup(new ListTagGroupRequest() { Token = mLoginId }).Group;
                if(re!=null)
                {
                    rre.AddRange(re);
                }
                return rre;
            }
            return null;
        }

        /// <summary>
        /// 根据变量名称获取变量配置
        /// </summary>
        /// <param name="tagnames">变量名称的集合</param>
        /// <returns></returns>
        public List<Tagbase> GetTagByNameRequest(IEnumerable<string> tagnames)
        {
            if (mTagInfoClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                List<Tagbase> rre = new List<Tagbase>();
                var vv = new DBGrpcApi.GetTagByNameRequest() { Token = mLoginId };
                vv.TagNames.AddRange(tagnames);

                var re = mTagInfoClient.GetTagByName(vv).Tags;
                if (re != null)
                {
                    foreach(var vvv in re)
                    {
                        rre.Add(ConvertTo(vvv));
                    }
                }
                return rre;
            }
            return null;
        }

        /// <summary>
        /// 通过变量组获取变量配置
        /// </summary>
        /// <param name="group">变量组</param>
        /// <returns></returns>
        public List<Tagbase> GetTagByGroup(string group)
        {
            if (mTagInfoClient != null && !string.IsNullOrEmpty(mLoginId))
            {
                List<Tagbase> rre = new List<Tagbase>();
                var vv = new DBGrpcApi.GetTagByGroupRequest() { Token = mLoginId,Group=group };

                var re = mTagInfoClient.GetTagByGroup(vv).Tags;
                if (re != null)
                {
                    foreach (var vvv in re)
                    {
                        rre.Add(ConvertTo(vvv));
                    }
                }
                return rre;
            }
            return null;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <returns></returns>
        private Tagbase ConvertTo(TagBase tag)
        {
            Cdy.Tag.TagType tp = (TagType) Enum.Parse(typeof(Cdy.Tag.TagType), tag.Type);
            Cdy.Tag.Tagbase re=null;
            switch (tp)
            {
                case TagType.Bool:
                    re = new Cdy.Tag.BoolTag();
                    break;
                case TagType.Byte:
                    re = new Cdy.Tag.ByteTag();
                    break;
                case TagType.DateTime:
                    re = new Cdy.Tag.DateTimeTag();
                    break;
                case TagType.String:
                    re = new Cdy.Tag.StringTag();
                    break;
                case TagType.IntPoint3:
                    re = new Cdy.Tag.IntPoint3Tag();
                    break;
                case TagType.IntPoint:
                    re = new Cdy.Tag.IntPointTag();
                    break;
                case TagType.UIntPoint3:
                    re = new Cdy.Tag.UIntPoint3Tag();
                    break;
                case TagType.UIntPoint:
                    re = new Cdy.Tag.UIntPointTag();
                    break;
                case TagType.LongPoint3:
                    re = new Cdy.Tag.LongPoint3Tag();
                    break;
                case TagType.LongPoint:
                    re = new Cdy.Tag.LongPointTag();
                    break;
                case TagType.ULongPoint3:
                    re = new Cdy.Tag.ULongPoint3Tag();
                    break;
                case TagType.ULongPoint:
                    re = new Cdy.Tag.ULongPointTag();
                    break;
                case TagType.Double:
                    re = new Cdy.Tag.DoubleTag();
                    (re as Cdy.Tag.DoubleTag).MaxValue = double.Parse(tag.MaxValue);
                    (re as Cdy.Tag.DoubleTag).MinValue = double.Parse(tag.MinValue);
                    (re as Cdy.Tag.DoubleTag).Precision = byte.Parse(tag.Precision);
                    break;
                case TagType.Float:
                    re = new Cdy.Tag.FloatTag();
                    (re as Cdy.Tag.FloatTag).MaxValue = double.Parse(tag.MaxValue);
                    (re as Cdy.Tag.FloatTag).MinValue = double.Parse(tag.MinValue);
                    (re as Cdy.Tag.FloatTag).Precision = byte.Parse(tag.Precision);
                    break;
                case TagType.Int:
                    re = new Cdy.Tag.IntTag();
                    (re as Cdy.Tag.IntTag).MaxValue = double.Parse(tag.MaxValue);
                    (re as Cdy.Tag.IntTag).MinValue = double.Parse(tag.MinValue);
                    break;
                case TagType.UInt:
                    re = new Cdy.Tag.UIntTag();
                    (re as Cdy.Tag.UIntTag).MaxValue = double.Parse(tag.MaxValue);
                    (re as Cdy.Tag.UIntTag).MinValue = double.Parse(tag.MinValue);
                    break;
                case TagType.Long:
                    re = new Cdy.Tag.LongTag();
                    (re as Cdy.Tag.LongTag).MaxValue = double.Parse(tag.MaxValue);
                    (re as Cdy.Tag.LongTag).MinValue = double.Parse(tag.MinValue);
                    break;
                case TagType.ULong:
                    re = new Cdy.Tag.ULongTag();
                    (re as Cdy.Tag.ULongTag).MaxValue = double.Parse(tag.MaxValue);
                    (re as Cdy.Tag.ULongTag).MinValue = double.Parse(tag.MinValue);
                    break;
                case TagType.Complex:
                    re = new Cdy.Tag.ComplexTag();
                    break;
            }
            if (re != null)
            {
                re.Id = tag.Id;
                re.Name = tag.Name;
                re.Group = tag.Group;
                re.Desc = tag.Desc;
                re.LinkAddress = tag.LinkAddress;
                re.ReadWriteType = (ReadWriteMode)(Enum.Parse(typeof(ReadWriteMode), tag.ReadWriteType));

                if((re is ComplexTag)&&!string.IsNullOrEmpty(tag.SubTags))
                {
                    ComplexTag ctag = re as ComplexTag;
                    var dtags = DeseriseTags(tag.SubTags);
                    foreach(var vv in dtags)
                    {
                        ctag.Tags.Add(vv.Id, vv);
                    }
                }
            }
            return re;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        private List<Tagbase> DeseriseTags(string val)
        {
            List<Tagbase> re = new List<Tagbase>();
            XElement xx = XElement.Parse(val);
            foreach(var vv in xx.Elements())
            {
                re.Add(vv.LoadTagFromXML());
            }
            return re;
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

    public class ComplexValues : Dictionary<int, RealTagValueWithTimer>
    { }


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
