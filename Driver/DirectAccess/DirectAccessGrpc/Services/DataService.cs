using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Cdy.Tag;
using DirectAccess;
using DirectAccessDriver.ClientApi;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Microsoft.JSInterop;

namespace DirectAccessGrpc
{
    /// <summary>
    /// 
    /// </summary>
    public class DataService : DirectAccess.Data.DataBase
    {
        private RealDataBuffer rdb = new RealDataBuffer();

        private HisDataBuffer hdb = new HisDataBuffer();

        private readonly ILogger<DataService> _logger;

        public DataService(ILogger<DataService> logger)
        {
            _logger = logger;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReply> UpdateValue(DataRequest request, ServerCallContext context)
        {
            try
            {
                if (SecurityManager.Manager.IsLogin(request.Token))
                {
                    bool re = false;
                    if (DirectAccessProxy.Proxy.IsConnected && DirectAccessProxy.Proxy.TagCach.Count > 0)
                    {
                        lock (DirectAccessProxy.Proxy.RealSyncLocker)
                        {
                            rdb.CheckAndResize(request.Value.Count * 305);
                            rdb.Clear();
                            foreach (var vv in request.Value)
                            {
                                if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Tag))
                                {
                                    var vtg = DirectAccessProxy.Proxy.TagCach[vv.Tag];

                                    switch (vtg.Item2)
                                    {
                                        case (byte)(Cdy.Tag.TagType.Bool):
                                            rdb.AppendValue(vtg.Item1, Convert.ToBoolean(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.Byte):
                                            rdb.AppendValue(vtg.Item1, Convert.ToByte(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.Short):
                                            rdb.AppendValue(vtg.Item1, Convert.ToInt16(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.UShort):
                                            rdb.AppendValue(vtg.Item1, Convert.ToUInt16(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.Int):
                                            rdb.AppendValue(vtg.Item1, Convert.ToInt32(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.UInt):
                                            rdb.AppendValue(vtg.Item1, Convert.ToUInt32(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.Long):
                                            rdb.AppendValue(vtg.Item1, Convert.ToInt64(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.ULong):
                                            rdb.AppendValue(vtg.Item1, Convert.ToUInt64(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.Double):
                                            rdb.AppendValue(vtg.Item1, Convert.ToDouble(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.Float):
                                            rdb.AppendValue(vtg.Item1, Convert.ToSingle(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.String):
                                            rdb.AppendValue(vtg.Item1, vv.Value, (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.DateTime):
                                            rdb.AppendValue(vtg.Item1, DateTime.Parse(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.IntPoint):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.IntPointData.FromString(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.IntPoint3):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.IntPoint3Data.FromString(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.UIntPoint):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.UIntPointData.FromString(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.UIntPoint3):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.UIntPoint3Data.FromString(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.LongPoint):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.LongPointData.FromString(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.LongPoint3):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.LongPoint3Data.FromString(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.ULongPoint):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.ULongPointData.FromString(vv.Value), (byte)vv.Quality);
                                            break;
                                        case (byte)(Cdy.Tag.TagType.ULongPoint3):
                                            rdb.AppendValue(vtg.Item1, Cdy.Tag.ULongPoint3Data.FromString(vv.Value), (byte)vv.Quality);
                                            break;

                                    }
                                }
                            }
                            if (rdb.ValueCount > 0)
                            {
                                re = DirectAccessProxy.Proxy.UpdateData(rdb);
                            }
                        }

                        return Task.FromResult(new BoolReply() { Result = re, ErroMessage = re ? "" : "Send data failed" });
                    }
                    else
                    {
                        return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Connect server failed" });
                    }

                }
                else
                {
                    return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Login failed" });
                }
            }
            catch(Exception ex)
            {
                Cdy.Tag.LoggerService.Service.Erro("DataService", $"UpdateValue: {ex.Message} {ex.StackTrace}");
                return Task.FromResult(new BoolReply() { Result = false, ErroMessage = ex.Message });
            }
        }

        

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer2(TagType type, object value, byte quality, HisDataBuffer re)
        {
            switch (type)
            {
                case TagType.Bool:
                    re.Write(Convert.ToByte(value));
                    break;
                case TagType.Byte:
                    re.Write(Convert.ToByte(value));
                    break;
                case TagType.Short:
                    re.Write(Convert.ToInt16(value));
                    break;
                case TagType.UShort:
                    re.Write(Convert.ToUInt16(value));
                    break;
                case TagType.Int:
                    re.Write(Convert.ToInt32(value));
                    break;
                case TagType.UInt:
                    re.Write(Convert.ToUInt32(value));
                    break;
                case TagType.Long:
                case TagType.ULong:
                    re.Write(Convert.ToInt64(value));
                    break;
                case TagType.Float:
                    re.Write(Convert.ToSingle(value));
                    break;
                case TagType.Double:
                    re.Write(Convert.ToDouble(value));
                    break;
                case TagType.String:
                    string sval = value.ToString();
                    //re.WriteInt(sval.Length);
                    re.Write(sval, Encoding.Unicode);
                    break;
                case TagType.DateTime:
                    re.Write(((DateTime)value).ToBinary());
                    break;
                case TagType.IntPoint:
                    re.Write(((IntPointData)value).X);
                    re.Write(((IntPointData)value).Y);
                    break;
                case TagType.UIntPoint:
                    re.Write((int)((UIntPointData)value).X);
                    re.Write((int)((UIntPointData)value).Y);
                    break;
                case TagType.IntPoint3:
                    re.Write(((IntPoint3Data)value).X);
                    re.Write(((IntPoint3Data)value).Y);
                    re.Write(((IntPoint3Data)value).Z);
                    break;
                case TagType.UIntPoint3:
                    re.Write((int)((UIntPoint3Data)value).X);
                    re.Write((int)((UIntPoint3Data)value).Y);
                    re.Write((int)((UIntPoint3Data)value).Z);
                    break;
                case TagType.LongPoint:
                    re.Write(((LongPointData)value).X);
                    re.Write(((LongPointData)value).Y);
                    break;
                case TagType.ULongPoint:
                    re.Write((long)((ULongPointData)value).X);
                    re.Write((long)((ULongPointData)value).Y);
                    break;
                case TagType.LongPoint3:
                    re.Write(((LongPoint3Data)value).X);
                    re.Write(((LongPoint3Data)value).Y);
                    re.Write(((LongPoint3Data)value).Z);
                    break;
                case TagType.ULongPoint3:
                    re.Write((long)((ULongPoint3Data)value).X);
                    re.Write((long)((ULongPoint3Data)value).Y);
                    re.Write((long)((ULongPoint3Data)value).Z);
                    break;
            }
            re.WriteByte(quality);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReply> UpdateHisValue(HisDataRequest request, ServerCallContext context)
        {
            try
            {
                if (SecurityManager.Manager.IsLogin(request.Token))
                {
                    if (DirectAccessProxy.Proxy.IsConnected && DirectAccessProxy.Proxy.TagCach.Count > 0)
                    {
                        int size = 0;

                        foreach (var vv in request.Value)
                        {
                            if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Tag))
                            {
                                size += 9;
                                var vtg = DirectAccessProxy.Proxy.TagCach[vv.Tag];
                                switch (vtg.Item2)
                                {
                                    case (byte)(Cdy.Tag.TagType.Bool):
                                        size += vv.Value.Count * (8 + 1 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Byte):
                                        size += vv.Value.Count * (8 + 1 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Short):
                                        size += vv.Value.Count * (8 + 2 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.UShort):
                                        size += vv.Value.Count * (8 + 2 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Int):
                                    case (byte)(Cdy.Tag.TagType.UInt):
                                        size += vv.Value.Count * (8 + 4 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Long):
                                    case (byte)(Cdy.Tag.TagType.ULong):
                                        size += vv.Value.Count * (8 + 8 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Double):
                                        size += vv.Value.Count * (8 + 8 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Float):
                                        size += vv.Value.Count * (8 + 4 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.String):
                                        size += vv.Value.Count * (8 + 258 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.DateTime):
                                        size += vv.Value.Count * (8 + 8 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.IntPoint):
                                        size += vv.Value.Count * (8 + 8 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.IntPoint3):
                                        size += vv.Value.Count * (8 + 12 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.UIntPoint):
                                        size += vv.Value.Count * (8 + 8 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.UIntPoint3):
                                        size += vv.Value.Count * (8 + 12 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.LongPoint):
                                        size += vv.Value.Count * (8 + 16 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.LongPoint3):
                                        size += vv.Value.Count * (8 + 24 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.ULongPoint):
                                        size += vv.Value.Count * (8 + 16 + 1);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.ULongPoint3):
                                        size += vv.Value.Count * (8 + 24 + 1);
                                        break;
                                }
                            }
                        }

                        hdb.CheckAndResize(size);
                        hdb.Clear();
                        int valuegroupcount = 0;
                        lock (DirectAccessProxy.Proxy.HisSyncLocker)
                        {
                            foreach (var vv in request.Value)
                            {
                                if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Tag))
                                {
                                    valuegroupcount++;
                                    var vtg = DirectAccessProxy.Proxy.TagCach[vv.Tag];
                                    hdb.Write(vtg.Item1);
                                    hdb.Write(vv.Value.Count);
                                    hdb.Write((byte)vtg.Item2);

                                    foreach (var val in vv.Value)
                                    {
                                        hdb.Write(val.Time);
                                        SetTagValueToBuffer2((TagType)vtg.Item2, val.Value, (byte)val.Quality, hdb);
                                    }
                                }
                            }
                        }
                        bool re = false;
                        if (hdb.Position > 0)
                        {
                            re = DirectAccessProxy.Proxy.UpdateHisData(hdb, valuegroupcount);
                        }
                        return Task.FromResult(new BoolReply() { Result = re, ErroMessage = re ? "" : "Send data failed" });
                    }
                    else
                    {
                        return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Connect server failed" });
                    }
                }
                return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Login failed" });
            }
            catch(Exception ex)
            {
                Cdy.Tag.LoggerService.Service.Erro("DataService", $"UpdateHisValue: {ex.Message} {ex.StackTrace}");
                return Task.FromResult(new BoolReply() { Result = false, ErroMessage = ex.Message });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReply> UpdateAreaHisValue(HisAreaDataRequest request, ServerCallContext context)
        {
            try
            {
                if (SecurityManager.Manager.IsLogin(request.Token))
                {
                    if (DirectAccessProxy.Proxy.IsConnected && DirectAccessProxy.Proxy.TagCach.Count > 0)
                    {
                        int size = 0;
                        bool re = false;
                        DateTime dtime = DateTime.FromBinary(request.Value.Time);

                        List<RealTagValue> values = new List<RealTagValue>();
                        foreach(var vv in request.Value.Values)
                        {
                            string sname = vv.Name;
                            if (DirectAccessProxy.Proxy.TagCach.ContainsKey(sname))
                            {
                                var vtg = DirectAccessProxy.Proxy.TagCach[sname];
                                RealTagValue rt = new RealTagValue();
                                rt.Id = vtg.Item1;
                                switch (vtg.Item2)
                                {
                                    case (byte)(Cdy.Tag.TagType.Bool):
                                        rt.Value = Convert.ToBoolean(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Byte):
                                       rt.Value = Convert.ToByte(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Short):
                                        rt.Value = Convert.ToInt16(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.UShort):
                                        rt.Value = Convert.ToUInt16(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Int):
                                        rt.Value = Convert.ToInt32(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.UInt):
                                        rt.Value = Convert.ToUInt32(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Long):
                                        rt.Value = Convert.ToInt64(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.ULong):
                                        rt.Value = Convert.ToUInt64(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Double):
                                        rt.Value = Convert.ToDouble(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.Float):
                                        rt.Value = Convert.ToSingle(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.String):
                                        rt.Value = vv.Value;
                                        break;
                                    case (byte)(Cdy.Tag.TagType.DateTime):
                                        rt.Value = Convert.ToDateTime(vv.Value);
                                        break;
                                    case (byte)(Cdy.Tag.TagType.IntPoint):
                                        string[] sval = vv.Value.Split(",");
                                        rt.Value = new IntPointData(int.Parse(sval[0]),int.Parse(sval[1]));
                                        break;
                                    case (byte)(Cdy.Tag.TagType.IntPoint3):
                                        sval = vv.Value.Split(",");
                                        rt.Value = new IntPoint3Data(int.Parse(sval[0]), int.Parse(sval[1]), int.Parse(sval[2]));
                                        break;
                                    case (byte)(Cdy.Tag.TagType.UIntPoint):
                                        sval = vv.Value.Split(",");
                                        rt.Value = new UIntPointData(int.Parse(sval[0]), int.Parse(sval[1]));
                                        break;
                                    case (byte)(Cdy.Tag.TagType.UIntPoint3):
                                        sval = vv.Value.Split(",");
                                        rt.Value = new UIntPoint3Data(uint.Parse(sval[0]), uint.Parse(sval[1]), uint.Parse(sval[2]));
                                        break;
                                    case (byte)(Cdy.Tag.TagType.LongPoint):
                                        sval = vv.Value.Split(",");
                                        rt.Value = new LongPointData(long.Parse(sval[0]), long.Parse(sval[1]));
                                        break;
                                    case (byte)(Cdy.Tag.TagType.LongPoint3):
                                        sval = vv.Value.Split(",");
                                        rt.Value = new LongPoint3Data(long.Parse(sval[0]), long.Parse(sval[1]), long.Parse(sval[2]));
                                        break;
                                    case (byte)(Cdy.Tag.TagType.ULongPoint):
                                        sval = vv.Value.Split(",");
                                        rt.Value = new ULongPointData(ulong.Parse(sval[0]), ulong.Parse(sval[1]));
                                        break;
                                    case (byte)(Cdy.Tag.TagType.ULongPoint3):
                                        sval = vv.Value.Split(",");
                                        rt.Value = new ULongPoint3Data(ulong.Parse(sval[0]), ulong.Parse(sval[1]), ulong.Parse(sval[2]));
                                        break;
                                }
                            }
                        }
                        if (values.Count > 0)
                        {
                            re = DirectAccessProxy.Proxy.UpdateAreaHisData(dtime, values);
                        }
                        return Task.FromResult(new BoolReply() { Result = re, ErroMessage = re ? "" : "Send data failed" });
                    }
                    else
                    {
                        return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Connect server failed" });
                    }
                }
                return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Login failed" });
            }
            catch (Exception ex)
            {
                Cdy.Tag.LoggerService.Service.Erro("DataService", $"UpdateHisValue: {ex.Message} {ex.StackTrace}");
                return Task.FromResult(new BoolReply() { Result = false, ErroMessage = ex.Message });
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<LoginReply> Login(LoginRequest request, ServerCallContext context)
        {
            string Token = DirectAccessProxy.Proxy.Login(request.Name, request.Password);
            if (!string.IsNullOrEmpty(Token))
            {
                SecurityManager.Manager.CachUser(Token);
                return Task.FromResult(new LoginReply() { Token = Token, Time = DateTime.UtcNow.ToBinary(), Timeout = SecurityManager.Manager.Timeout });
            }
            else
                return Task.FromResult(new LoginReply());
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReply> Logout(LogoutRequest request, ServerCallContext context)
        {
            //Cdy.Tag.ServiceLocator.Locator.Resolve<Cdy.Tag.IRuntimeSecurity>().Logout(request.Token);
            SecurityManager.Manager.RemoveUser(request.Token);
            return Task.FromResult(new BoolReply() { Result = true });
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<BoolReply> Hart(HartRequest request, ServerCallContext context)
        {

            var dt = DateTime.FromBinary(request.Time);
            if ((DateTime.UtcNow - dt).TotalSeconds > SecurityManager.Manager.Timeout)
            {
                return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Login failed" });
            }

            SecurityManager.Manager.RefreshLogin(request.Token);
            return Task.FromResult(new BoolReply() { Result = true });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetHisValue(QueryHisDataRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    ReadTagHisValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Duration, request.QueryType, re);
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetHisValueAtTimes(QueryHisDataAtTimesRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };

                var times = request.Times.Select(e => DateTime.FromBinary(e)).ToList();

                foreach (var vv in request.Tags)
                {
                    ReadTagHisValue(vv, times, request.QueryType, re);
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetHisValueIgnorSystemExit(QueryHisDataRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    ReadTagHisValueByIgnorSystemExit(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), request.Duration, request.QueryType, re);
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetHisValueAtTimesIgnorSystemExit(QueryHisDataAtTimesRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };
                var times = request.Times.Select(e => DateTime.FromBinary(e)).ToList();
                foreach (var vv in request.Tags)
                {
                    ReadTagHisValueByIgnorSystemExit(vv, times, request.QueryType, re);
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<HisDataCollectionReplay> GetAllHisValue(QueryAllHisDataRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token))
            {
                HisDataCollectionReplay re = new HisDataCollectionReplay() { Result = true };
                foreach (var vv in request.Tags)
                {
                    ReadTagAllHisValue(vv, DateTime.FromBinary(request.StartTime), DateTime.FromBinary(request.EndTime), re);
                }
                return Task.FromResult(re);
            }
            else
            {
                return Task.FromResult(new HisDataCollectionReplay() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="request"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public override Task<GetRealValueReply> GetRealValue(GetRealValueRequest request, ServerCallContext context)
        {
            if (SecurityManager.Manager.IsLogin(request.Token) )
            {
                GetRealValueReply response = new GetRealValueReply() { Result = true };
                var ids = (request.TagNames.Select(e => string.IsNullOrEmpty(request.Group) ? e : request.Group + "." + e).Where(e => DirectAccessProxy.Proxy.TagCach.ContainsKey(e)).Select(e => DirectAccessProxy.Proxy.TagCach[e].Item1).ToList());
                var vals = DirectAccessProxy.Proxy.GetRealData(ids);
                if (ids.Any())
                {
                    for (int i = 0; i < request.TagNames.Count; i++)
                    {
                        var vid = DirectAccessProxy.Proxy.TagCach.ContainsKey((string)request.TagNames[i])?DirectAccessProxy.Proxy.TagCach[request.TagNames[i]].Item1:-1;
                        var typ = DirectAccessProxy.Proxy.TagCach[request.TagNames[i]].Item2;
                        if (vals!=null && vals.ContainsKey(vid))
                        {
                            var val = vals[vid];
                            if(val.Value is ComplexRealValue)
                            {
                                response.Values.Add(new ValueQualityTime() { Id = i, Quality = val.Quality, Value = FormateToString(val.Value as ComplexRealValue), ValueType = typ, Time = val.Time.Ticks });
                            }
                            else
                            {
                                response.Values.Add(new ValueQualityTime() { Id = i, Quality = val.Quality, Value = val.Value.ToString(), ValueType = typ, Time = val.Time.Ticks });
                            }
                           
                        }
                        else
                        {
                            response.Values.Add(new ValueQualityTime() { Id = i, Quality = (byte)QualityConst.Null, Value = "", ValueType = typ, Time = 0 });
                        }
                    }
                }
                return Task.FromResult(response);
            }
            else
            {
                return Task.FromResult(new GetRealValueReply() { Result = false });
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="vals"></param>
        /// <returns></returns>
        private string FormateToString(ComplexRealValue vals)
        {
            StringBuilder sb = new StringBuilder();
            foreach (var val in vals)
            {
                sb.Append($"{val.Id}:{val.ValueType}:{val.Value}:{val.Quality}:{val.Time.ToBinary()};");
            }
            return sb.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValue(string tag, DateTime startTime, DateTime endTime, int duration, int type, HisDataCollectionReplay result)
        {
            if (!DirectAccessProxy.Proxy.TagCach.ContainsKey(tag)) return;
            object res;
            var times = GetTimes(startTime, endTime, TimeSpan.FromMilliseconds(duration));
            var tgs = DirectAccessProxy.Proxy.TagCach[tag];
            switch ((Cdy.Tag.TagType)tgs.Item2)
            {
                case Cdy.Tag.TagType.Bool:
                    res =  DirectAccessProxy.Proxy.QueryHisData<bool>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<bool>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res =  DirectAccessProxy.Proxy.QueryHisData<byte>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<byte>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res =  DirectAccessProxy.Proxy.QueryHisData<DateTime>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res =  DirectAccessProxy.Proxy.QueryHisData<double>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<double>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res =  DirectAccessProxy.Proxy.QueryHisData<float>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<float>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res =  DirectAccessProxy.Proxy.QueryHisData<int>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<int>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res =  DirectAccessProxy.Proxy.QueryHisData<long>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<long>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res =  DirectAccessProxy.Proxy.QueryHisData<short>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<short>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res =  DirectAccessProxy.Proxy.QueryHisData<string>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<string>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res =  DirectAccessProxy.Proxy.QueryHisData<uint>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<uint>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res =  DirectAccessProxy.Proxy.QueryHisData<ulong>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res =  DirectAccessProxy.Proxy.QueryHisData<ushort>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisData<IntPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisData<UIntPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisData<IntPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisData<UIntPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisData<LongPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisData<ULongPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisData<LongPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisData<ULongPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="times"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValue(string tag, List<DateTime> times, int type, HisDataCollectionReplay result)
        {
            if (!DirectAccessProxy.Proxy.TagCach.ContainsKey(tag)) return;
            var tgs = DirectAccessProxy.Proxy.TagCach[tag];
            object res;
            switch ((Cdy.Tag.TagType)tgs.Item2)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DirectAccessProxy.Proxy.QueryHisData<bool>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<bool>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DirectAccessProxy.Proxy.QueryHisData<byte>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<byte>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DirectAccessProxy.Proxy.QueryHisData<DateTime>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DirectAccessProxy.Proxy.QueryHisData<double>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<double>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DirectAccessProxy.Proxy.QueryHisData<float>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<float>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DirectAccessProxy.Proxy.QueryHisData<int>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<int>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DirectAccessProxy.Proxy.QueryHisData<long>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<long>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DirectAccessProxy.Proxy.QueryHisData<short>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<short>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DirectAccessProxy.Proxy.QueryHisData<string>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<string>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DirectAccessProxy.Proxy.QueryHisData<uint>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<uint>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DirectAccessProxy.Proxy.QueryHisData<ulong>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DirectAccessProxy.Proxy.QueryHisData<ushort>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DirectAccessProxy.Proxy.QueryHisData<IntPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DirectAccessProxy.Proxy.QueryHisData<UIntPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisData<IntPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisData<UIntPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DirectAccessProxy.Proxy.QueryHisData<LongPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DirectAccessProxy.Proxy.QueryHisData<ULongPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisData<LongPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisData<ULongPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValueByIgnorSystemExit(string tag, DateTime startTime, DateTime endTime, int duration, int type, HisDataCollectionReplay result)
        {
            if (!DirectAccessProxy.Proxy.TagCach.ContainsKey(tag)) return;
            var tgs = DirectAccessProxy.Proxy.TagCach[tag];
            object res;
            var times = GetTimes(startTime, endTime, TimeSpan.FromMilliseconds(duration));
            switch ((Cdy.Tag.TagType)tgs.Item2)
            {
                case Cdy.Tag.TagType.Bool:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<bool>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<bool>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<byte>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<byte>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<double>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<double>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<float>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<float>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<int>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<int>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<long>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<long>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<short>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<short>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<string>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<string>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<uint>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<uint>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ULongPointData>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res =  DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tgs.Item1, startTime, endTime, TimeSpan.FromMilliseconds(duration), (QueryValueMatchType)(type));
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="duration"></param>
        /// <param name="type"></param>
        /// <param name="result"></param>
        private void ReadTagHisValueByIgnorSystemExit(string tag, List<DateTime> times, int type, HisDataCollectionReplay result)
        {
            if (!DirectAccessProxy.Proxy.TagCach.ContainsKey(tag)) return;
            var tgs = DirectAccessProxy.Proxy.TagCach[tag];
            object res;
            switch ((Cdy.Tag.TagType)tgs.Item2)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<bool>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<bool>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<byte>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<byte>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<DateTime>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<double>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<double>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<float>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<float>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<int>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<int>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<long>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<long>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.Short:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<short>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<short>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.String:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<string>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<string>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<uint>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<uint>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ulong>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ushort>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<IntPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<UIntPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<IntPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<UIntPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<LongPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ULongPointData>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<LongPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res = DirectAccessProxy.Proxy.QueryHisDataByIgnorSystemExit<ULongPoint3Data>(tgs.Item1, times, (QueryValueMatchType)(type));
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Item2, times);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="tag"></param>
        /// <param name="startTime"></param>
        /// <param name="endTime"></param>
        /// <param name="result"></param>
        private void ReadTagAllHisValue(string tag, DateTime startTime, DateTime endTime, HisDataCollectionReplay result)
        {
            if (!DirectAccessProxy.Proxy.TagCach.ContainsKey(tag)) return;
            var tgs = DirectAccessProxy.Proxy.TagCach[tag];

            object res;
            switch ((Cdy.Tag.TagType)tgs.Item2)
            {
                case Cdy.Tag.TagType.Bool:
                    res = DirectAccessProxy.Proxy.QueryAllHisData<bool>(tgs.Item1, startTime, endTime);
                    ProcessResult<bool>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.Byte:
                    res = DirectAccessProxy.Proxy.QueryAllHisData<byte>(tgs.Item1, startTime, endTime);
                    ProcessResult<byte>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.DateTime:
                    res = DirectAccessProxy.Proxy.QueryAllHisData<DateTime>(tgs.Item1, startTime, endTime);
                    ProcessResult<DateTime>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.Double:
                    res = DirectAccessProxy.Proxy.QueryAllHisData<double>(tgs.Item1, startTime, endTime);
                    ProcessResult<double>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.Float:
                    res = DirectAccessProxy.Proxy.QueryAllHisData<float>(tgs.Item1, startTime, endTime);
                    ProcessResult<float>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.Int:
                    res = DirectAccessProxy.Proxy.QueryAllHisData<int>(tgs.Item1, startTime, endTime);
                    ProcessResult<int>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.Long:
                    res = DirectAccessProxy.Proxy.QueryAllHisData<long>(tgs.Item1, startTime, endTime);
                    ProcessResult<long>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.Short:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<short>(tgs.Item1, startTime, endTime);
                    ProcessResult<short>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.String:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<string>(tgs.Item1, startTime, endTime);
                    ProcessResult<string>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.UInt:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<uint>(tgs.Item1, startTime, endTime);
                    ProcessResult<uint>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.ULong:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<ulong>(tgs.Item1, startTime, endTime);
                    ProcessResult<ulong>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.UShort:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<ushort>(tgs.Item1, startTime, endTime);
                    ProcessResult<ushort>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.IntPoint:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<IntPointData>(tgs.Item1, startTime, endTime);
                    ProcessResult<IntPointData>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.UIntPoint:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<UIntPointData>(tgs.Item1, startTime, endTime);
                    ProcessResult<UIntPointData>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.IntPoint3:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<IntPoint3Data>(tgs.Item1, startTime, endTime);
                    ProcessResult<IntPoint3Data>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.UIntPoint3:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<UIntPoint3Data>(tgs.Item1, startTime, endTime);
                    ProcessResult<UIntPoint3Data>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.LongPoint:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<LongPointData>(tgs.Item1, startTime, endTime);
                    ProcessResult<LongPointData>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.ULongPoint:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<ULongPointData>(tgs.Item1, startTime, endTime);
                    ProcessResult<ULongPointData>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.LongPoint3:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<LongPoint3Data>(tgs.Item1, startTime, endTime);
                    ProcessResult<LongPoint3Data>(tag, res, result, (int)tgs.Item2);
                    break;
                case Cdy.Tag.TagType.ULongPoint3:
                    res =  DirectAccessProxy.Proxy.QueryAllHisData<ULongPoint3Data>(tgs.Item1, startTime, endTime);
                    ProcessResult<ULongPoint3Data>(tag, res, result, (int)tgs.Item2);
                    break;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="result"></param>
        /// <param name="valueType"></param>
        /// <param name="times"></param>
        private void ProcessResult<T>(string tag, object value, HisDataCollectionReplay result, int valueType, List<DateTime> times)
        {
            HisDataPointCollection hdp = new HisDataPointCollection() { Tag = tag, ValueType = valueType };
            var vdata = value as HisQueryResult<T>;
            if (vdata != null)
            {
                SortedDictionary<DateTime, HisDataPoint> rtmp = new SortedDictionary<DateTime, HisDataPoint>();
                for (int i = 0; i < vdata.Count; i++)
                {
                    byte qu;
                    DateTime time;
                    var val = vdata.GetValue(i, out time, out qu);
                    rtmp.Add(time, new HisDataPoint() { Time = time.ToBinary(), Value = val.ToString(), Quality = qu });
                    //hdp.Values.Add(new HisDataPoint() { Time = time.ToBinary(), Value = val.ToString() });
                }

                foreach (var vv in times)
                {
                    if (!rtmp.ContainsKey(vv))
                    {
                        rtmp.Add(vv, new HisDataPoint() { Time = vv.ToBinary(), Value = default(T).ToString(), Quality = (byte)QualityConst.Null });
                    }
                }

                foreach (var vv in rtmp)
                {
                    hdp.Values.Add(vv.Value);
                }
            }
            result.Values.Add(hdp);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tag"></param>
        /// <param name="value"></param>
        /// <param name="result"></param>
        /// <param name="valueType"></param>
        private void ProcessResult<T>(string tag, object value, HisDataCollectionReplay result, int valueType)
        {
            HisDataPointCollection hdp = new HisDataPointCollection() { Tag = tag, ValueType = valueType };
            var vdata = value as HisQueryResult<T>;
            if (vdata != null)
            {
                for (int i = 0; i < vdata.Count; i++)
                {
                    byte qu;
                    DateTime time;
                    var val = vdata.GetValue(i, out time, out qu);
                    hdp.Values.Add(new HisDataPoint() { Time = time.ToBinary(), Value = val.ToString(), Quality = qu });
                }
            }
            result.Values.Add(hdp);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        /// <param name="timespan"></param>
        /// <returns></returns>
        private List<DateTime> GetTimes(DateTime starttime, DateTime endtime, TimeSpan timespan)
        {
            List<DateTime> re = new List<DateTime>();
            DateTime dt = starttime;
            while (dt < endtime)
            {
                re.Add(dt);
                dt = dt.Add(timespan);
            }
            return re;
        }
    }
}
