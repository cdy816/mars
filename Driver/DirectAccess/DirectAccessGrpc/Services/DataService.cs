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

        //public override Task<BoolReply> UpdateHisValue(HisDataRequest request, ServerCallContext context)
        //{
        //    try
        //    {
        //        if (SecurityManager.Manager.IsLogin(request.Token))
        //        {
        //            if (DirectAccessProxy.Proxy.IsConnected && DirectAccessProxy.Proxy.TagCach.Count > 0)
        //            {
        //                Dictionary<int, IEnumerable<TagValueAndType>> values = new Dictionary<int, IEnumerable<TagValueAndType>>();
        //                foreach (var vv in request.Value)
        //                {
        //                    if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Tag))
        //                    {
        //                        var vtg = DirectAccessProxy.Proxy.TagCach[vv.Tag];
        //                        List<TagValueAndType> val;
        //                        if (values.ContainsKey(vtg.Item1))
        //                        {
        //                            val = values[vtg.Item1] as List<TagValueAndType>;
        //                        }
        //                        else
        //                        {
        //                            val = new List<TagValueAndType>();
        //                        }

        //                        foreach (var vvv in vv.Value)
        //                        {
        //                            val.Add(new TagValueAndType() { Value = vvv.Value, ValueType = (TagType)vtg.Item2, Quality = (byte)vvv.Quality, Time = DateTime.FromBinary(vvv.Time) });
        //                        }
        //                    }
        //                }

        //                var re = DirectAccessProxy.Proxy.UpdateHisData(values);
        //                return Task.FromResult(new BoolReply() { Result = re, ErroMessage = re ? "" : "Send data failed" });
        //            }
        //            else
        //            {
        //                return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Connect server failed" });
        //            }
        //        }
        //        return Task.FromResult(new BoolReply() { Result = false, ErroMessage = "Login failed" });
        //    }
        //    catch (Exception ex)
        //    {
        //        Cdy.Tag.LoggerService.Service.Erro("DataService", $"UpdateHisValue: {ex.Message} {ex.StackTrace}");
        //        return Task.FromResult(new BoolReply() { Result = false, ErroMessage = ex.Message });
        //    }
        //}

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

    }
}
