using Cdy.Tag;
using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using MQTTnet.Protocol;
using MQTTnet.Server;
using System.Runtime.Intrinsics.Arm;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DirectAccessMqtt
{
    public class MqttServer
    {
        private MqttFactory mqttFactory;

        private MqttClientOptions options;

        private IMqttClient mqttClient;

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, string> mRegistorTags = new Dictionary<string, string>();

        private Dictionary<string, string> mDeviceTopics = new Dictionary<string, string>();


        /// <summary>
        /// 
        /// </summary>
        public static MqttServer Instance = new MqttServer();

        /// <summary>
        /// 
        /// </summary>
        public string ServerTopic { get; set; } = "Mars_DirectAccess";

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            if (Config.Instance.UseInnerMqttServer)
            {
                StartLocalServer();
                Config.Instance.RemoteMqttServer = "127.0.0.1";
                Config.Instance.RemoteMqttPort = Config.Instance.InnerServer.Port;
                Config.Instance.UserName = Config.Instance.InnerServer.UserName;
                Config.Instance.Password = Config.Instance.InnerServer.Password;
            }
            StartClient();

            DirectAccessProxy.Proxy.ValueChangedCallBack = (name,val) => { 
                if(mRegistorTags.ContainsKey(name))
                {
                    SetTagValueToDevice(name,val);
                }
            };
        }

        public void Stop()
        {
            DirectAccessProxy.Proxy.ValueChangedCallBack = null;
            StopClient();
        }

        private void StartClient()
        {
            var tlsOptions = new MqttClientTlsOptions
            {
                UseTls = false,
                IgnoreCertificateChainErrors = true,
                IgnoreCertificateRevocationErrors = true,
                AllowUntrustedCertificates = true
            };

            var version = Config.Instance.ProtocolVersion == "V31" ? MqttProtocolVersion.V311 : (Config.Instance.ProtocolVersion == "V50" ? MqttProtocolVersion.V500 : MqttProtocolVersion.V310);

            options = new MqttClientOptions
            {
                ClientId = Guid.NewGuid().ToString(),
                ProtocolVersion = version,
                ChannelOptions = new MqttClientTcpOptions
                {
                    Server = Config.Instance.RemoteMqttServer,
                    Port = Config.Instance.RemoteMqttPort,
                    TlsOptions = tlsOptions
                }
            };

            options.Credentials = new MqttClientCredentials(Config.Instance.UserName, Encoding.UTF8.GetBytes(Config.Instance.Password));
            options.CleanSession = true;
            options.KeepAlivePeriod = TimeSpan.FromMilliseconds(5000);
            mqttFactory = new MqttFactory();
            mqttClient = mqttFactory.CreateMqttClient();
            mqttClient.ApplicationMessageReceivedAsync += MqttClient_ApplicationMessageReceivedAsync;
            mqttClient.ConnectedAsync += MqttClient_ConnectedAsync;
            mqttClient.DisconnectedAsync += MqttClient_DisconnectedAsync;

            mqttClient.ConnectAsync(options);
        }

        private void StopClient()
        {
            if (mqttClient != null)
            {
                mqttClient.ApplicationMessageReceivedAsync -= MqttClient_ApplicationMessageReceivedAsync;
                mqttClient.ConnectedAsync -= MqttClient_ConnectedAsync;
                mqttClient.DisconnectedAsync -= MqttClient_DisconnectedAsync;
                mqttClient.DisconnectAsync();
                mqttClient?.Dispose();
                mqttClient = null;
            }
        }

        private Task MqttClient_DisconnectedAsync(MqttClientDisconnectedEventArgs arg)
        {
            LoggerService.Service.Warn("MqttServer", $"DisConnect Mqtt Server{Config.Instance.RemoteMqttServer} {Config.Instance.RemoteMqttPort}!");
            return Task.CompletedTask;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="arg"></param>
        /// <returns></returns>
        private Task MqttClient_ConnectedAsync(MqttClientConnectedEventArgs arg)
        {
            mqttClient.SubscribeAsync(this.ServerTopic);
            LoggerService.Service.Info("MqttServer", $"Connect Mqtt Server {Config.Instance.RemoteMqttServer } {Config.Instance.RemoteMqttPort} sucessfull!");
            LoggerService.Service.Info("MqttServer", $"Subscribe Topic {this.ServerTopic} .");
            return Task.CompletedTask;
        }

        private Task MqttClient_ApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs arg)
        {
            if(arg.ApplicationMessage.Payload!=null)
            {
                var node = JsonNode.Parse(arg.ApplicationMessage.Payload);
                if (node["Fun"]!=null)
                {
                    string sfun = node["Fun"].ToString().ToLower();
                    switch (sfun)
                    {
                        case "0":
                        case "login":
                            Task.Run(() => {
                                ProcessLogin(JsonSerializer.Deserialize<LoginRequest>(node));
                            });
                         
                            break;
                        case "1":
                        case "updatedevicevalue":
                            Task.Run(() => {
                                ProcessUpdateDeviceValue(JsonSerializer.Deserialize<UpdateDeviceValueRequest>(node));
                            });
                          
                            break;
                        case "2":
                        case "updateareadevicevalue":
                            Task.Run(() => {
                                ProcessAreaHisValue(JsonSerializer.Deserialize<UpdateAreaDeviceValueRequest>(node));
                            });
                           
                            break;
                        case "3":
                        case "updatehisvalue":
                            Task.Run(() => {
                                ProcessUpdateHisValue(JsonSerializer.Deserialize<UpdateHisValueRequest>(node));
                            });
                           
                            break;
                        case "4":
                        case "registorcallback":
                            Task.Run(() => {
                                ProcessRegistorCallBack(JsonSerializer.Deserialize<RegistorCallBackRequest>(node));
                            });
                            
                            break;
                        case "5":
                        case "devicesetresponse":
                            Task.Run(() => {
                                ProcessSetTagValueDeviceCallBack(JsonSerializer.Deserialize<DeviceSetResponse>(node));
                            });
                           
                            break;
                    }
                }
                
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="lg"></param>
        private void ProcessLogin(LoginRequest lg)
        {
            string Token = DirectAccessProxy.Proxy.Login(lg.UserName, lg.Password);
            string device = lg.DeviceId;

            if (!string.IsNullOrEmpty(Token))
            {
                SecurityManager.Manager.CachUser(Token);
                mDeviceTopics.Add(Token, device);
                SendToTopicData(device, "", Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new LoginResponse() { Token = Token })));
            }
            else
            {
                SendToTopicData(device, "", Encoding.UTF8.GetBytes(JsonSerializer.Serialize(new LoginResponse() { Token = "" })));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="updateDeviceValue"></param>
        private bool ProcessUpdateDeviceValue(UpdateDeviceValueRequest updateDeviceValue)
        {
            if(updateDeviceValue!=null && SecurityManager.Manager.IsLogin(updateDeviceValue.Token))
            {
                SecurityManager.Manager.RefreshLogin(updateDeviceValue.Token);
                bool re = false;
                using (DirectAccessDriver.ClientApi.RealDataBuffer rdb = new DirectAccessDriver.ClientApi.RealDataBuffer(updateDeviceValue.Values.Length * 305))
                {
                    foreach (var vv in updateDeviceValue.Values)
                    {
                        
                        if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Name))
                        {
                            var vtg = DirectAccessProxy.Proxy.TagCach[vv.Name];
                            switch (vtg.Item2)
                            {
                                case (byte)(Cdy.Tag.TagType.Bool):
                                    rdb.AppendValue(vtg.Item1, Convert.ToBoolean(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.Byte):
                                    rdb.AppendValue(vtg.Item1, Convert.ToByte(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.Short):
                                    rdb.AppendValue(vtg.Item1, Convert.ToUInt16(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.UShort):
                                    rdb.AppendValue(vtg.Item1, Convert.ToUInt16(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.Int):
                                    rdb.AppendValue(vtg.Item1, Convert.ToInt32(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.UInt):
                                    rdb.AppendValue(vtg.Item1, Convert.ToUInt32(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.Long):
                                    rdb.AppendValue(vtg.Item1, Convert.ToInt64(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.ULong):
                                    rdb.AppendValue(vtg.Item1, Convert.ToUInt64(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.Double):
                                    rdb.AppendValue(vtg.Item1, Convert.ToDouble(ConvertTo(vtg.Item2,(JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.Float):
                                    rdb.AppendValue(vtg.Item1, Convert.ToSingle(ConvertTo(vtg.Item2, (JsonElement)vv.Value)), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.String):
                                    rdb.AppendValue(vtg.Item1, ((JsonElement)vv.Value).GetRawText(), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.DateTime):
                                    rdb.AppendValue(vtg.Item1, DateTime.Parse(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.IntPoint):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.IntPointData.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.IntPoint3):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.IntPoint3Data.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.UIntPoint):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.UIntPointData.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.UIntPoint3):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.UIntPoint3Data.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.LongPoint):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.LongPointData.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.LongPoint3):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.LongPoint3Data.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.ULongPoint):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.ULongPointData.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;
                                case (byte)(Cdy.Tag.TagType.ULongPoint3):
                                    rdb.AppendValue(vtg.Item1, Cdy.Tag.ULongPoint3Data.FromString(((JsonElement)vv.Value).GetRawText()), 0);
                                    break;

                            }
                        }
                    }
                    re = DirectAccessProxy.Proxy.UpdateData(rdb);
                    
                }
                if(mDeviceTopics.ContainsKey(updateDeviceValue.Token))
                {
                    string dtopic = mDeviceTopics[updateDeviceValue.Token];
                    SendToTopicData(dtopic, new BoolReponse() { CallId = updateDeviceValue.CallId, Result = true });
                }
                return re;
            }
            return false;
        }

        private object ConvertTo(byte tp,JsonElement vv)
        {
            object re = null;
            switch (tp)
            {
                case (byte)(Cdy.Tag.TagType.Bool):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String? bool.Parse(((JsonElement)vv).GetString()):((JsonElement)vv).GetBoolean();
                    break;
                case (byte)(Cdy.Tag.TagType.Byte):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? byte.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetByte();
                    break;
                case (byte)(Cdy.Tag.TagType.Short):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? Int16.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetInt16();
                    break;
                case (byte)(Cdy.Tag.TagType.UShort):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? UInt16.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetUInt16();
                    break;  
                case (byte)(Cdy.Tag.TagType.Int):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? Int32.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetInt32();
                    break;
                case (byte)(Cdy.Tag.TagType.UInt):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? UInt32.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetUInt32();
                    break;
                case (byte)(Cdy.Tag.TagType.Long):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? Int64.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetInt64();
                    break;
                case (byte)(Cdy.Tag.TagType.ULong):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? UInt64.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetUInt64();
                    break;
                case (byte)(Cdy.Tag.TagType.Double):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? Double.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetDouble();
                    break;
                case (byte)(Cdy.Tag.TagType.Float):
                    re = ((JsonElement)vv).ValueKind == JsonValueKind.String ? float.Parse(((JsonElement)vv).GetString()) : ((JsonElement)vv).GetSingle();
                    break;
                case (byte)(Cdy.Tag.TagType.String):
                    re = ((JsonElement)vv).GetRawText();
                    break;
                case (byte)(Cdy.Tag.TagType.DateTime):
                    re = DateTime.Parse(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.IntPoint):
                    re = Cdy.Tag.IntPointData.FromString(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.IntPoint3):
                    re = Cdy.Tag.IntPoint3Data.FromString(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.UIntPoint):
                    re = Cdy.Tag.UIntPointData.FromString(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.UIntPoint3):
                    re = Cdy.Tag.UIntPoint3Data.FromString(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.LongPoint):
                    re = Cdy.Tag.LongPointData.FromString(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.LongPoint3):
                    re = Cdy.Tag.LongPoint3Data.FromString(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.ULongPoint):
                    re = Cdy.Tag.ULongPointData.FromString(((JsonElement)vv).GetRawText());
                    break;
                case (byte)(Cdy.Tag.TagType.ULongPoint3):
                    re = Cdy.Tag.ULongPoint3Data.FromString(((JsonElement)vv).GetRawText());
                    break;

            }
            return re;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="updateHisValue"></param>
        private bool ProcessAreaHisValue(UpdateAreaDeviceValueRequest updateHisValue)
        {
            if (updateHisValue != null && SecurityManager.Manager.IsLogin(updateHisValue.Token))
            {
                SecurityManager.Manager.RefreshLogin(updateHisValue.Token);

                DateTime time = DateTime.UtcNow;
                List<RealTagValue> values = new List<RealTagValue>();
                int size = 0;
                foreach (var vv in updateHisValue.Values)
                {
                    if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Name))
                    {
                        size += 9;
                        var vtg = DirectAccessProxy.Proxy.TagCach[vv.Name];
                        RealTagValue val = new RealTagValue() { Id = vtg.Item1, Value = ConvertTo(vtg.Item2, (JsonElement)vv.Value), ValueType = vtg.Item2, Quality = 0 };
                        values.Add(val);
                    }
                }
                var re =  DirectAccessProxy.Proxy.UpdateAreaHisData(time, values);
                if (mDeviceTopics.ContainsKey(updateHisValue.Token))
                {
                    string dtopic = mDeviceTopics[updateHisValue.Token];
                    SendToTopicData(dtopic, new BoolReponse() { CallId = updateHisValue.CallId, Result = true });
                }
                return re;
            }
            return false;
        }
        /// <summary>
        /// 
        /// </summary>
        /// <param name="updateHisValue"></param>
        private bool ProcessUpdateHisValue(UpdateHisValueRequest updateHisValue)
        {
            if (updateHisValue != null && SecurityManager.Manager.IsLogin(updateHisValue.Token))
            {
                SecurityManager.Manager.RefreshLogin(updateHisValue.Token);
                bool re = false;
                int size = 0;

                foreach (var vv in updateHisValue.Values)
                {
                    if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Name))
                    {
                        size += 9;
                        var vtg = DirectAccessProxy.Proxy.TagCach[vv.Name];
                        switch (vtg.Item2)
                        {
                            case (byte)(Cdy.Tag.TagType.Bool):
                                size += vv.Values.Length * (8 + 1 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.Byte):
                                size += vv.Values.Length * (8 + 1 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.Short):
                                size += vv.Values.Length * (8 + 2 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.UShort):
                                size += vv.Values.Length * (8 + 2 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.Int):
                            case (byte)(Cdy.Tag.TagType.UInt):
                                size += vv.Values.Length * (8 + 4 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.Long):
                            case (byte)(Cdy.Tag.TagType.ULong):
                                size += vv.Values.Length * (8 + 8 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.Double):
                                size += vv.Values.Length * (8 + 8 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.Float):
                                size += vv.Values.Length * (8 + 4 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.String):
                                size += vv.Values.Length * (8 + 258 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.DateTime):
                                size += vv.Values.Length * (8 + 8 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.IntPoint):
                                size += vv.Values.Length * (8 + 8 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.IntPoint3):
                                size += vv.Values.Length * (8 + 12 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.UIntPoint):
                                size += vv.Values.Length * (8 + 8 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.UIntPoint3):
                                size += vv.Values.Length * (8 + 12 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.LongPoint):
                                size += vv.Values.Length * (8 + 16 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.LongPoint3):
                                size += vv.Values.Length * (8 + 24 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.ULongPoint):
                                size += vv.Values.Length * (8 + 16 + 1);
                                break;
                            case (byte)(Cdy.Tag.TagType.ULongPoint3):
                                size += vv.Values.Length * (8 + 24 + 1);
                                break;
                        }
                    }
                }
                using (DirectAccessDriver.ClientApi.HisDataBuffer hdb = new DirectAccessDriver.ClientApi.HisDataBuffer(size))
                {
                    int valuegroupcount = 0;
                    lock (DirectAccessProxy.Proxy.HisSyncLocker)
                    {
                        foreach (var vv in updateHisValue.Values)
                        {
                            if (DirectAccessProxy.Proxy.TagCach.ContainsKey(vv.Name))
                            {
                                valuegroupcount++;
                                var vtg = DirectAccessProxy.Proxy.TagCach[vv.Name];
                                hdb.Write(vtg.Item1);
                                hdb.Write(vv.Values.Length);
                                hdb.Write((byte)vtg.Item2);

                                foreach (var val in vv.Values)
                                {
                                    hdb.Write(val.Time);
                                    SetTagValueToBuffer2((TagType)vtg.Item2, ConvertTo(vtg.Item2, (JsonElement)val.Value), 0, hdb);
                                }
                            }
                        }
                    }
                  
                    if (hdb.Position > 0)
                    {
                        re = DirectAccessProxy.Proxy.UpdateHisData(hdb, valuegroupcount);
                    }
                }

                if (mDeviceTopics.ContainsKey(updateHisValue.Token))
                {
                    string dtopic = mDeviceTopics[updateHisValue.Token];
                    SendToTopicData(dtopic, new BoolReponse() { CallId = updateHisValue.CallId, Result = true });
                }

                return re;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        /// <param name="quality"></param>
        /// <param name="re"></param>
        private void SetTagValueToBuffer2(TagType type, object value, byte quality, DirectAccessDriver.ClientApi.HisDataBuffer re)
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

        private void ProcessRegistorCallBack(RegistorCallBackRequest callback)
        {
            if (callback != null)
            {
                if (SecurityManager.Manager.IsLogin(callback.Token))
                {
                    foreach (var vv in callback.Tags)
                    {
                        lock (mRegistorTags)
                        {
                            if (mRegistorTags.ContainsKey(vv))
                            {
                                mRegistorTags[vv] = callback.Token;
                            }
                            else
                            {
                                mRegistorTags.Add(vv, callback.Token);
                            }
                        }
                    }
                }
            }
        }

        public void SetTagValueToDevice(string tag,object value)
        {
            string rt = string.Empty;
            lock (mRegistorTags)
            {
                if (mRegistorTags.ContainsKey(tag))
                {
                    rt = mRegistorTags[tag];
                }
            }

            if (!string.IsNullOrEmpty(rt))
            {
                DeviceValueSetRequest dvs = new DeviceValueSetRequest();
                dvs.Tag = tag;
                dvs.Value = value.ToString();
                dvs.CallId = new Guid().ToString();

                if (mDeviceTopics.ContainsKey(rt))
                {
                    SendToTopicData(mDeviceTopics[rt], dvs);
                }
                else
                {
                    lock (mRegistorTags)
                        mRegistorTags.Remove(tag);
                }
            }
        }

        private void ProcessSetTagValueDeviceCallBack(DeviceSetResponse dr)
        {
            if(dr!=null)
            {
                if (dr.Result)
                {
                    LoggerService.Service.Info("MqttServer", $"set tag {dr.Tag} value to device {dr.DeviceTopic}  successful.");
                }
                else
                {
                    LoggerService.Service.Warn("MqttServer", $"set tag {dr.Tag} value to device {dr.DeviceTopic}  failed.");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="responeTopic"></param>
        /// <param name="data"></param>
        private void SendToTopicData(string topic, string responeTopic, byte[] data)
        {
            try
            {
                var msg = new MqttApplicationMessageBuilder().WithTopic(topic).WithResponseTopic(responeTopic).WithPayload(data).WithQualityOfServiceLevel(MQTTnet.Protocol.MqttQualityOfServiceLevel.AtLeastOnce).Build();
                this.mqttClient.PublishAsync(msg);
            }
            catch(Exception ex) 
            {
                LoggerService.Service.Warn("MqttServer", $"{ex.Message} {ex.StackTrace}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="data"></param>
        private void SendToTopicData(string topic, object data)
        {
            SendToTopicData(topic, "", Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data)));
        }

        private MQTTnet.Server.MqttServer mServer;


        /// <summary>
        /// 
        /// </summary>
        private void StartLocalServer()
        {
            if (mServer != null) return;

            var mf = new MqttFactory();
            MqttServerOptions ms = mf.CreateServerOptionsBuilder().WithDefaultEndpoint().Build();
            ms.DefaultEndpointOptions.Port = Config.Instance.InnerServer.Port;
            
            this.mServer = mf.CreateMqttServer(ms);
            mServer.ValidatingConnectionAsync += MServer_ValidatingConnectionAsync;
           

            string storePath = System.IO.Path.Combine(PathHelper.helper.AppPath,"MqttCach");
            if(!System.IO.Directory.Exists(storePath))
            {
                System.IO.Directory.CreateDirectory(storePath);
            }
            storePath = System.IO.Path.Combine(storePath, "mqttservercach");

            mServer.LoadingRetainedMessageAsync += async eventArgs =>
            {
                try
                {
                    if(System.IO.File.Exists(storePath))
                    {
                        eventArgs.LoadedRetainedMessages = await JsonSerializer.DeserializeAsync<List<MqttApplicationMessage>>(File.OpenRead(storePath));
                        Console.WriteLine("Retained messages loaded.");
                    }
                    
                }
                catch (FileNotFoundException)
                {
                    // Ignore because nothing is stored yet.
                    Console.WriteLine("No retained messages stored yet.");
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                }
            };

            // Make sure to persist the changed retained messages.
            mServer.RetainedMessageChangedAsync += async eventArgs =>
            {
                try
                {
                    var buffer = JsonSerializer.SerializeToUtf8Bytes(eventArgs.StoredRetainedMessages);
                    await File.WriteAllBytesAsync(storePath, buffer);
                    Console.WriteLine("Retained messages saved.");
                }
                catch (Exception exception)
                {
                    Console.WriteLine(exception);
                }
            };

            // Make sure to clear the retained messages when they are all deleted via API.
            mServer.RetainedMessagesClearedAsync += _ =>
            {
                if(File.Exists(storePath))
                {
                    try
                    {
                        File.Delete(storePath);
                    }
                    catch
                    {

                    }
                }
              
                return Task.CompletedTask;
            };

            mServer.StartAsync();
        }

        private Task MServer_ValidatingConnectionAsync(ValidatingConnectionEventArgs arg)
        {
            if (arg.ClientId.Length < 10)
            {
                arg.ReasonCode = MqttConnectReasonCode.ClientIdentifierNotValid;
            }
            else if (arg.Password != Config.Instance.InnerServer.Password || arg.UserName!=Config.Instance.InnerServer.UserName)
            {
                arg.ReasonCode = MQTTnet.Protocol.MqttConnectReasonCode.BadUserNameOrPassword;
            }
            else
            {
                arg.ReasonCode = MQTTnet.Protocol.MqttConnectReasonCode.Success;
            }
            return Task.CompletedTask;
        }
    }
}
