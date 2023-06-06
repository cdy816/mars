using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Formatter;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace DirectAccessMqtt.Client
{
    /// <summary>
    /// 
    /// </summary>
    public class Client
    {

        #region ... Variables  ...
        private IMqttClient mqttClient;
        private string mToken = string.Empty;

        private Dictionary<string, ManualResetEvent> mReponseCach = new Dictionary<string, ManualResetEvent>();

        public event Action<bool> LoginChangedEvent;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        
        /// <summary>
        /// 
        /// </summary>
        public Client()
        {
            ClientTopic = Guid.NewGuid().ToString();
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 登录Broker用户名
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// 登录Broker密码
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// 登录服务器用户名
        /// </summary>
        public string ServerUser { get; set; }

        /// <summary>
        /// 登录服务器密码
        /// </summary>
        public string ServerPassword { get; set; }

        /// <summary>
        /// 客户端Topic
        /// </summary>
        public string ClientTopic { get; set; }

        /// <summary>
        /// 服务器Topic
        /// </summary>
        public string ServerTopic { get; set; }

        /// <summary>
        /// 服务器地址
        /// </summary>
        public string Server { get; set; }

        /// <summary>
        /// 端口
        /// </summary>
        public int Port { get; set; }

        /// <summary>
        /// 建立连接
        /// </summary>
        public bool IsConnected { get; set; }=false;

        /// <summary>
        /// 登录成功
        /// </summary>
        public bool IsLogin { get; set; }

        public Func<string,object,bool> ServerSetValueCallBack { get; set; }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 更新实时历史数据
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool UpdateData(Dictionary<string,object> data,int timeout=5000)
        {
            try
            {
                var callid = new Guid().ToString();

                UpdateDeviceValueRequest udv = new UpdateDeviceValueRequest();
                udv.Token = mToken;
                udv.CallId = callid;
                udv.Values = data.Select(x => new RealValue() { Name = x.Key, Value = x.Value }).ToArray();
                SendToTopicData(ServerTopic, udv);
                ManualResetEvent mre = new ManualResetEvent(false);
                lock (mReponseCach)
                {
                    if(!mReponseCach.ContainsKey(callid))
                    mReponseCach.Add(callid, mre);
                    else
                    {
                        mReponseCach[callid] = mre;
                    }
                }
                if (mre.WaitOne(timeout))
                {
                    return true;
                }
                mre.Dispose();
            }
            catch
            {

            }
            return false;
        }


        /// <summary>
        /// 按区域更新实时历史数据
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool UpdateAreaData(Dictionary<string, object> data, int timeout = 5000)
        {
            try
            {
                var callid = new Guid().ToString();

                UpdateAreaDeviceValueRequest udv = new UpdateAreaDeviceValueRequest();
                udv.Token = mToken;
                udv.CallId = callid;
                udv.Values = data.Select(x => new RealValue() { Name = x.Key, Value = x.Value }).ToArray();
                SendToTopicData(ServerTopic, udv);
                ManualResetEvent mre = new ManualResetEvent(false);
                lock (mReponseCach)
                {
                    if (!mReponseCach.ContainsKey(callid))
                        mReponseCach.Add(callid, mre);
                    else
                    {
                        mReponseCach[callid] = mre;
                    }
                }
                if (mre.WaitOne(timeout))
                {
                    return true;
                }
                mre.Dispose();

                lock (mReponseCach)
                {
                    if (mReponseCach.ContainsKey(callid))
                        mReponseCach.Remove(callid);
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 补录历史数据
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool UpdateHisData(string tag ,Dictionary<DateTime, object> data, int timeout = 5000)
        {
            try
            {
                var callid = new Guid().ToString();

                UpdateHisValueRequest udv = new UpdateHisValueRequest();
                udv.Token = mToken;
                udv.CallId = callid;
                udv.Values = new HisValue[] {new HisValue() { Name = tag,Values = data.Select((x) => new HisValueItem() { Time=x.Key,Value=x.Value }).ToArray()} };
                SendToTopicData(ServerTopic, udv);
                ManualResetEvent mre = new ManualResetEvent(false);
                lock (mReponseCach)
                {
                    if (!mReponseCach.ContainsKey(callid))
                        mReponseCach.Add(callid, mre);
                    else
                    {
                        mReponseCach[callid] = mre;
                    }
                }
                if (mre.WaitOne(timeout))
                {
                    return true;
                }
                mre.Dispose();

                lock (mReponseCach)
                {
                    if (mReponseCach.ContainsKey(callid))
                        mReponseCach.Remove(callid);
                }
            }
            catch
            {

            }
            return false;
        }


        /// <summary>
        /// 补录历史数据
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool UpdateHisData(Dictionary<string, Dictionary<DateTime, object>> data, int timeout = 5000)
        {
            try
            {
                var callid = new Guid().ToString();

                UpdateHisValueRequest udv = new UpdateHisValueRequest();
                udv.Token = mToken;
                udv.CallId = callid;

                HisValue[] vv = new HisValue[data.Count];
                int i = 0;
                foreach(var vvv in data)
                {
                    vv[i] = new HisValue()
                    {
                        Name = vvv.Key,
                        Values = vvv.Value.Select((x) => new HisValueItem() { Time = x.Key, Value = x.Value }).ToArray()
                    };
                }

                udv.Values = vv;
                SendToTopicData(ServerTopic, udv);
                ManualResetEvent mre = new ManualResetEvent(false);
                lock (mReponseCach)
                {
                    if (!mReponseCach.ContainsKey(callid))
                        mReponseCach.Add(callid, mre);
                    else
                    {
                        mReponseCach[callid] = mre;
                    }
                }
                if (mre.WaitOne(timeout))
                {
                    return true;
                }
                mre.Dispose();

                lock (mReponseCach)
                {
                    if (mReponseCach.ContainsKey(callid))
                        mReponseCach.Remove(callid);
                }
            }
            catch
            {

            }
            return false;
        }

        /// <summary>
        /// 注册允许回设的变量
        /// </summary>
        /// <param name="data"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public bool RegistorTag(List<string> tags, int timeout = 5000)
        {
            try
            {
                var callid = new Guid().ToString();
                RegistorCallBackRequest udv = new RegistorCallBackRequest();
                udv.Token = mToken;
                udv.CallId = callid;
                udv.Tags = tags.ToArray();
                SendToTopicData(ServerTopic, udv);
                ManualResetEvent mre = new ManualResetEvent(false);
                lock (mReponseCach)
                {
                    if (!mReponseCach.ContainsKey(callid))
                        mReponseCach.Add(callid, mre);
                    else
                    {
                        mReponseCach[callid] = mre;
                    }
                }
                if (mre.WaitOne(timeout))
                {
                    return true;
                }
                mre.Dispose();

                lock (mReponseCach)
                {
                    if (mReponseCach.ContainsKey(callid))
                        mReponseCach.Remove(callid);
                }
            }
            catch
            {

            }
            finally
            {
            }
            return false;
        }

        /// <summary>
        /// 连接服务器
        /// </summary>
        public void Connect()
        {
            StartClient();
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


            var options = new MqttClientOptions
            {
                ClientId = Guid.NewGuid().ToString(),
                ProtocolVersion = MqttProtocolVersion.V500,
                ChannelOptions = new MqttClientTcpOptions
                {
                    Server = Server,
                    Port = Port,
                    TlsOptions = tlsOptions
                }
            };

            options.Credentials = new MqttClientCredentials(UserName, Encoding.UTF8.GetBytes(Password));
            options.CleanSession = true;
            options.KeepAlivePeriod = TimeSpan.FromMilliseconds(5000);
            var mqttFactory = new MqttFactory();
            mqttClient = mqttFactory.CreateMqttClient();
            mqttClient.ApplicationMessageReceivedAsync += MqttClient_ApplicationMessageReceivedAsync;
            mqttClient.ConnectedAsync += MqttClient_ConnectedAsync;
            mqttClient.DisconnectedAsync += MqttClient_DisconnectedAsync;

            mqttClient.ConnectAsync(options).Wait();
        }

        /// <summary>
        /// 登录
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        private void Login(string username,string password)
        {
            LoginRequest ll = new LoginRequest() { DeviceId=ClientTopic,UserName=username, Password=password};
            SendToTopicData(ServerTopic, ll);
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
            catch (Exception ex)
            {
                //LoggerService.Service.Warn("MqttServer", $"{ex.Message} {ex.StackTrace}");
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="topic"></param>
        /// <param name="data"></param>
        private void SendToTopicData(string topic,object data)
        {
            SendToTopicData(topic,"",Encoding.UTF8.GetBytes(JsonSerializer.Serialize(data)));
        }


        private Task MqttClient_DisconnectedAsync(MqttClientDisconnectedEventArgs arg)
        {
            IsConnected = false;
            IsLogin=false;
            LoginChangedEvent?.Invoke(IsLogin);
            return Task.CompletedTask;
        }

        private Task MqttClient_ConnectedAsync(MqttClientConnectedEventArgs arg)
        {
            IsConnected=true;
            mqttClient.SubscribeAsync(this.ClientTopic);
            Task.Run(() =>
            {
                Login(ServerUser, ServerPassword);
            });
           
            return Task.CompletedTask;
        }

        private Task MqttClient_ApplicationMessageReceivedAsync(MqttApplicationMessageReceivedEventArgs arg)
        {
            if (arg.ApplicationMessage.Payload != null)
            {
                var node = JsonNode.Parse(arg.ApplicationMessage.Payload);
                if (node["Fun"] != null)
                {
                    var fun = node["Fun"].ToString().ToLower();
                    switch(fun)
                    {
                        case "0":
                            var lre = JsonSerializer.Deserialize<LoginResponse>(node);
                            mToken = lre.Token;
                            IsLogin = !string.IsNullOrEmpty(mToken);
                            Task.Run(() => {
                                LoginChangedEvent?.Invoke(IsLogin);
                            });
                            break;
                        case "1":
                            var dvs = JsonSerializer.Deserialize<DeviceValueSetRequest>(node);
                            var re = false;
                            if(dvs!=null)
                            {
                                if(ServerSetValueCallBack!=null)
                                {
                                     re = ServerSetValueCallBack(dvs.Tag, dvs.Value);
                                }
                                DeviceSetResponse deviceSetResponse = new DeviceSetResponse() { Result = re, CallId = dvs.CallId,DeviceTopic=this.ClientTopic,Tag=dvs.Tag };
                                Task.Run(() => {
                                    SendToTopicData(ServerTopic, deviceSetResponse);
                                });
                            }
                            break;
                        case "2":
                            var bvs = JsonSerializer.Deserialize<BoolReponse>(node);
                            if(bvs!=null)
                            {
                                CheckResponse(bvs.CallId,bvs.Result);
                            }
                            break;
                    }
                }
            }
            return Task.CompletedTask;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="id"></param>
        /// <param name="val"></param>
        private void CheckResponse(string id,bool val)
        {
            lock (mReponseCach)
            {
                try
                {
                    if (mReponseCach.ContainsKey(id))
                    {
                        if (!mReponseCach[id].SafeWaitHandle.IsClosed)
                        {
                            mReponseCach[id].Set();
                        }
                        else
                        {
                            mReponseCach.Remove(id);
                        }
                    }

                }catch
                {

                }
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
