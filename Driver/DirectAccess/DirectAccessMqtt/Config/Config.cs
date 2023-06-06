using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Xml.Linq;

namespace DirectAccessMqtt
{
    /// <summary>
    /// 
    /// </summary>
    public class Config
    {
        /// <summary>
        /// 
        /// </summary>
        public static Config Instance = new Config();

        /// <summary>
        /// 使用内部MQTT服务器
        /// </summary>
        public bool UseInnerMqttServer { get; set; }

        /// <summary>
        /// Server ip
        /// </summary>
        public string RemoteMqttServer { get; set; }

        /// <summary>
        /// Port
        /// </summary>
        public int RemoteMqttPort { get; set; } = 9801;

        /// <summary>
        /// 用户名
        /// </summary>
        public string UserName { get; set; }

        /// <summary>
        /// 密码
        /// </summary>
        public string Password { get; set; }

        /// <summary>
        /// Tls
        /// </summary>
        public bool UseTls { get; set; }

        /// <summary>
        /// 协议
        /// </summary>
        public string ProtocolVersion { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public LocalServer InnerServer { get; set; }=new LocalServer();

        /// <summary>
        /// 
        /// </summary>
        public Config Load()
        {
            string sfileName = Assembly.GetEntryAssembly().Location;
            string spath = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(sfileName), "Config");
            sfileName = System.IO.Path.Combine(spath, System.IO.Path.GetFileNameWithoutExtension(sfileName) + ".cfg");

            if (System.IO.File.Exists(sfileName))
            {
                XElement xx = XElement.Load(sfileName);
            
           
                if (xx.Attribute("UseInnerMqttServer") != null)
                {
                    UseInnerMqttServer = bool.Parse(xx.Attribute("UseInnerMqttServer").Value);
                }

                if (xx.Attribute("RemoteMqttPort") != null)
                {
                    RemoteMqttPort = int.Parse(xx.Attribute("RemoteMqttPort").Value);
                }

                if (xx.Attribute("RemoteMqttServer") != null)
                {
                    RemoteMqttServer = xx.Attribute("RemoteMqttServer").Value;
                }

                if (xx.Attribute("UserName") != null)
                {
                    UserName = xx.Attribute("UserName").Value;
                }

                if (xx.Attribute("Password") != null)
                {
                    Password = xx.Attribute("Password").Value;
                }

                if (xx.Attribute("UseTls") != null)
                {
                    UseTls = bool.Parse(xx.Attribute("UseTls").Value);
                }

                if (xx.Attribute("ProtocolVersion") != null)
                {
                    ProtocolVersion = xx.Attribute("ProtocolVersion").Value;
                }               

                if (xx.Element("InnerMqttServer") !=null)
                {
                    var xxx = xx.Element("InnerMqttServer");
                    if(xxx!=null)
                    {
                        if (xxx.Attribute("Port") != null)
                        {
                            InnerServer.Port = int.Parse(xxx.Attribute("Port").Value);
                        }

                        if (xxx.Attribute("UserName") != null)
                        {
                            InnerServer.UserName = xxx.Attribute("UserName").Value;
                        }

                        if (xxx.Attribute("Password") != null)
                        {
                            InnerServer.Password = xxx.Attribute("Password").Value;
                        }
                    }

                }
            }

            return this;
        }
    }

    public class LocalServer
    {
        /// <summary>
        /// 
        /// </summary>
        public int Port { get; set; } = 9801;

        /// <summary>
        /// 
        /// </summary>
        public string UserName { get; set; } = "Admin";

        /// <summary>
        /// 
        /// </summary>
        public string Password { get; set; } = "Admin";
    }
}
