using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag.Common
{
    /// <summary>
    /// 
    /// </summary>
    public class ClientAuthorization
    {
        /// <summary>
        /// 
        /// </summary>
        public static ClientAuthorization Instance = new ClientAuthorization();

        /// <summary>
        /// 允许的IP地址
        /// </summary>
        public List<string> AllowIps { get; set; }

        /// <summary>
        /// 允许的程序的HashCode值
        /// </summary>
        public List<string> AllowApplication { get; set; }

        /// <summary>
        /// 使能白名单策略
        /// </summary>
        public bool EnableAllows { get; set; }=false;


        /// <summary>
        /// 禁止的IP地址
        /// </summary>
        public List<string> ForbiddenIps { get; set; }

        /// <summary>
        /// 禁止的程序的HashCode值
        /// </summary>
        public List<string> ForbiddenApplication { get; set; }

        /// <summary>
        /// 使能黑名单策略
        /// </summary>
        public bool EnableForbidden { get; set; } = true;

        /// <summary>
        /// 检查IP可用
        /// </summary>
        /// <param name="ip"></param>
        /// <returns></returns>
        public bool CheckIp(string ip)
        {
            if (EnableAllows && AllowIps!=null)
            {
                return AllowIps.Contains(ip);
            }
            if(EnableForbidden && ForbiddenIps!=null)
            {
                return !ForbiddenIps.Contains(ip);
            }
            return true;
        }

        /// <summary>
        /// 检查程序的HashCode 是否可用
        /// </summary>
        /// <param name="code"></param>
        /// <returns></returns>
        public bool CheckApplication(string code)
        {
            if (EnableAllows)
            {
                return AllowApplication.Contains(code);
            }
            if (EnableForbidden)
            {
                return !ForbiddenApplication.Contains(code);
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        public void LoadAllowFromXML(XElement xe)
        {
            if(xe.Attribute("Enable") !=null)
            {
                this.EnableAllows = bool.Parse(xe.Attribute("Enable").Value);
            }

            if (xe.Attribute("Ips") != null)
            {
                this.AllowIps = xe.Attribute("Ips").Value.Split(",",StringSplitOptions.RemoveEmptyEntries).ToList();
            }

            if (xe.Attribute("ApplicationCode") != null)
            {
                this.AllowApplication = xe.Attribute("ApplicationCode").Value.Split(",", StringSplitOptions.RemoveEmptyEntries).ToList();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="xe"></param>
        public void LoadForbiddenFromXML(XElement xe)
        {
            if (xe.Attribute("Enable") != null)
            {
                this.EnableForbidden = bool.Parse(xe.Attribute("Enable").Value);
            }

            if (xe.Attribute("Ips") != null)
            {
                this.ForbiddenIps = xe.Attribute("Ips").Value.Split(",", StringSplitOptions.RemoveEmptyEntries).ToList();
            }

            if (xe.Attribute("ApplicationCode") != null)
            {
                this.ForbiddenApplication = xe.Attribute("ApplicationCode").Value.Split(",", StringSplitOptions.RemoveEmptyEntries).ToList();
            }
        }

    }
}
