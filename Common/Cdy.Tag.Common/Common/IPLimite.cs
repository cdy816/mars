using System;
using System.Collections.Generic;
using System.Text;
using System.Xml.Linq;

namespace Cdy.Tag
{

    public  class IPLimite
    {
        /// <summary>
        /// 
        /// </summary>
        public List<string> AllowedIps { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<string> RejectedIps{ get; set; }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sfile"></param>
        public void LoadFromXML(string sfile)
        {
            XElement xe = XElement.Load(sfile);
            if(xe.Element("AllowedIps")!=null)
            {
                string sval = xe.Element("AllowedIps").Value;
                if(!string.IsNullOrEmpty(sval))
                {
                    AllowedIps = new List<string>();
                    AllowedIps.AddRange(sval.Split(";"));
                }
            }
            if (xe.Element("RejectedIps") != null)
            {
                string sval = xe.Element("RejectedIps").Value;
                if (!string.IsNullOrEmpty(sval))
                {
                    RejectedIps = new List<string>();
                    RejectedIps.AddRange(sval.Split(";"));
                }
            }
        }

        /// <summary>
        /// IP是否被拒绝
        /// </summary>
        /// <param name="ip"></param>
        /// <returns></returns>
        public bool IsRejects(string ip)
        {
            if (AllowedIps != null && AllowedIps.Count>0)
            {
                return !AllowedIps.Contains(ip);
            }
            else
            {
                if (RejectedIps != null && RejectedIps.Count>0)
                {
                    return RejectedIps.Contains(ip);
                }
                else
                {
                    return false;
                }
            }
        }

        /// <summary>
        /// 是否允许
        /// </summary>
        /// <param name="ip"></param>
        /// <returns></returns>
        public bool IsAllowed(string ip)
        {
            return !IsRejects(ip);
        }
    }
}
