using System;
using System.Collections.Generic;
using System.Text;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class ComplexTagClassDocument
    {
        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Version { get; set; } = "0.1";

        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string, ComplexTagClass> mClass = new Dictionary<string, ComplexTagClass>();   

        /// <summary>
        /// 
        /// </summary>
        public Dictionary<string, ComplexTagClass> Class
        {
            get { return mClass; }
            set { mClass = value; }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public string CheckAndGetAvaiableClassName(string name)
        {
            if(!mClass.ContainsKey(name))
            {
                return name;
            }
            else
            {
                string stmp = name + "1";
                return CheckAndGetAvaiableClassName(stmp);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cls"></param>
        public void AddClass(ComplexTagClass cls)
        {
            cls.Owner = this;
            if(!mClass.ContainsKey(cls.Name))
            {
                mClass.Add(cls.Name, cls);
            }
            else
            {
                mClass[cls.Name].Owner = null;
                mClass[cls.Name] = cls;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public bool RemoveClass(string name)
        {
            if(mClass.ContainsKey(name))
            {
                mClass[name].Owner = null;
                mClass.Remove(name);
                return true;
            }
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldname"></param>
        /// <param name="newname"></param>
        public bool ReNameClass(string oldname,string newname)
        {
            if (mClass.ContainsKey(oldname)&&!mClass.ContainsKey(newname))
            {
                var mc = mClass[oldname];
                mc.Name= newname;
                mClass.Remove(oldname);
                mClass.Add(mc.Name, mc);
                return true;
            }
            return false;
        }

    }
}
