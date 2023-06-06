using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntimeImp
{
    /// <summary>
    /// 
    /// </summary>
    public class ProcessMonitorManager
    {

        #region ... Variables  ...
        /// <summary>
        /// 
        /// </summary>
        private Dictionary<string,ProcessMonitorItem> mItems = new Dictionary<string,ProcessMonitorItem>();

        /// <summary>
        /// 
        /// </summary>
        public static ProcessMonitorManager Manager = new ProcessMonitorManager();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public ProcessMonitorManager()
        {
            Init();
        }
        #endregion ...Constructor...

        #region ... Properties ...

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            AddProcess("DBInRun");
            AddProcess("DBInStudioServer");
            AddProcess("DBHighApi");
            AddProcess("DBGrpcApi");
            AddProcess("DbWebApi");
            AddProcess("DBOpcServer");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        public void AddProcess(string name)
        {
            if(!mItems.ContainsKey(name))
            {
                ProcessMonitorItem pitem = new ProcessMonitorItem() { Name = name };
                lock(mItems)
                {
                    mItems.Add(name, pitem);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Update()
        {
            lock (mItems)
            {
                foreach (var vv in mItems.Values)
                {
                    vv.Update();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<ProcessMonitorItem> ListProcess()
        {
            return mItems.Values;
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }


    public class ProcessMonitorItem
    {

        private Cdy.Tag.Common.ProcessMemoryInfo pInfo;

        /// <summary>
        /// 
        /// </summary>
        public ProcessMonitorItem()
        {
            pInfo=new Cdy.Tag.Common.ProcessMemoryInfo();
        }

        /// <summary>
        /// 
        /// </summary>
        public string Name { get {return pInfo.Name; } set {pInfo.Name = value; } }

        /// <summary>
        /// 
        /// </summary>
        public Cdy.Tag.Common.ProcessMemoryInfo Info { get { return pInfo; } }

        /// <summary>
        /// 
        /// </summary>
        public bool IsOpened { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
            pInfo.Name= Name;
            if(pInfo.TryOpen())
            {
                IsOpened = true;
            }
            else
            {
                IsOpened = false;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Update()
        {
            if(!IsOpened)
            {
                IsOpened = pInfo.TryOpen();
            }
            else
            {
                IsOpened = pInfo.Read();
            }
        }

    }

}
