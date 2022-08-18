using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace RuntimeServiceImp
{
    public class RuntimeServiceManager:DBRuntimeAPI.ILocalResourceService
    {
        public static RuntimeServiceManager Instance = new RuntimeServiceManager();

        private DatabaseManager database = new DatabaseManager();

        private LocalResourceMonitor localResource = new LocalResourceMonitor();

        /// <summary>
        /// 
        /// </summary>
        private Thread mThread;

        /// <summary>
        /// 
        /// </summary>
        public string OSVersion => LocalResourceMonitor.OSVersion;

        /// <summary>
        /// 
        /// </summary>
        public string DotnetVersion => LocalResourceMonitor.DotnetVersion;

        /// <summary>
        /// 
        /// </summary>
        public int ProcessCount => LocalResourceMonitor.ProcessCount;

        /// <summary>
        /// 
        /// </summary>
        public bool Is64Bit => LocalResourceMonitor.Is64Bit;

        /// <summary>
        /// 
        /// </summary>
        public string MachineName => LocalResourceMonitor.MachineName;

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            database.Init();
            Cdy.Tag.ServiceLocator.Locator.Registor<DBRuntimeAPI.IDatabaseService>(database);
            Cdy.Tag.ServiceLocator.Locator.Registor<DBRuntimeAPI.ILogService>(database);
            Cdy.Tag.ServiceLocator.Locator.Registor<DBRuntimeAPI.ILocalResourceService>(this);

            mThread = new Thread(LocalResourceProcess);
            mThread.IsBackground = true;
            mThread.Start();
        }
        
        private void LocalResourceProcess()
        {
            while (true)
            {
                localResource.Update();
                Thread.Sleep(1000);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public double CPU()
        {
            return localResource.CPU;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Tuple<double, double> Memory()
        {
            return new Tuple<double, double>(localResource.TotalMemory-localResource.FreeMemory,localResource.TotalMemory);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public Tuple<double, double> Network()
        {
            return new Tuple<double, double>(localResource.NetworkReceiveBytes, localResource.NetworkSendBytes);
        }
    }
}
