using Microsoft.VisualStudio.TestTools.UnitTesting;
using Cdy.Tag.Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Cdy.Tag.Common.Tests
{
    [TestClass()]
    public class ProcessMemoryInfoTests
    {
        [TestMethod()]
        public void AddClientTest()
        {
            ProcessMemoryInfo.Instances.Name= "test";
            ProcessMemoryInfo.Instances.New();
            ProcessMemoryInfo.Instances.RefreshMemory();
            ProcessMemoryInfo.Instances.AddClient("127.0.0.1", 80);
            

           var pm = new ProcessMemoryInfo() { Name = "test" };
            pm.TryOpen();
            pm.Read();
            Assert.IsTrue(pm.Clients.Count > 0);
        }
    }
}