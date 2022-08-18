using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntimeAPI
{
    public interface ILocalResourceService
    {
        /// <summary>
        /// 
        /// </summary>
        string OSVersion { get;  }

        /// <summary>
        /// 
        /// </summary>
        string DotnetVersion { get;  }

        /// <summary>
        /// 
        /// </summary>
        int ProcessCount { get;  }

        /// <summary>
        /// 
        /// </summary>
        bool Is64Bit { get;  }

        /// <summary>
        /// 
        /// </summary>
        string MachineName { get; }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        double CPU();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        Tuple<double, double> Memory();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        Tuple<double, double> Network();
    }
}
