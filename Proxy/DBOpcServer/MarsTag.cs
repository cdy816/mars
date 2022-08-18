using Opc.Ua;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBOpcServer
{
    internal class MarsTag : BaseDataVariableState
    {
        public MarsTag(NodeState parent) : base(parent)
        {
        }

        /// <summary>
        /// 
        /// </summary>
        public int Id { get; set; }
    }
}
