using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBRuntimeMonitor
{
    public class DatabaseDocument
    {
        /// <summary>
        /// 
        /// </summary>
        public static DatabaseDocument Instance = new DatabaseDocument();

        private List<Database> mDatabase = new List<Database>();

        /// <summary>
        /// 
        /// </summary>
        public List<Database> Databases
        {
            get { return mDatabase; }
            set
            {
                mDatabase = value;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void RemoveDatabase(Database database)
        {
            if(mDatabase.Contains(database))
            {
                mDatabase.Remove(database);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        public void AddDatabase(Database database)
        {
            mDatabase.Add(database);
        }
    }
}
