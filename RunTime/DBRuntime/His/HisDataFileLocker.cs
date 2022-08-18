using Cdy.Tag;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DBRuntime.His
{
    public class HisDataFileLocker
    {
        /// <summary>
        /// 
        /// </summary>
        public static HisDataFileLocker Locker = new HisDataFileLocker();

        private Dictionary<string,bool> mLockObjs = new Dictionary<string,bool>();

        private object mLocker = new object();

        private object mRelaseLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Take(string file)
        {
          
            lock (mLocker)
            {
                if (mLockObjs.ContainsKey(file))
                {

                    while(mLockObjs[file])
                    {
                        Thread.Sleep(10);
                    }

                    mLockObjs[file] = true;
                }
                else
                {
                    mLockObjs.Add(file, true);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        public void Relase(string file)
        {
          
            lock (mRelaseLocker)
            {
                if(mLockObjs.ContainsKey(file))
                {
                    mLockObjs[file] = false;
                }
                else
                {
                    mLockObjs.Add(file,false);
                }
             
            }
        }
    }
}
