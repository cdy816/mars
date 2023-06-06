using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace Cdy.Tag.Common
{
    /// <summary>
    /// 线程池
    /// </summary>
    public class ThreadPool:IDisposable
    {

        #region ... Variables  ...
        
        /// <summary>
        /// 
        /// </summary>
        public static ThreadPool instance = new ThreadPool();

        /// <summary>
        /// 
        /// </summary>
        private List<ThreadExecuter> mExecuter = new List<ThreadExecuter>();

        private object Locker = new object();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 线程个数
        /// </summary>
        public Byte ThreadCount { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; } = "";

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        /// <param name="action"></param>
        /// <param name="parameter"></param>
        public void Registor(Action<object[]> action,params object[] parameter)
        {
            lock (Locker)
            {
                ThreadExecuter sel = null;
                int max = int.MaxValue;
                foreach (ThreadExecuter executer in mExecuter)
                {
                    if (executer.WaitCount < max)
                    {
                        max = executer.WaitCount;
                        sel = executer;
                    }
                }

                if (sel != null)
                {
                    sel.AddExecuter(new ThreadContext() { action = action, parameter = parameter });
                }
            }

        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            lock (Locker)
            {
                mExecuter.Clear();
                for (int i = 0; i < ThreadCount; i++)
                {
                    mExecuter.Add(new ThreadExecuter() { Name=this.Name+"_"+i});
                }

                foreach(var vv in mExecuter)
                {
                    vv.Start();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            lock(Locker)
            {
                foreach(var vv in mExecuter)
                {
                    vv.Close();
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            if (mExecuter != null)
            {
                foreach (var vv in mExecuter)
                {
                    vv.Dispose();
                }
                mExecuter.Clear();
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }

    /// <summary>
    /// 
    /// </summary>
    public struct ThreadContext
    {
        /// <summary>
        /// 
        /// </summary>
        public Action<object[]> action { get; set; }
        /// <summary>
        /// 
        /// </summary>
        public object[] parameter { get; set; }
    }

    /// <summary>
    /// 
    /// </summary>
    public class ThreadExecuter:IDisposable
    {
        private ManualResetEvent mEvent = new ManualResetEvent(false);

        private bool mIsClosed = false;

        private Thread mThread;

        /// <summary>
        /// 
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public int WaitCount { get { return Queue.Count; } }


        /// <summary>
        /// 
        /// </summary>
        public Queue<ThreadContext> Queue { get; set; } = new Queue<ThreadContext>();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="ctx"></param>
        public void AddExecuter(ThreadContext ctx)
        {
            lock(Queue)
            {
                Queue.Enqueue(ctx);
                mEvent.Set();
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void ThreadPro()
        {
            while(!mIsClosed)
            {
                mEvent.WaitOne();
                mEvent.Reset();
                if (mIsClosed) break;
                if (Queue.Count > 0)
                {
                    ThreadContext tc = Queue.Peek();
                    tc.action(tc.parameter);
                    lock (Queue)
                    {
                        tc = Queue.Dequeue();
                    }
                    if (Queue.Count > 0)
                        Thread.Sleep(1);
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            mThread = new Thread(ThreadPro);
            mThread.IsBackground = true;
            mThread.Start();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Close()
        {
            mIsClosed = true;
            mEvent.Set();

            while (mThread.IsAlive) Thread.Sleep(1);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Dispose()
        {
            mEvent.Dispose();
            while(Queue.Count>0)
            {
                var vv = Queue.Dequeue();
                if(vv.parameter !=null)
                {
                    foreach (var vvp in vv.parameter)
                    {
                        if (vvp is IDisposable)
                        {
                            (vvp as IDisposable).Dispose();
                        }
                    }
                }
            }
        }

    }

}
