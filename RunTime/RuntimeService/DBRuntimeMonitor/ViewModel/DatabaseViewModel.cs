using DBRuntimeMonitor.ViewModel;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DBRuntimeMonitor
{
    /// <summary>
    /// 
    /// </summary>
    public class DatabaseViewModel : HasChildrenTreeItemViewModel
    {

        #region ... Variables  ...
        private System.Timers.Timer? mConnectTimer;
        private bool mIsRunning = false;
        private ICommand mModifyCommand;
        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        public DatabaseViewModel(Database? database)
        {
            if (database != null)
            {
                this.Children.Add(new RootTagGroupViewModel() { Model = database, Parent = this, IsEnable = false });
                this.Children.Add(new LogViewModel() { Model = database, Parent = this });
                this.Model = database;
            }
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public Database? Model { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public override string Name { get => this.Model?.DatabseName; set => base.Name = value; }

        /// <summary>
        /// 
        /// </summary>
        public IParent Owner { get; set; }

        ///// <summary>
        ///// 
        ///// </summary>
        //private DBHighApi.ApiClient mDataClient;

        /// <summary>
        /// 
        /// </summary>
        private DBRuntimeServer.Client? mServerClient;

        ///// <summary>
        ///// 
        ///// </summary>
        //public DBHighApi.ApiClient DataClient
        //{
        //    get
        //    {
        //        return mDataClient;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        public DBRuntimeServer.Client ServerClient
        {
            get
            {
                return mServerClient;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsRunning
        {
            get
            {
                return mIsRunning;
            }
            set
            {
                if (mIsRunning != value)
                {
                    mIsRunning = value;
                    this.Children[0].IsEnable = value;
                    OnPropertyChanged("IsRunning");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand ModifyCommand
        {
            get
            {
                if(mModifyCommand == null)
                {
                    mModifyCommand = new RelayCommand(() => {
                        DatabaseManagerViewModel dmv = new DatabaseManagerViewModel(this.Name);
                        dmv.SetDatabase(Model);
                        if(dmv.ShowDialog().Value)
                        {
                            Owner?.SaveConfig();
                        }
                    });
                }
                return mModifyCommand;
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        public void Manager()
        {
            if (mServerClient != null)
            {
                mServerClient.CheckProcessStart("DBInStudioServer");
            }
            StartManagerClient();
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start()
        {
            if (mServerClient != null)
            {
                mServerClient.StartDatabase(Model.DatabseName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void Stop()
        {
            if (mServerClient != null)
            {
                mServerClient.StopDatabase(Model.DatabseName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReStart()
        {
            if (mServerClient != null)
            {
                mServerClient.ReStart(Model.DatabseName);
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Remove()
        {
            if (MessageBox.Show(String.Format(Res.Get("RemoveConfirm"),this.Name), Res.Get("Remove"), MessageBoxButton.YesNo) == MessageBoxResult.Yes)
            {
                Owner?.Remove(this);
                mConnectTimer?.Stop();
                mConnectTimer?.Dispose();
                mConnectTimer = null;

                mServerClient?.Dispose();
                mServerClient = null;
            }
            base.Remove();
        }

        /// <summary>
        /// 
        /// </summary>
        private void StartManagerClient()
        {
            var pps = Process.GetProcessesByName("DBInStudio");

            if (pps == null || pps.Length == 0)
            {
                try
                {
                    var vfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DBInStudio.exe");
                    ProcessStartInfo pinfo = new ProcessStartInfo();
                    pinfo.FileName = vfile;
                    if (Model != null)
                    {
                        pinfo.Arguments = $"{Model.HostAddress} {Model.DatabseName} {Model.UserName} {Model.Password}"; ;
                    }
                    Process.Start(pinfo).WaitForInputIdle(50000);
                }
                catch
                {

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override ViewModelBase GetModel(ViewModelBase mode)
        {
            if (mode is DatabaseDetailViewModel)
            {
                (mode as DatabaseDetailViewModel).Node = this;
                return mode;
            }
            else
            {
                return new DatabaseDetailViewModel() { Node = this };
            }

        }

        /// <summary>
        /// 
        /// </summary>
        private void CheckLocaServerIsRun()
        {
            if (this.Model.HostAddress == "127.0.0.1" || this.Model.HostAddress == "localhost")
            {
                var vpp = Process.GetProcessesByName("DBGuardian");
                if (vpp.Length == 0)
                {
                    var vfile = System.IO.Path.Combine(System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location), "DBGuardian.exe");
                    ProcessStartInfo pinfo = new ProcessStartInfo();
                    pinfo.UseShellExecute = true;
                    pinfo.WindowStyle = ProcessWindowStyle.Minimized;
                    pinfo.FileName = vfile;
                    pinfo.Arguments = "/m";
                    Process.Start(pinfo);
                }
            }
        }

        private object mLocker = new object();

        /// <summary>
        /// 
        /// </summary>
        public void InitClient()
        {
            lock (mLocker)
            {
                try
                {
                    CheckLocaServerIsRun();
                    if (mServerClient == null)
                    {
                        mServerClient = new DBRuntimeServer.Client(this.Model.HostAddress, 14000);
                        mServerClient.Login(Model.UserName, Model.Password, Model.DatabseName);
                        mConnectTimer = new System.Timers.Timer(2000);
                        mConnectTimer.Elapsed += MConnectTimer_Elapsed;
                        mConnectTimer.Start();
                    }

                    if (mServerClient.HasAntAlarm(Model.DatabseName, out bool isgrpc, out int port))
                    {
                        Application.Current.Dispatcher.Invoke(new Action(() => {
                            if(this.Children.Where(e=>e is AlarmNodeViewModel).Count()==0)
                            this.Children.Add(new AlarmNodeViewModel() { IsGrpc = isgrpc, Port = port,Parent=this });
                        }));
                    }
                }
                catch
                {

                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        private void MConnectTimer_Elapsed(object? sender, System.Timers.ElapsedEventArgs e)
        {
            if (mConnectTimer != null)
                mConnectTimer.Elapsed -= MConnectTimer_Elapsed;
            if (mServerClient != null)
            {
                try
                {
                    if (!mServerClient.IsLogined)
                    {
                        mServerClient.Login(Model.UserName, Model.Password, Model.DatabseName);
                        if (mServerClient.IsLogined)
                        {
                            CheckDataClient();
                        }
                    }
                    else
                    {
                        IsRunning = mServerClient.IsDatabaseRun(this.Model.DatabseName);
                        CheckDataClient();
                    }
                }
                catch (Exception ex)
                {

                }
            }
            if (mConnectTimer != null)
                mConnectTimer.Elapsed += MConnectTimer_Elapsed;
        }

        private bool mIsInited = false;

        /// <summary>
        /// 
        /// </summary>
        private void CheckDataClient()
        {
            if (mServerClient != null && DataServerPort < 0)
            {
                try
                {
                    var settings = mServerClient.GetApiSetting(this.Model.DatabseName);
                    if (settings != null)
                    {
                        foreach (var vv in settings)
                        {
                            if (vv.Name == "HighAPI")
                            {
                                DataServerPort = vv.Port;
                            }
                        }
                    }
                    if (IsRunning)
                    {
                        mIsInited = true;
                        mServerClient.CheckProcessStart("DBHighApi");
                    }
                }
                catch
                {

                }
            }
            else if (mServerClient != null)
            {
                if (IsRunning)
                {
                    mIsInited = true;
                    mServerClient.CheckProcessStart("DBHighApi");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="cancelAction"></param>
        /// <returns></returns>
        public int GetDataServerPort(Func<bool> cancelAction)
        {
            if (DataServerPort == -1)
            {
                InitClient();
                while (DataServerPort <= 0)
                {
                    Thread.Sleep(10);
                    if (cancelAction())
                    {
                        break;
                    }
                }
            }
            return DataServerPort;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="oldName"></param>
        /// <param name="newName"></param>
        /// <returns></returns>
        public override bool OnRename(string oldName, string newName)
        {
            return base.OnRename(oldName, newName);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanAddChild()
        {
            return false;
        }

        /// <summary>
        /// 
        /// </summary>
        public override void Dispose()
        {
            base.Dispose();
        }

        /// <summary>
        /// 
        /// </summary>
        public int DataServerPort { get; set; } = -1;

        ///// <summary>
        ///// 
        ///// </summary>
        //private void Close()
        //{
        //    if(mConnectTimer!=null)
        //    {
        //        mConnectTimer.Close();
        //        mConnectTimer = null;
        //    }
        //}

        /// <summary>
        /// 
        /// </summary>
        public void ReInit()
        {
            if (mServerClient != null)
            {
                mServerClient.Dispose();
                mServerClient = null;
            }
            InitClient();
        }


        /// <summary>
        /// 
        /// </summary>
        /// <param name="isselected"></param>
        public override void OnSelected(bool isselected)
        {
            if (isselected)
            {
                if (mServerClient == null)
                {
                    Task.Run(() =>
                    {
                        InitClient();
                    });
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="value"></param>
        public override void OnExpand(bool value)
        {
            if (value)
            {
                if (mServerClient == null)
                {
                    Task.Run(() =>
                    {
                        InitClient();
                    });
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public override bool CanRemove()
        {
            return true;
        }




        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
