using Cdy.Ant;
using DBRuntimeMonitor.ViewModel;
using System;
using System.Collections.Generic;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;
using System.Windows.Interop;
using System.Windows.Media;

namespace DBRuntimeMonitor.ViewModel
{
    /// <summary>
    /// 
    /// </summary>
    public class AlarmDetailViewModel : ViewModelBase, IModeSwitch
    {

        #region ... Variables  ...
        private AlarmNodeViewModel mNode;
        private DateTime mStartTime;
        private DateTime mEndTime;
        private bool mIsBusy = false;

        private ICommand mQueryCommand;

        private ICommand mQueryToNowCommand;

        private AntRuntime.GrpcApi.Client mClient;

        private System.Collections.ObjectModel.ObservableCollection<AlarmItem> mItems=new System.Collections.ObjectModel.ObservableCollection<AlarmItem>();

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public AlarmDetailViewModel()
        {
            mStartTime = DateTime.Now.AddDays(-1);
            mEndTime = DateTime.Now;
        }
        #endregion ...Constructor...

        #region ... Properties ...

        public System.Collections.ObjectModel.ObservableCollection<AlarmItem> Items { get { return mItems; } }

        /// <summary>
        /// 
        /// </summary>
        public ICommand QueryCommand
        { 
            get
            {
                if(mQueryCommand==null)
                {
                    mQueryCommand = new RelayCommand(() => {

                        Query();
                    }, () => { return mClient != null; });
                }
                return mQueryCommand; 
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public ICommand QueryToNowCommand
        {
            get
            {
                if(mQueryToNowCommand == null)
                {
                    mQueryToNowCommand = new RelayCommand(() =>
                    {
                        EndTime= DateTime.Now;
                        Query();
                    }, () => { return mClient != null; });
                }
                return mQueryToNowCommand;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public AlarmNodeViewModel Node
        {
            get
            {
                return mNode;
            }
            set
            {
                if (mNode != value)
                {
                    mNode = value;
                    OnPropertyChanged("Node");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public DateTime StartTime
        {
            get
            {
                return mStartTime;
            }
            set
            {
                if (mStartTime != value)
                {
                    mStartTime = value;
                    OnPropertyChanged("StartTime");
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public DateTime EndTime
        {
            get
            {
                return mEndTime;
            }
            set
            {
                if (mEndTime != value)
                {
                    mEndTime = value;
                    OnPropertyChanged("EndTime");
                }
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsBusy
        {
            get
            {
                return mIsBusy;
            }
            set
            {
                if (mIsBusy != value)
                {
                    mIsBusy = value;
                    OnPropertyChanged("IsBusy");
                }
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void ClearAlarms()
        {
            Application.Current.Dispatcher.Invoke(() => {
                mItems.Clear();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="logs"></param>
        public void AppendLogs(IEnumerable<Message> logs)
        {
            if (logs == null) return;

            Application.Current.Dispatcher.Invoke(() => {
                foreach (var vv in logs.OrderByDescending(e=>e.CreateTime))
                {
                    AlarmItem item= new AlarmItem();
                    item.From(vv);
                    mItems.Add(item);
                }
            });
        }

        /// <summary>
        /// 
        /// </summary>
        private void Query()
        {
            ClearAlarms();
            IsBusy = true;
            Task.Run(() => {
                try
                {
                    var msg = this.mClient.QueryMessage(StartTime, EndTime);
                    AppendLogs(msg);
                }
                catch (Exception)
                {

                }
                IsBusy = false;
                Application.Current.Dispatcher.Invoke(() => {
                    CommandManager.InvalidateRequerySuggested();
                });

            });
        }

        /// <summary>
        /// 
        /// </summary>
        public void Active()
        {
            mClient = new AntRuntime.GrpcApi.Client();
            mClient.Port=Node.Port;
          
            Task.Run(() => {
                var vmod = (mNode.Parent as DatabaseViewModel).Model;
                if (vmod != null)
                {
                    mClient.Login(vmod.HostAddress, vmod.UserName, vmod.Password);
                }
            });
            
        }

        /// <summary>
        /// 
        /// </summary>
        public void DeActive()
        {
            if(mClient!=null)
            {
                mClient.Logout();
            }
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }

    public class AlarmItem
    {
        public long Id { get; set; }

        public string Server { get; set; } = "";


        public string SourceTag { get; set; } = "";

        /// <summary>
        /// 
        /// </summary>
        public string CreateTime { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string RestoreTime { get; set; }

        /// <summary>
        /// 恢复值
        /// </summary>
        public string RestoreValue { get; set; } = "";

        /// <summary>
        /// 报警值
        /// </summary>
        public string AlarmValue { get; set; } = "";

        /// <summary>
        /// 
        /// </summary>
        public string AlarmLevel { get; set; }

        public string MessageBody { get; set; } = "";


        public string Type { get; set; } = "";

        /// <summary>
        /// 
        /// </summary>
        public System.Windows.Media.Brush Brush { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<DisposalMessageItem> DisposalMessages { get; set; }

        public string AppendContent1 { get; set; } = "";


        public string AppendContent2 { get; set; } = "";


        public string AppendContent3 { get; set; } = "";


        public string DeleteNote { get; set; } = "";


        public string DeleteUser { get; set; } = "";


        public string DeleteTime { get; set; }


        public void From(Message msg)
        {
            Id = msg.Id;
            Server = msg.Server;
            SourceTag = msg.SourceTag;
            MessageBody = msg.MessageBody;
            CreateTime = msg.CreateTime.ToString("yyyy-MM-dd HH:mm:ss.fff");
            Type = msg.Type.ToString();
            AppendContent1 = msg.AppendContent1;
            AppendContent2 = msg.AppendContent2;
            AppendContent3 = msg.AppendContent3;
            DeleteNote = msg.DeleteNote;
            DeleteUser = msg.DeleteUser;
            DeleteTime = msg.DeleteTime != DateTime.MinValue ? msg.DeleteTime.ToString("yyyy-MM-dd HH:mm:ss.fff"):"";

            if (msg.DisposalMessages != null)
            {
                this.DisposalMessages = new List<DisposalMessageItem>();
                foreach (var vv in msg.DisposalMessages)
                {
                    DisposalMessageItem dm = new DisposalMessageItem();
                    dm.From(vv);
                    this.DisposalMessages.Add(dm);
                }
            }

            if (msg is Cdy.Ant.AlarmMessage)
            {
                var vmsg = msg as Cdy.Ant.AlarmMessage;
                this.AlarmValue = vmsg.AlarmValue;
                this.RestoreValue = vmsg.RestoreValue;
                this.RestoreTime = vmsg.RestoreTime!=DateTime.MinValue? vmsg.RestoreTime.ToString("yyyy-MM-dd HH:mm:ss.fff"):"";
                this.AlarmLevel = vmsg.AlarmLevel.ToString();
                if (vmsg.RestoreTime == DateTime.MinValue)
                {
                    switch (vmsg.AlarmLevel)
                    {
                        case Cdy.Ant.AlarmLevel.Info:
                            Brush = System.Windows.Media.Brushes.LightGray;
                            break;
                        case Cdy.Ant.AlarmLevel.EarlyWarning:
                            Brush = System.Windows.Media.Brushes.LightBlue;
                            break;
                        case Cdy.Ant.AlarmLevel.Normal:
                            Brush = System.Windows.Media.Brushes.Yellow;
                            break;
                        case Cdy.Ant.AlarmLevel.Critical:
                            Brush = System.Windows.Media.Brushes.Pink;
                            break;
                        case Cdy.Ant.AlarmLevel.Urgency:
                            Brush = System.Windows.Media.Brushes.Red;
                            break;
                        case Cdy.Ant.AlarmLevel.VeryUrgency:
                            Brush = System.Windows.Media.Brushes.DarkRed;
                            break;
                    }
                }
                else
                {
                    Brush = System.Windows.Media.Brushes.Green;
                }
            }
            else
            {
                Brush = System.Windows.Media.Brushes.LightGray;
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class DisposalMessageItem
    {
        public DateTime Time { get; set; }

        public string User { get; set; } = "";


        public string Message { get; set; } = "";

        public void From(DisposalItem msg)
        {
            Time = msg.Time;
            User = msg.User;
            Message = msg.Message;
        }
    }

}
