using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using System.Windows.Input;

namespace DBRuntimeMonitor.ViewModel
{
    public class LogDetailViewModel : ViewModelBase, IModeSwitch
    {

        #region ... Variables  ...
        private LogViewModel mNode;
        private string mType;
        private DateTime mStartTime;
        private DateTime mEndTime;

        private ICommand mQueryCommand;
        private ICommand mQueryToNowCommand;

        private List<string> mLogTypes;

        private bool mIsBusy = false;

        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...
        /// <summary>
        /// 
        /// </summary>
        public LogDetailViewModel()
        {
            mStartTime = DateTime.Now.AddDays(-1);
            mEndTime = DateTime.Now;
        }
        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public List<string> LogTypes
        {
            get
            {
                return mLogTypes;
            }
            set
            {

                mLogTypes = value;
                OnPropertyChanged("LogTypes");
            }

        }

        /// <summary>
        /// 
        /// </summary>
        public LogViewModel Node
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
        public string Type
        {
            get
            {
                return mType;
            }
            set
            {
                if (mType != value)
                {
                    mType = value;
                    ClearLogs();
                    OnPropertyChanged("Type");
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
                        QueryLogs(StartTime,EndTime);
                    }, () => { return !string.IsNullOrEmpty(Type)&& !IsBusy; });
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
                if(mQueryToNowCommand==null)
                {
                    mQueryToNowCommand = new RelayCommand(() => {

                        EndTime = DateTime.Now;
                        QueryLogs(StartTime, EndTime);
                    }, () => { return !string.IsNullOrEmpty(Type) && !IsBusy; });
                }
                return mQueryToNowCommand;
            }
        }

        System.Windows.Controls.TextBox mTextBox;

        /// <summary>
        /// 
        /// </summary>
        public System.Windows.Controls.TextBox TextBox
        {
            get { return mTextBox; }
            set
            {
                mTextBox = value;
                if(mTextBox != null)
                {
                    mTextBox.Clear();
                }
            }
        }

        #endregion ...Properties...

        #region ... Methods    ...



        /// <summary>
        /// 
        /// </summary>

        public void ClearLogs()
        {
            TextBox?.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="logs"></param>
        public void AppendLogs(IEnumerable<string> logs)
        {
            Application.Current.Dispatcher.Invoke(() => {
                foreach (var vv in logs)
                {
                    TextBox.AppendText(vv+Environment.NewLine);
                }
                TextBox.ScrollToEnd();
            });
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void Active()
        {
            IsBusy = false;
            LogTypes = new List<string>() { "DBInRun", "DBInStudioServer", "DBGrpcApi", "DBHighApi", "DbWebApi", "DbOpcServer" };
            Type = LogTypes.FirstOrDefault();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <exception cref="NotImplementedException"></exception>
        public void DeActive()
        {
            if (IsBusy)
            {

                IsBusy = false;
            }
            mTextBox?.Clear();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="starttime"></param>
        /// <param name="endtime"></param>
        private void QueryLogs(DateTime starttime,DateTime endtime)
        {
            ClearLogs();
            IsBusy = true;
            Task.Run(() => {
                if (mNode != null && (mNode.Parent as DatabaseViewModel) != null && (mNode.Parent as DatabaseViewModel).ServerClient != null && (mNode.Parent as DatabaseViewModel).ServerClient.IsLogined)
                {
                    var client = (mNode.Parent as DatabaseViewModel).ServerClient;
                    var logs = client.GetLogs(mNode.Model.DatabseName, mStartTime, mEndTime, Type);
                    if (logs != null)
                    {
                        AppendLogs(logs);
                    }
                }
                IsBusy = false;
                Application.Current.Dispatcher.Invoke(() => {
                    CommandManager.InvalidateRequerySuggested();
                });
               
            });

           
        }

        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...

    }
}
