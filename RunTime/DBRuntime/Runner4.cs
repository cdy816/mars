//==============================================================
//  Copyright (C) 2021  Inc. All rights reserved.
//
//==============================================================
//  Create by 种道洋 at 2021/1/14 14:45:02.
//  Version 1.0
//  种道洋
//==============================================================
using Cdy.Tag.Driver;
using DBRuntime;
using DBRuntime.His;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Cdy.Tag
{
    /// <summary>
    /// 
    /// </summary>
    public class Runner4
    {

        #region ... Variables  ...

        public static string CurrentDatabase = "";

        public static string CurrentDatabaseVersion = "";

        public static string CurrentDatabaseLastUpdateTime = "";

        /// <summary>
        /// 
        /// </summary>
        public static Runner4 RunInstance = new Runner4();

        /// <summary>
        /// 
        /// </summary>
        private string mDatabaseName = "local";

        private int mPort = -1;

        private Database mDatabase;

        private RealDatabase mRealDatabase;

        private HisDatabase mHisDatabase;

        private RealEnginer realEnginer;

        private HisEnginer4 hisEnginer;

        private CompressEnginer4 compressEnginer;

        private SeriseEnginer6 seriseEnginer;

        private DataFileManager mHisFileManager;

        private QuerySerivce querySerivce;

        private SecurityRunner mSecurityRunner;

        private bool mIsStarted = false;



        #endregion ...Variables...

        #region ... Events     ...

        #endregion ...Events...

        #region ... Constructor...

        /// <summary>
        /// 
        /// </summary>
        static Runner4()
        {
            RDDCManager.Manager.StartTime = DateTime.UtcNow;
        }

        /// <summary>
        /// 
        /// </summary>
        public Runner4()
        {
            RDDCManager.Manager.SwitchWorkStateAction = new Func<WorkState, bool>((state) => 
            {
                if (mIsStarted)
                {
                    if (state == WorkState.Primary)
                    {

                        return SwitchToPrimary();
                    }
                    else
                    {
                        return SwitchToStandby();
                    }
                }
                else
                {
                    while (!mIsStarted) System.Threading.Thread.Sleep(100);
                    if (state == WorkState.Primary)
                    {

                        return SwitchToPrimary();
                    }
                    else
                    {
                        return SwitchToStandby();
                    }
                }
            });
        }

        #endregion ...Constructor...

        #region ... Properties ...

        /// <summary>
        /// 
        /// </summary>
        public WorkState State
        {
            get
            {
                return RDDCManager.Manager.CurrentState;
            }
        }


        ///// <summary>
        ///// 数据库存访路径
        ///// </summary>
        //public string DatabasePath { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public Database Database { get { return mDatabase; } }

        /// <summary>
        /// 
        /// </summary>
        public bool IsStarted
        {
            get
            {
                return mIsStarted;
            }
        }

        /// <summary>
        /// 
        /// </summary>
        public bool IsRestartBusy { get; set; } = false;

        #endregion ...Properties...

        #region ... Methods    ...

        /// <summary>
        /// 
        /// </summary>
        private void StartLocalApi()
        {
            try
            {
                string spath = System.IO.Path.GetDirectoryName(this.GetType().Assembly.Location);

                if (mDatabase.Setting.EnableWebApi)
                {
                    if (!CheckProcessExist("DbWebApi"))
                    {
                        string sfile = System.IO.Path.Combine(spath, "DbWebApi", "DbWebApi");
                        StartProcess(sfile);
                    }
                }

                if (mDatabase.Setting.EnableGrpcApi)
                {
                    if (!CheckProcessExist("DBGrpcApi"))
                    {
                        string sfile = System.IO.Path.Combine(spath, "DBGrpcApi");
                        StartProcess(sfile);
                    }
                }

                if (mDatabase.Setting.EnableHighApi)
                {
                    if (!CheckProcessExist("DBHighApi"))
                    {
                        string sfile = System.IO.Path.Combine(spath, "DBHighApi");
                        StartProcess(sfile);
                    }
                }

                if (mDatabase.Setting.EnableOpcServer)
                {
                    if (!CheckProcessExist("DBOpcServer"))
                    {
                        string sfile = System.IO.Path.Combine(spath, "DbOpcServer", "DBOpcServer");
                        StartProcess(sfile);
                    }
                }
            }
            catch (Exception ex)
            {

            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private bool CheckProcessExist(string name)
        {
            return Process.GetProcessesByName(name).Length > 0;
        }

        private void StartProcess(string file)
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                if (System.IO.File.Exists(file + ".exe"))
                {
                    var vfile = file;
                    ProcessStartInfo pinfo = new ProcessStartInfo();
                    pinfo.FileName = vfile + ".exe";
                    pinfo.Arguments = "/m";
                    pinfo.RedirectStandardOutput = false;
                    pinfo.RedirectStandardInput = false;
                    pinfo.UseShellExecute = true;
                    pinfo.WindowStyle = ProcessWindowStyle.Minimized;
                    Process.Start(pinfo);
                }
            }
            else
            {
                if (System.IO.File.Exists(file + ".dll"))
                {
                    ProcessStartInfo info = new ProcessStartInfo("dotnet", $"{file}.dll /m") { RedirectStandardOutput = false, RedirectStandardInput = false, RedirectStandardError = false, UseShellExecute = true };
                    Process.Start(info);
                }
            }
        }


        /// <summary>
        /// 
        /// </summary>
        public void Init()
        {
           
            //注册日志
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());

            //注册线性转换器
            //ValueConvertManager.manager.Registor(new LinerConvert());

            ValueConvertManager.manager.Init();
            DataFileSeriserManager.manager.Init();
            CPUAssignHelper.Helper.Init();
        }

        /// <summary>
        /// 
        /// </summary>
        private void InitPath()
        {
            PathHelper.helper.CheckDataPathExist();
        }
        
        /// <summary>
        /// 
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        private bool CheckDatabaseExist(string name)
        {
            return System.IO.File.Exists(PathHelper.helper.GetDataPath(name, name + ".db"));
        }

        /// <summary>
        /// 
        /// </summary>
        private void LoadDatabase()
        {
            Stopwatch sw = new Stopwatch();
            sw.Start();
            this.mDatabase = new DatabaseSerise().Load(mDatabaseName);
            this.mRealDatabase = this.mDatabase.RealDatabase;
            this.mHisDatabase = this.mDatabase.HisDatabase;
            CurrentDatabaseVersion = this.mRealDatabase.Version;
            CurrentDatabase = mRealDatabase.Name;
            CurrentDatabaseLastUpdateTime = mRealDatabase.UpdateTime;

            sw.Stop();
            LoggerService.Service.Info("LoadDatabase", "加载数据库 " +mDatabaseName +" 花费: " + sw.ElapsedMilliseconds.ToString() +" ms");
        }

        /// <summary>
        /// 
        /// </summary>
        public void DynamicLoadTags()
        {
            bool ischanged = false;
            bool hischanged = false;
            bool issecuritychanged = false;
            LoggerService.Service.Info("DynamicLoadTags", "开始动态加载变量.", ConsoleColor.DarkYellow);
            string spath = System.IO.Path.Combine(PathHelper.helper.GetDatabasePath(this.mRealDatabase.Name), "Dynamic");
            if(System.IO.Directory.Exists(spath))
            {
                foreach(var vv in System.IO.Directory.GetFiles(spath).ToArray())
                {
                    if(vv.EndsWith(".xdb"))
                    {
                        var db = new RealDatabaseSerise().Load(vv);
                        if(db!=null)
                        {
                            DynamicLoadTags(db.ListAllTags(), null);
                            ischanged = true;
                        }
                    }
                    else if(vv.EndsWith(".hdb"))
                    {
                        var db = new HisDatabaseSerise().Load(vv);
                        if(db!=null)
                        {
                            DynamicLoadTags(null, db.ListAllTags());
                            hischanged = true;
                        }
                    }
                    else if(vv.EndsWith(".sdb"))
                    {
                        var db = new SecuritySerise().Load(vv);
                        this.Database.Security = db;
                        mSecurityRunner.Document = db;
                        issecuritychanged = true;
                    }

                    try
                    {
                        System.IO.File.Delete(vv);
                    }
                    catch
                    {
                        
                    }

                }

            }

            NotifyDatabaseChanged(ischanged,hischanged,issecuritychanged);

            LoggerService.Service.Info("DynamicLoadTags", "完成动态加载变量", ConsoleColor.DarkYellow);
        }

        /// <summary>
        /// 动态加载变量
        /// </summary>
        /// <param name="realtags"></param>
        /// <param name="hisTags"></param>
        public void DynamicLoadTags(IEnumerable<Tagbase> realtags,IEnumerable<HisTag> hisTags)
        {
            List<Tagbase> ltmp = new List<Tagbase>();
            List<Tagbase> changedrealtag = new List<Tagbase>();
            List<HisTag> htmp = new List<HisTag>();
            List<HisTag> changedhistag = new List<HisTag>();

            if (realtags != null)
            {
                foreach (var vv in realtags)
                {
                    if (this.mRealDatabase.Tags.ContainsKey(vv.Id))
                    {
                        var otag = this.mRealDatabase.Tags[vv.Id];
                        if (!otag.Equals(vv))
                        {
                            if (otag.Type != vv.Type)
                            {
                                LoggerService.Service.Warn("DynamicLoadTags", "不允许动态修改变量 '"+vv.Name+"' 的类型，你可以整体重新启动数据库来生效!");
                            }
                            else
                            {
                                changedrealtag.Add(vv);
                            }
                        }
                    }
                    else
                    {
                        ltmp.Add(vv);
                    }
                }

                if (changedrealtag.Count > 0 || ltmp.Count > 0)
                {
                    realEnginer.UpdateTags(changedrealtag);
                    realEnginer.AddTags(ltmp);
                   

                    DriverManager.Manager.NotifyRealTagChanged(ltmp.ToDictionary(e => e.Id, e => e.LinkAddress), changedrealtag.ToDictionary(e => e.Id, e => e.LinkAddress),null);
                }
            }

            if (hisTags != null)
            {
                foreach (var vv in hisTags)
                {
                    if (this.mHisDatabase.HisTags.ContainsKey(vv.Id))
                    {
                        var otag = this.mHisDatabase.HisTags[vv.Id];
                        if (!otag.Equals(vv))
                        {
                            if (otag.Type == vv.Type)
                            {
                                changedhistag.Add(vv);
                            }
                        }
                    }
                    else
                    {
                        htmp.Add(vv);
                    }
                }

                if (htmp.Count > 0 || changedhistag.Count > 0)
                {
                    var vids = htmp.Select(e => e.Id);
                    compressEnginer.ReSizeTagCompress(vids);
                    seriseEnginer.CheckAndAddSeriseFile(vids);

                    //hisEnginer.Pause();
                    hisEnginer.AddTags(htmp);
                    hisEnginer.UpdateTags(changedhistag);

                    compressEnginer.ReInitCompress(hisEnginer.CurrentMemory);

                    //hisEnginer.Resume();

                    DriverManager.Manager.NotifyHisTagChanged(htmp.Select(e => e.Id), changedhistag.Select(e => e.Id),null);
                }
            }

            CurrentDatabaseLastUpdateTime = DateTime.Now.ToString();
        }

        /// <summary>
        /// 
        /// </summary>
        private void NotifyDatabaseChanged(bool realchanged,bool hischanged,bool securitychanged)
        {
            var api = ServiceLocator.Locator.Resolve<IAPINotify>();
            api?.NotifyDatabaseChanged(realchanged,hischanged,securitychanged);
        }

        /// <summary>
        /// 重新加载数据库
        /// </summary>
        public void ReStartDatabase()
        {
            lock (RunInstance)
            {
                if (IsRestartBusy) return;

                IsRestartBusy = true;
            }

            try
            {
                bool ischanged = false;
                bool hischanged = false;
                bool issecuritychanged = false;
                LoggerService.Service.Info("ReStartDatabase", "开始重新热启动数据库", ConsoleColor.DarkYellow);

                Stopwatch sw = new Stopwatch();
                sw.Start();

                List<int> mRemoveRealTags;
                List<long> mRemovedHisTags = hisEnginer.Tags.Keys.ToList();

                var db = new DatabaseSerise().LoadDifference(mDatabaseName, this.Database.RealDatabase, new Func<HisTag, bool>((tag) =>
                {

                    if (this.hisEnginer.Tags.ContainsKey(tag.Id))
                    {
                        mRemovedHisTags.Remove(tag.Id);
                        return this.hisEnginer.Tags[tag.Id].EqualsTo(tag);
                    }
                    else
                    {
                        return false;
                    }
                }), out mRemoveRealTags);

                bool mNeedPause = false;

                List<Tagbase> ltmp = new List<Tagbase>();

                List<Tagbase> changedrealtag = new List<Tagbase>();

                List<HisTag> htmp = new List<HisTag>();
                List<HisTag> changedhistag = new List<HisTag>();

                Dictionary<int, string> mRemoveTagAddress = new Dictionary<int, string>();

                //
                foreach (var vv in db.RealDatabase.Tags)
                {
                    if (mRealDatabase.Tags.ContainsKey(vv.Key))
                    {
                        changedrealtag.Add(vv.Value);
                        ischanged = true;
                        mNeedPause = true;
                        mRealDatabase.UpdateForRuntime(vv.Value);
                    }
                    else
                    {
                        ltmp.Add(vv.Value);
                        mRealDatabase.Add(vv.Value);
                        ischanged = true;
                    }
                }

                foreach (var vv in mRemoveRealTags)
                {
                    if (mRealDatabase.Tags.ContainsKey(vv))
                    {
                        mRemoveTagAddress.Add(vv, mRealDatabase.Tags[vv].LinkAddress);
                        mRealDatabase.Remove(vv);
                    }
                }

                //
                foreach (var vv in db.HisDatabase.HisTags)
                {
                    if (this.hisEnginer.Tags.ContainsKey(vv.Key))
                    {
                        changedhistag.Add(vv.Value);
                    }
                    else
                    {
                        htmp.Add(vv.Value);
                    }
                    hischanged = true;
                    mNeedPause = true;
                }

                if (mRemovedHisTags.Count > 0) { mNeedPause = true; hischanged = true; }
                if (mRemoveRealTags.Count > 0) ischanged = true;

                LoggerService.Service.Info("ReStartDatabase", "加载 " + mDatabaseName + " 耗时: " + sw.ElapsedMilliseconds.ToString() + " ms");

                if (mNeedPause)
                {
                    //如果变量内容有变动,或者变量有被删除，则需要暂停,将已经记录的数据进行保存到磁盘
                    DriverManager.Manager.Pause();
                    //Thread.Sleep(100);
                    hisEnginer.Stop(false);
                    compressEnginer.Stop();

                }
                else
                {
                    compressEnginer.WaitForReady();
                }

                sw.Reset();
                sw.Start();

                var vids = htmp.Select(e => e.Id);

                compressEnginer.ReSizeTagCompress(vids);

                if (mNeedPause)
                {
                    //确保之前的数据写入到磁盘，新的变动后的数据在写入磁盘后，会使用新的文件头部
                    seriseEnginer.Stop();
                }

                seriseEnginer.CheckAndAddSeriseFile(vids);


                //hisEnginer.Pause();
                realEnginer.UpdateTags(changedrealtag);
                realEnginer.AddTags(ltmp);

                if (mRemoveRealTags.Count > 0)
                {
                    realEnginer.RemoveTags(mRemoveRealTags);
                }

                hisEnginer.AddTags(htmp);
                hisEnginer.UpdateTags(changedhistag);

                if (mRemovedHisTags.Count > 0)
                {
                    hisEnginer.RemoveTags(mRemovedHisTags);
                }

                if (db.HisDatabase != null)
                    mHisDatabase.Setting = db.HisDatabase.Setting;

                //hisEnginer.Resume();

                //
                if (!mSecurityRunner.Document.Equals(db.Security))
                {
                    mSecurityRunner.Document = db.Security;
                    issecuritychanged = true;
                }

                CurrentDatabaseVersion = db.Version;
                //CurrentDatabase = db.Name;
                CurrentDatabaseLastUpdateTime = mRealDatabase.UpdateTime;

                if (mNeedPause)
                {
                    compressEnginer.ReInitCompress(hisEnginer.CurrentMemory);
                    hisEnginer.Start();
                    DriverManager.Manager.Resume();
                    compressEnginer.Start();
                    seriseEnginer.ReInit();
                    seriseEnginer.Start();
                }
                else
                {
                    hisEnginer.CheckStartProcess();
                }

                //RegistorInterface();
                sw.Stop();
                LoggerService.Service.Info("ReStartDatabase", "初始化数据库" + mDatabaseName + " 耗时: " + sw.ElapsedMilliseconds.ToString() + " ms");

                NotifyDatabaseChanged(ischanged, hischanged, issecuritychanged);

                DriverManager.Manager.NotifyRealTagChanged(ltmp.ToDictionary(e => e.Id, e => e.LinkAddress), changedrealtag.ToDictionary(e => e.Id, e => e.LinkAddress), mRemoveTagAddress);

                DriverManager.Manager.NotifyHisTagChanged(htmp.Select(e => e.Id), changedhistag.Select(e => e.Id), mRemovedHisTags.Select(e => (int)e));


                StartLocalApi();

                LoggerService.Service.Info("ReStartDatabase", "热启动数据库完成", ConsoleColor.DarkYellow);
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("ReStartDatabase", $"{ex.Message} {ex.StackTrace}");
            }
            IsRestartBusy = false;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="database"></param>
        private async Task<bool> InitAsync(string database)
        {

            if (System.IO.Path.IsPathRooted(database))
            {
                this.mDatabaseName = System.IO.Path.GetFileNameWithoutExtension(database);
                //this.DatabasePath = System.IO.Path.GetDirectoryName(database);
            }
            else
            {
                this.mDatabaseName = database;
            }
            InitPath();

            LoggerService.LogPath = System.IO.Path.Combine(PathHelper.helper.GetDatabasePath(database),"Log");

            if (CheckDatabaseExist(mDatabaseName))
            {
                LoadDatabase();

                mHisFileManager = new DataFileManager(mDatabaseName);
                mHisFileManager.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;
                mHisFileManager.PrimaryHisDataPath = mHisDatabase.Setting.HisDataPathPrimary;
                mHisFileManager.BackHisDataPath = mHisDatabase.Setting.HisDataPathBack;
                

                var task = mHisFileManager.Int();
                realEnginer = new RealEnginer(mRealDatabase);
                realEnginer.DatabasePath = PathHelper.helper.GetDatabasePath(mDatabase.Name);
                realEnginer.Init();

                //加载磁盘缓冲
                realEnginer.LoadDataCach();

                ServiceLocator.Locator.Registor<IRealData>(realEnginer);
                ServiceLocator.Locator.Registor<IRealDataNotify>(realEnginer);
                ServiceLocator.Locator.Registor<IRealDataNotifyForProducter>(realEnginer);
                ServiceLocator.Locator.Registor<IRealTagConsumer>(realEnginer);
                ServiceLocator.Locator.Registor<IRealTagProduct>(realEnginer);

                ServiceLocator.Locator.Registor("DatabaseLocation", PathHelper.helper.GetDatabasePath(mDatabase.Name));

                hisEnginer = new HisEnginer4(mHisDatabase, realEnginer);
                hisEnginer.MergeMemoryTime = mHisDatabase.Setting.DataBlockDuration * 60;
                //hisEnginer.LogManager = new LogManager3() { Database = mDatabaseName,Parent=hisEnginer };
                hisEnginer.WorkMode = mDatabase.Setting.HisWorkMode;

                hisEnginer.Init();
                ServiceLocator.Locator.Registor<IHisEngine3>(hisEnginer);
                ServiceLocator.Locator.Registor<ITagHisValueProduct>(hisEnginer);

                //初始化从内存中查询数据的服务
                //HisDataMemoryQueryService3.Service.HisEnginer = hisEnginer;
                ServiceLocator.Locator.Registor<IHisQueryFromMemory>(HisDataMemoryQueryService3.Service);


                compressEnginer = new CompressEnginer4();
                compressEnginer.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;
                compressEnginer.Init();
                ServiceLocator.Locator.Registor<IDataCompress3>(compressEnginer);

                seriseEnginer = new SeriseEnginer6() { DatabaseName = database };
                seriseEnginer.FileDuration = mHisDatabase.Setting.FileDataDuration;
                seriseEnginer.BlockDuration = mHisDatabase.Setting.DataBlockDuration;
                seriseEnginer.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;
                seriseEnginer.DataSeriser = mHisDatabase.Setting.DataSeriser;
                SeriseEnginer6.HisDataPathPrimary = mHisDatabase.Setting.HisDataPathPrimary;
                SeriseEnginer6.HisDataPathBack = mHisDatabase.Setting.HisDataPathBack;
                SeriseEnginer6.HisDataKeepTimeInPrimaryPath = mHisDatabase.Setting.HisDataKeepTimeInPrimaryPath;
                SeriseEnginer6.KeepNoZipFileDays = mHisDatabase.Setting.KeepNoZipFileDays;
                seriseEnginer.Init();
                ServiceLocator.Locator.Registor<IDataSerialize4>(seriseEnginer);



                HisDataPatchSeriser.patchSeriser.FileDuration = mHisDatabase.Setting.FileDataDuration;
                HisDataPatchSeriser.patchSeriser.DatabaseName = database;
                HisDataPatchSeriser.patchSeriser.DataSeriser = mHisDatabase.Setting.DataSeriser;
                HisDataPatchSeriser.patchSeriser.TagCountOneFile = mHisDatabase.Setting.TagCountOneFile;
                ServiceLocator.Locator.Registor<IHisDataPatch>(HisDataPatchSeriser.patchSeriser);

                HisQueryManager.Instance.Registor(mDatabaseName, mHisFileManager);
                querySerivce = new QuerySerivce(this.mDatabaseName);

                mSecurityRunner = new SecurityRunner() { Document = mDatabase.Security };

                RegistorInterface();

                DriverManager.Manager.Init(realEnginer, hisEnginer);

                HisQueryManager.Instance.StartMonitor();

                await task;

                return true;
            }
            else
            {
                LoggerService.Service.Erro("Runner", string.Format(DBRuntime.Res.Get("databasenotexist"), mDatabaseName));
                return false;
            }
           

        }

        /// <summary>
        /// 
        /// </summary>
        private void RegistorInterface()
        {
            ServiceLocator.Locator.Registor<IHisQuery>(querySerivce);
            ServiceLocator.Locator.Registor<IHisTagQuery>(hisEnginer);
            ServiceLocator.Locator.Registor<ITagManager>(mRealDatabase);
            ServiceLocator.Locator.Registor<IRuntimeSecurity>(mSecurityRunner);
        }

        /// <summary>
        /// 
        /// </summary>
        public void ReStart()
        {
            Stop(false);
            StartAsync(mDatabaseName,mPort);
        }

        /// <summary>
        /// 
        /// </summary>
        public void Start(int port = -1)
        {
            StartAsync("local",port);
        }

        /// <summary>
        /// 启动
        /// </summary>
        /// <param name="database"></param>
        public async void StartAsync(string database,int port = -1)
        {
            LoggerService.Service.EnableLogger = true;
            LoggerService.Service.Info("Runner", " 数据库 " + database+" 开始启动");


            RDDCManager.Manager.Load(database);
            RDDCManager.Manager.Start();

            var re = await InitAsync(database);

            if (!re)
            {
                return;
            }

            //检查恢复数据
            HisDataRestore.Instance.CheckAndRestore();

            mPort = port;
            if(mPort < 1)
            {
                mPort = mDatabase.Setting.RealDataServerPort;
            }

            try
            {
                DBRuntime.Api.DataService.Service.Start(mPort);
            }
            catch
            {

            }

            GC.Collect();
            GC.WaitForFullGCComplete();

            if (RDDCManager.Manager.CurrentState == WorkState.Primary)
            {
                seriseEnginer.Start();
                compressEnginer.Start();
                hisEnginer.Start();
                DriverManager.Manager.Start();
            }

            StartLocalApi();
            mIsStarted = true;
            LoggerService.Service.Info("Runner", " 数据库 " + database + " 启动完成");
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mode"></param>
        public bool Switch(WorkState mode)
        {
            if (RDDCManager.Manager.EnableRDDC)
            {
                if (RDDCManager.Manager.CurrentState != mode)
                {
                    return RDDCManager.Manager.ManualSwitchTo(mode);
                }
            }
            else
            {
                LoggerService.Service.Info("Runner", " Rddc is not enable.");
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        private bool SwitchToStandby()
        {
            try
            {
                hisEnginer.Stop(false);
                compressEnginer.Stop();
                seriseEnginer.Stop();
                DriverManager.Manager.Stop();
            }
            catch
            {
                return false;
            }
            return true;
        }

        /// <summary>
        /// 
        /// </summary>
        private bool SwitchToPrimary()
        {
            try
            {
                DriverManager.Manager.Start();
                seriseEnginer.Start();
                compressEnginer.Start();
                hisEnginer.Start();
            }
            catch
            {
                return false;
            }
            return true;
        }


        /// <summary>
        /// 停止
        /// </summary>
        public  void Stop(bool ignorDispose=true)
        {
            LoggerService.Service.EnableLogger = true;
            try
            {
                realEnginer.CachDataToDisk();
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("RealEnginer", $"{ex.Message} {ex.StackTrace}");
            }
            try
            {
                DBRuntime.Api.DataService.Service.Stop();
               
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("DataService", $"{ex.Message} {ex.StackTrace}");
            }

            try
            {
                DriverManager.Manager.Stop();
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("DriverManager", $"{ex.Message} {ex.StackTrace}");
            }

            HisQueryManager.Instance.StartMonitor();

            try
            {
                hisEnginer.Stop(true);
               
            }
            catch(Exception ex)
            {
                LoggerService.Service.Warn("HisEnginer", $"{ex.Message} {ex.StackTrace}");
            }

            compressEnginer.Stop();
            seriseEnginer.Stop();
            if (!ignorDispose)
            {
                hisEnginer.Dispose();
                compressEnginer.Dispose();
                seriseEnginer.Dispose();
            }

            mIsStarted = false;
        }


        #endregion ...Methods...

        #region ... Interfaces ...

        #endregion ...Interfaces...
    }
}
