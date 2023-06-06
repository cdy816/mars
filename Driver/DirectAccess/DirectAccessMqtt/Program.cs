
using Cdy.Tag;

namespace DirectAccessMqtt
{
    internal class Program
    {
        static void Main(string[] args)
        {
            ServiceLocator.Locator.Registor<ILog>(new ConsoleLogger());
            Console.Title = "DirectAccessMqttApi";
            if (args.Contains("/m"))
            {
                WindowConsolHelper.MinWindow("DirectAccessMqttApi");
            }
            Config.Instance.Load();
            DirectAccessProxy.Proxy.Load();

            DirectAccessProxy.Proxy.ConnectChangedEvent += Proxy_ConnectChangedEvent;
            DirectAccessProxy.Proxy.Start();

            while (true)
            {
                string cmd = Console.ReadLine();
                if(cmd=="exit")
                {
                    break;
                }
            }
        }

        private static void Proxy_ConnectChangedEvent(bool obj)
        {
            if(obj)
            {
                Task.Run(() => {
                
                    Thread.Sleep(1000);
                    var dbname = DirectAccessProxy.Proxy.GetDatabaseName();
                    if (!string.IsNullOrEmpty(dbname))
                    {
                        MqttServer.Instance.ServerTopic = "Mars.DirectAccess." + dbname;
                        MqttServer.Instance.Start();
                    }
                });
            }
            else
            {
                MqttServer.Instance.Stop();
            }
        }
    }
}