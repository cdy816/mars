using System;

namespace DBStudio
{
    class Program
    {
        static void Main(string[] args)
        {
            int port = 5001;
            if(args.Length>0)
            {
                port = int.Parse(args[0]);
            }
            DBDevelopService.DBService.Service.Start(port);
            Console.WriteLine("输入exit退出服务");
            while (true)
            {
                string[] cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                if (cmd.Length == 0) continue;
                if(cmd[0].ToLower()=="exit")
                {
                    Console.WriteLine("确定要退出?输入y确定,输入其他任意字符取消");
                    cmd = Console.ReadLine().Split(new string[] { " " }, StringSplitOptions.RemoveEmptyEntries);
                    if (cmd.Length == 0) continue;
                    if (cmd[0].ToLower() == "y")
                        break;
                }
            }
            DBDevelopService.DBService.Service.StopAsync();

        }
    }
}
