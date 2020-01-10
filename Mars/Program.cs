using System;

namespace Mars
{
    class Program
    {
        static void Main(string[] args)
        {
            if(args.Length>0 && args[0]== "start")
            {
                if (args.Length > 1)
                {
                    Cdy.Tag.Runner.RunInstance.StartAsync(args[1]);
                }
                else
                {
                    Cdy.Tag.Runner.RunInstance.Start();
                }
            }
            Console.WriteLine("输入h获取命令帮助信息");
            while (true)
            {
                Console.Write(">");
                string[] cmd = Console.ReadLine().Split(new string[] { " " },StringSplitOptions.RemoveEmptyEntries);
                if(cmd[0] == "exit")
                {
                    if(Cdy.Tag.Runner.RunInstance.IsStarted)
                    {
                        Cdy.Tag.Runner.RunInstance.Stop();
                    }
                    break;
                }
                else if(cmd[0] == "start")
                {
                    if (cmd.Length > 1)
                    {
                        Cdy.Tag.Runner.RunInstance.StartAsync(cmd[1]);
                    }
                    else
                    {
                        Cdy.Tag.Runner.RunInstance.Start();
                    }
                }
                else if(cmd[0] == "stop")
                {
                    Cdy.Tag.Runner.RunInstance.Stop();
                }
                else if(cmd[0] == "gd")
                {
                    switch(cmd.Length)
                    {
                        case 1:
                            GeneratorTestDatabase("local");
                            break;
                        case 2:
                            GeneratorTestDatabase(cmd[1]);
                            break;
                        case 3:
                            GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]));
                            break;
                        case 4:
                            GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]), int.Parse(cmd[3]));
                            break;
                        case 5:
                            GeneratorTestDatabase(cmd[1], int.Parse(cmd[2]), int.Parse(cmd[3]), int.Parse(cmd[4]));
                            break;
                    }
                }
                else if(cmd[0]=="h")
                {

                }
            }
        }

        static void GeneratorTestDatabase(string databaseName,int dcount=100000,int icount=100000,int bcount=100000)
        {
            Cdy.Tag.PathHelper.helper.SetDataBasePath(databaseName);
            Cdy.Tag.Database test = new Cdy.Tag.Database() { Name = databaseName };

            for(int i=0;i<dcount;i++)
            {
                test.Append(new Cdy.Tag.DoubleTag() { Name = "Double" + i ,Group="Double"});
            }

            for (int i = 0; i < icount; i++)
            {
                test.Append(new Cdy.Tag.IntTag() { Name = "Int" + i, Group = "Int" });
            }

            for (int i = 0; i < bcount; i++)
            {
                test.Append(new Cdy.Tag.BoolTag() { Name = "Int" + i, Group = "Bool" });
            }

            new Cdy.Tag.DatabaseManager() { Database = test }.Save();

            Cdy.Tag.HisDatabase htest = new Cdy.Tag.HisDatabase() { Name = databaseName,Setting = new Cdy.Tag.HisSettingDoc() };
            int id = 0;
            for (int i = 0; i < dcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Double, Circle = 1000,Type = Cdy.Tag.RecordType.Timer });
                id++;
            }

            for (int i = 0; i < icount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Int, Circle = 1000, Type = Cdy.Tag.RecordType.Timer });
                id++;
            }

            for (int i = 0; i < bcount; i++)
            {
                htest.AddHisTags(new Cdy.Tag.HisTag() { Id = id, TagType = Cdy.Tag.TagType.Bool, Circle = 1000, Type = Cdy.Tag.RecordType.Timer });
                id++;
            }

            new Cdy.Tag.HisDatabaseManager() { Database = htest }.Save();

        }

    }
}
