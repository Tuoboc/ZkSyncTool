using org.apache.zookeeper;
using org.apache.zookeeper.data;
using Rabbit.Zookeeper;
using Rabbit.Zookeeper.Implementation;
using Spectre.Console;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using static org.apache.zookeeper.ZooDefs;

namespace ZkSyncTool
{
    class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                SyncInfo config = new SyncInfo();
                AnsiConsole.Write(new FigletText("Zookeepeer Sync Tool").Centered().Color(Color.Blue));
            BeginConfig:
                do
                {
                    await CheckSourceZkStatus(config);
                }
                while (!config.SourceConnected);
                do
                {
                    await CheckTargetZkStatus(config);
                }
                while (!config.TargetConnected);
                var rule = new Rule("Sync Config");
                rule.RuleStyle("blue dim");
                AnsiConsole.Write(rule);
                config.SyncPath = AnsiConsole.Ask<string>("Sync Path(eg. /configs/xxx):");
                config.SyncAuth = AnsiConsole.Prompt(new SelectionPrompt<string>().Title("Sync ACL?").AddChoices(new string[] { "Yes", "No" }));
                AnsiConsole.WriteLine("Sync ACL:" + config.SyncAuth);
                config.ConsoleLog = AnsiConsole.Prompt(new SelectionPrompt<string>().Title("Console Sync Log?").AddChoices(new string[] { "Yes", "No" }));
                AnsiConsole.WriteLine("Console Sync Log:" + config.ConsoleLog);
                var checkresult = SyncConfigCheck(config);
                if (checkresult == "No")
                    goto BeginConfig;
                await Sync(config);

            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine("[bold red]Throw Exception:" + ex.Message + "[/]");
            }
            finally
            {
                AnsiConsole.WriteLine("Press Any Key Exit!");
                Console.ReadLine();
                AnsiConsole.Write(new FigletText("Bye").Centered().Color(Color.Blue));
            }
        }

        public static async Task CheckTargetZkStatus(SyncInfo config)
        {
            config.TargetZkHost = "";
            var rule = new Rule("Target Zookeepeer Config");
            rule.RuleStyle("blue dim");
            AnsiConsole.Write(rule);
            config.TargetZkHost = AnsiConsole.Ask<string>("Target ZK Host(eg. localhost:2181,default port 2181):");
            if (!config.TargetZkHost.Contains(":"))
                config.TargetZkHost += ":2181";
            AnsiConsole.Status().Start("Connection Test", ctx =>
            {
                IZookeeperClient client = client = new ZookeeperClient(new ZookeeperClientOptions(config.TargetZkHost) { ConnectionTimeout = TimeSpan.FromSeconds(10), OperatingTimeout = TimeSpan.FromSeconds(10) }); ;
                try
                {
                    if (!string.IsNullOrWhiteSpace(config.SourceZkAuthValue))
                    {
                        var pwd = Encoding.UTF8.GetBytes(config.SourceZkAuthValue);
                        client.ZooKeeper.addAuthInfo(config.SourceZkAuthType, pwd);
                    }
                    var fff = client.GetChildrenAsync("/").Result;
                    AnsiConsole.MarkupLine("[bold green]Connect Success![/]");
                    config.TargetConnected = true;
                }
                catch (Exception ex)
                {
                    AnsiConsole.MarkupLine("[bold red]Connect failue!" + ex.Message + "[/]");
                    config.TargetConnected = false;
                }
                finally
                {
                    client.Dispose();
                }
            });
        }
        public static async Task CheckSourceZkStatus(SyncInfo config)
        {
            config.SourceZkAuthValue = "";
            config.SourceZkHost = "";
            var rule = new Rule("Source Zookeepeer Config");
            rule.RuleStyle("blue dim");
            AnsiConsole.Write(rule);
            config.SourceZkHost = AnsiConsole.Ask<string>("Source ZK Host(eg. localhost:2181,default port 2181):");
            if (!config.SourceZkHost.Contains(":"))
                config.SourceZkHost += ":2181";
            var needsorcezkauth = AnsiConsole.Prompt(new SelectionPrompt<string>().Title("Add AuthInfo?").AddChoices(new string[] { "Yes", "No" }));
            if (needsorcezkauth == "Yes")
            {
                config.SourceZkAuthType = AnsiConsole.Prompt(new SelectionPrompt<string>().Title("AuthType").AddChoices(new string[] { "digest", "auth", "ip" }));
                config.SourceZkAuthValue = AnsiConsole.Ask<string>("AuthInfo(eg. username:password):");
            }
            AnsiConsole.WriteLine("Add AuthInfo:" + needsorcezkauth);
            AnsiConsole.Status().Start("Connection Test", ctx =>
             {
                 IZookeeperClient client = client = new ZookeeperClient(new ZookeeperClientOptions(config.SourceZkHost) { ConnectionTimeout = TimeSpan.FromSeconds(10), OperatingTimeout = TimeSpan.FromSeconds(10) }); ;
                 try
                 {
                     if (needsorcezkauth == "Yes")
                     {
                         var pwd = Encoding.UTF8.GetBytes(config.SourceZkAuthValue);
                         client.ZooKeeper.addAuthInfo(config.SourceZkAuthType, pwd);
                     }
                     var fff = client.GetChildrenAsync("/").Result;
                     AnsiConsole.MarkupLine("[bold green]Connect Success![/]");
                     config.SourceConnected = true;
                 }
                 catch (Exception ex)
                 {
                     AnsiConsole.MarkupLine("[bold red]Connect failue!" + ex.Message + "[/]");
                     config.SourceConnected = false;
                 }
                 finally
                 {
                     client.Dispose();
                 }
             });
        }

        public static async Task Sync(SyncInfo config)
        {
            var rule = new Rule("Sync Status");
            rule.RuleStyle("blue dim");
            AnsiConsole.Write(rule);
            IZookeeperClient sclient = new ZookeeperClient(new ZookeeperClientOptions(config.SourceZkHost) { ConnectionTimeout = TimeSpan.FromSeconds(10), OperatingTimeout = TimeSpan.FromSeconds(10) });
            if (!string.IsNullOrWhiteSpace(config.SourceZkAuthValue))
            {
                var pwd = Encoding.UTF8.GetBytes(config.SourceZkAuthValue);
                sclient.ZooKeeper.addAuthInfo(config.SourceZkAuthType, pwd);
            }
            IZookeeperClient tclient = new ZookeeperClient(new ZookeeperClientOptions(config.TargetZkHost) { ConnectionTimeout = TimeSpan.FromSeconds(10), OperatingTimeout = TimeSpan.FromSeconds(10) });
            if (!string.IsNullOrWhiteSpace(config.SourceZkAuthValue))
            {
                var pwd = Encoding.UTF8.GetBytes(config.SourceZkAuthValue);
                tclient.ZooKeeper.addAuthInfo(config.SourceZkAuthType, pwd);
            }
            if (!await sclient.ExistsAsync(config.SyncPath))
            {
                AnsiConsole.MarkupLine("[bold red]" + config.SyncPath + " not exist[/]");
            }
            else
                await SyncItem(sclient, tclient, config.SyncPath, config);
            AnsiConsole.MarkupLine("[bold green]Sync Success![/]");
        }

        public static async Task SyncItem(IZookeeperClient sclient, IZookeeperClient tclient, string path, SyncInfo config)
        {
            var nodevalue = await sclient.GetDataAsync(path);
            var acl = await sclient.ZooKeeper.getACLAsync(path);
            if (await tclient.ExistsAsync(path))
            {
                await tclient.SetDataAsync(path, nodevalue?.ToArray());
                if (config.ConsoleLog == "Yes")
                {
                    AnsiConsole.MarkupLine("[bold green]" + path + "[/] has been updated");
                }
            }
            else
            {
                List<ACL> defaultacl = new List<ACL>() { new ACL(31, Ids.ANYONE_ID_UNSAFE) };
                await tclient.CreateAsync(path, nodevalue?.ToArray(), config.SyncAuth == "Yes" ? acl.Acls : defaultacl, CreateMode.PERSISTENT);
                if (config.ConsoleLog == "Yes")
                {
                    AnsiConsole.MarkupLine("[bold green]" + path + "[/] has been created");
                }
            }

            var node = await sclient.GetChildrenAsync(path);
            if (node.Count() > 0)
            {
                foreach (var item in node)
                {
                    await SyncItem(sclient, tclient, path + "/" + item, config);
                }
            }
        }

        public static string SyncConfigCheck(SyncInfo config)
        {
            var rule = new Rule("Sync Config Check");
            rule.RuleStyle("blue dim");
            AnsiConsole.Write(rule);
            var table = new Table().Centered().Border(TableBorder.DoubleEdge);
            table.AddColumn(new TableColumn("Section"));
            table.AddColumn(new TableColumn("Value"));
            table.AddRow("Source ZK Host", config.SourceZkHost ?? "");
            table.AddRow("Source ZK Auth", config.SourceZkAuthType + " " + config.SourceZkAuthValue ?? "");
            table.AddRow("Target ZK Host", config.TargetZkHost ?? "");
            table.AddRow("Sync Path", config.SyncPath ?? "");
            table.AddRow("Sync ACL", config.SyncAuth ?? "");
            table.AddRow("Console Sync Log", config.ConsoleLog ?? "");
            AnsiConsole.Write(table);
            return AnsiConsole.Prompt(new SelectionPrompt<string>().Title("All config is right?").AddChoices(new string[] { "Yes", "No" }));
        }
    }
}
