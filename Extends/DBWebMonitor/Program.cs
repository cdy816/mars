using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Security.Cryptography.X509Certificates;

namespace DBWebMonitor
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);
            string spath = System.IO.Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "mars.pfx");
            if (!builder.Environment.IsDevelopment())
            {
                builder.WebHost.ConfigureKestrel((context) =>
                {
                    context.ListenAnyIP(14603, listenOps =>
                    {
                        listenOps.UseHttps(callback =>
                        {
                            callback.AllowAnyClientCertificate();
                            callback.ServerCertificate = new System.Security.Cryptography.X509Certificates.X509Certificate2(spath, "mars");
                        });
                    });
                    context.ListenAnyIP(14602);
                });
            }

            // Add services to the container.
            builder.Services.AddRazorPages();
            builder.Services.AddServerSideBlazor();
            builder.Services.AddAntDesign();
            builder.Services.AddECharts();
            builder.Services.AddScoped<MarsProxy>();
            builder.Services.AddScoped<RuntimeClient>();

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (!app.Environment.IsDevelopment())
            {
                app.UseExceptionHandler("/Error");
                // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
                app.UseHsts();

                //try
                //{
                //    app.Urls.Add("http://127.0.0.1:14602");
                //    app.Urls.Add("https://127.0.0.1:14603");
                //}
                //catch
                //{

                //}
            }

            //app.UseHttpsRedirection();

            app.UseStaticFiles();

            app.UseRouting();

            app.MapBlazorHub();
            app.MapFallbackToPage("/_Host");
            CheckStartDBGuardian();
            app.Run();
        }

        private static void CheckStartDBGuardian()
        {
            if (!CheckProcessExist("DBGuardian"))
            {
                string sname = new System.IO.DirectoryInfo(System.IO.Path.GetDirectoryName(typeof(Program).Assembly.Location)).Parent.FullName;
                string spath = System.IO.Path.Combine(sname, "DBGuardian");
                StartProcess(spath);
            }
        }

        private static bool CheckProcessExist(string name)
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                return Process.GetProcessesByName(name).Length > 0;
            else
            {
                return Cdy.Tag.Common.ProcessMemoryInfo.IsExist(name);
            }
        }

        private static void StartProcess(string file, string arg = "", Action close = null)
        {
            if (Environment.OSVersion.Platform == PlatformID.Win32NT)
            {
                if (System.IO.File.Exists(file + ".exe"))
                {
                    var vfile = file;
                    ProcessStartInfo pinfo = new ProcessStartInfo();
                    pinfo.FileName = vfile + ".exe";
                    pinfo.Arguments = string.IsNullOrEmpty(arg) ? "/m" : arg + " " + "/m";
                    pinfo.RedirectStandardOutput = true;
                    pinfo.RedirectStandardInput = true;
                    pinfo.CreateNoWindow = true;
                    pinfo.UseShellExecute = false;

                    pinfo.WindowStyle = ProcessWindowStyle.Minimized;

                    Process sp = new Process();
                    sp.StartInfo = pinfo;
                    sp.Start();
                }
            }
            else
            {
                if (System.IO.File.Exists(file + ".dll"))
                {
                    string sarg = string.IsNullOrEmpty(arg) ? "/m" : arg + " " + "/m";
                    ProcessStartInfo info = new ProcessStartInfo("dotnet", $"{file}.dll {sarg}") { RedirectStandardOutput = true, RedirectStandardInput = true, RedirectStandardError = true, UseShellExecute = false, CreateNoWindow = true };
                    string sname = System.IO.Path.GetFileNameWithoutExtension(file + ".dll");

                    Process sp = new Process();
                    sp.StartInfo = info;
                    sp.Start();
                }
            }
        }
    }
}