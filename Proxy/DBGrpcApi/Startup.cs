using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using DBRuntime.Proxy;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace DBGrpcApi
{
    public class Startup
    {

        public static string Server = "";

        // This method gets called by the runtime. Use this method to add services to the container.
        // For more information on how to configure your application, visit https://go.microsoft.com/fwlink/?LinkID=398940
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddGrpc();
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseRouting();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapGrpcService<SecurityService>();
                endpoints.MapGrpcService<RealDataService>();
                endpoints.MapGrpcService<HisDataService>();

                endpoints.MapGet("/", async context =>
                {
                    await context.Response.WriteAsync("Communication with gRPC endpoints must be made through a gRPC client.");
                });
            });

            DatabaseRunner.Manager.Load();
            if(!string.IsNullOrEmpty(Server))
            {
                DatabaseRunner.Manager.Ip = Server;
            }
            DatabaseRunner.Manager.Start();
        }
    }
}
