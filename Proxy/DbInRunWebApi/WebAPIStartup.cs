using System;
using System.Collections.Generic;
using System.IO.Compression;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using DBRuntime.Proxy;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.HttpsPolicy;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.ResponseCompression;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace DbInRunWebApi
{
    /// <summary>
    /// 
    /// </summary>
    public class WebAPIStartup
    {

        public static bool IsRunningEmbed=false;

        /// <summary>
        /// 
        /// </summary>
        /// <param name="services"></param>
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddControllers().AddJsonOptions(configure =>
            {
                configure.JsonSerializerOptions.Converters.Add(new DatetimeJsonConverter());
            });

            services.AddResponseCompression(opps => { opps.EnableForHttps = true; opps.Providers.Add<GzipCompressionProvider>(); opps.Providers.Add<BrotliCompressionProvider>(); });

            services.Configure<GzipCompressionProviderOptions>(options =>
            {
                options.Level = CompressionLevel.Optimal;
            });

            services.AddSwaggerDocument(config=> {
                config.PostProcess = document =>
                {
                    document.Info.Title = "Mars Web api";
                    document.Info.Description = "Mars Web api";
                    document.Info.Version = "v1";
                    document.Info.TermsOfService = "https://github.com/cdy816/mars";
                    document.Info.Contact = new NSwag.OpenApiContact() { Url = "https://github.com/cdy816/mars", Email = "cdy816@hotmail.com" };
                };
                
            });
        }
        /// <summary>
        /// 
        /// </summary>
        public class DatetimeJsonConverter : JsonConverter<DateTime>
        {
            /// <summary>
            /// 
            /// </summary>
            /// <param name="reader"></param>
            /// <param name="typeToConvert"></param>
            /// <param name="options"></param>
            /// <returns></returns>
            public override DateTime Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
            {
                if (reader.TokenType == JsonTokenType.String)
                {
                    if (DateTime.TryParse(reader.GetString(), out DateTime date))
                        return date;
                }
                return reader.GetDateTime();
            }

            /// <summary>
            /// 
            /// </summary>
            /// <param name="writer"></param>
            /// <param name="value"></param>
            /// <param name="options"></param>
            public override void Write(Utf8JsonWriter writer, DateTime value, JsonSerializerOptions options)
            {
                writer.WriteStringValue(value.ToString("yyyy-MM-dd HH:mm:ss"));
            }
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="app"></param>
        /// <param name="env"></param>
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseResponseCompression();

            //app.UseHttpsRedirection();

            app.UseCors((builder) => {

                builder.AllowAnyHeader();
                builder.AllowAnyOrigin();
                builder.AllowAnyMethod();
            });

            app.UseRouting();

            app.UseAuthorization();

            app.UseStaticFiles();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();

                endpoints.MapControllerRoute(
                    name: "default",
                   pattern: "{controller=Home}/{action=Index}/{id?}");
            });

            app.UseOpenApi();
            app.UseSwaggerUi3((setting0=> {
                setting0.DocumentTitle = "Mars database web api access document";
            }));


            if (!IsRunningEmbed)
            {
                DatabaseRunner.Manager.Load();
                DatabaseRunner.Manager.Start();
            }
        }
    }
}
