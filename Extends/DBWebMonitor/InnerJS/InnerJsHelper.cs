using Microsoft.AspNetCore.Components;
using Microsoft.JSInterop;

namespace DBWebStudio
{
    public class InnerJsHelper : JSModuleInterop
    {
        private const string JsFilename = "./InnerHelper.js";

        private const string DownLoadSymbal = "WindowHelper.DownloadFromFile";
        private const string DownLoadFromStreamSymbal = "WindowHelper.DownloadFromStream";
        private const string AlertSymbal = "WindowHelper.Alert";
        private const string DialogSymbal = "WindowHelper.Dialog";
        private const string GetTextBoxValueSymbal = "WindowHelper.GetTextBoxValue";
        private const string SetTextBoxValueSymbal = "WindowHelper.SetTextBoxValue";
        private const string AppendTextAreaValueSymbal = "WindowHelper.AppendTextAreaValue";
        private const string ClearTextAreaValueSymbal = "WindowHelper.ClearTextAreaValue";

        public InnerJsHelper(IJSRuntime js):this(js,JsFilename)
        {

        }
        public InnerJsHelper(IJSRuntime js, string filename) : base(js, filename)
        {

        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="url"></param>
        public void DownLoadFromFile(string file,string url)
        {
            Invoke(DownLoadSymbal, file, url);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="file"></param>
        /// <param name="Stream"></param>
        public void DownLoadFromStream(string file, DotNetStreamReference Stream)
        {
            Invoke(DownLoadFromStreamSymbal, file, Stream);
        }

        public void Dialog(string id,object options)
        {
            Invoke(DialogSymbal, id, options);
        }

        public void Alert(string msg)
        {
            Invoke(AlertSymbal, msg);
        }

        public Task<string> GetTextBoxValue(ElementReference ele)
        {
            return Invoke<string>(GetTextBoxValueSymbal, ele);
        }

        public void SetTextBoxValue(ElementReference ele,string val)
        {
            Invoke(SetTextBoxValueSymbal, ele,val);
        }

        public void AppendTextAreaValue(ElementReference ele, string val)
        {
             Invoke(AppendTextAreaValueSymbal, ele, val);
        }

        public void ClearTextAreaValue(ElementReference ele)
        {
            Invoke(ClearTextAreaValueSymbal, ele);
        }
    }
}
