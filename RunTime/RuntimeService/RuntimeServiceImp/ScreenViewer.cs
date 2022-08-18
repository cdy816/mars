using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBMonitor
{
    /// <summary>
    /// 
    /// </summary>
    public class ScreenViewer
    {

        int count=0;
        
        /// <summary>
        /// 
        /// </summary>
        public List<ScreenLine> mBuffers = new List<ScreenLine>();

        /// <summary>
        /// 
        /// </summary>
        private bool mIsDirty = false;

        public static int StartPosition { get; set; } = 1;

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ScreenLine RequireBuffer()
        {
            var sl = new ScreenLine() { Line = count++,IsDirty=true };
            mBuffers.Add(sl);
            return sl;
        }
        
        /// <summary>
        /// 
        /// </summary>
        public void Invaidate()
        {
            mIsDirty = true;
        }
        
        /// <summary>
        /// 
        /// </summary>
        public void Show()
        {
            if (mIsDirty)
            {
                mIsDirty=false;
                lock (mBuffers)
                {
                    foreach (var vv in mBuffers)
                    {
                        vv.Show();
                    }
                }
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class ScreenLine
    {
        /// <summary>
        /// 
        /// </summary>
        public bool IsDirty { get; set; }


        /// <summary>
        /// 
        /// </summary>
        public int Line { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public List<ScreenItem> Items { get; set; }=new List<ScreenItem>();

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        public ScreenItem NewItem()
        {
            ScreenItem item = new ScreenItem();
            lock (Items)
                Items.Add(item);
            return item;
        }

        /// <summary>
        /// 
        /// </summary>
        public void Show()
        {
            if (IsDirty)
            {
                IsDirty = false;
                lock (Items)
                {
                    int col = 0;
                    foreach (var vv in Items)
                    {
                        col += Math.Max(vv.Length, string.IsNullOrEmpty(vv.Text) ? 0 : vv.Text.Length);
                        Console.SetCursorPosition(col, ScreenViewer.StartPosition + this.Line);
                        if (vv.Color != null)
                        {
                            var vvc = Console.ForegroundColor;
                            Console.ForegroundColor = vv.Color.Value;
                            Console.Write(vv.Text);
                            Console.ForegroundColor = vvc;
                        }
                    }
                }
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    public class ScreenItem
    {
        /// <summary>
        /// 
        /// </summary>
        public int Length { get; set; } = -1;

        /// <summary>
        /// 
        /// </summary>
        public ConsoleColor? Color { get; set; }

        /// <summary>
        /// 
        /// </summary>
        public string Text { get; set; }

    }

}
