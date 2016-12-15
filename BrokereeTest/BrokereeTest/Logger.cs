using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace BrokereeTest
{
    class Logger
    {
        string logFileName = "";
        string logFilePath = "";
        string logFullPath = "";

        public Logger()
        {
            logFileName = string.Format(@"{0}{1}.txt", @"Log_", DateTime.Now.Ticks);
            logFilePath = Path.Combine(Environment.CurrentDirectory, @"Log files\");
            logFullPath = Path.Combine(logFilePath, logFileName);
        }

        public void OpenLogFile()
        {
            if (!Directory.Exists(@logFilePath))
            {
                DirectoryInfo di = Directory.CreateDirectory(@logFilePath);
                Stream strim= File.Create(@logFullPath);
                strim.Close();
            }
            if (!File.Exists(@logFullPath))
            {
                Stream strim = File.Create(@logFullPath);
                strim.Close();
            }
            using (var writer = File.AppendText(@logFullPath))
            {
                Log("Start logging!", writer);
            }
        }

        public void Log(string logMessage, TextWriter w)
        {
            logMessage = logMessage + Environment.NewLine;
            w.Write("Log Entry: ");
            w.Write("{0} {1}", DateTime.Now.ToLongTimeString(),
                DateTime.Now.ToLongDateString());
            w.Write(", ");
            w.Write(logMessage);
        }

        public void WriteLog(string message)
        {
            using (var writer = File.AppendText(@logFullPath))
            {
                Log(message, writer);
            }
        }
    }
}
