using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using System.Net.Sockets;
using System.IO;



namespace BrokereeTest
{
    class SimpleServer
    {
        private const int bufferSize = 1024 * 512;
        public SimpleServer()
        {
        }
        
        static void Main(string[] args)
        {
            string HTTPAddress = "";
            if (args.Length > 1)
            {
                HTTPAddress = args[1];
            }
            else
            {
                HTTPAddress = "http://*:8080";
            }
            // Validation of HTTP(s) address
            Uri uriResult;
            bool result = Uri.TryCreate(HTTPAddress, UriKind.Absolute, out uriResult)
                && (uriResult.Scheme == Uri.UriSchemeHttp || uriResult.Scheme == Uri.UriSchemeHttps);
            if (result)
            {
                //Start the listener
                Logger log = new Logger();
                log.OpenLogFile();
                var listener = new HttpListener();
                listener.Prefixes.Add(HTTPAddress);
                Console.WriteLine("Listening..."); 
                listener.Start();
                log.WriteLog("Started lisening the " + HTTPAddress + " http addres");

                Console.WriteLine("Waiting HTTPClien connection...");
                TradeParser parser = null;
                //Logical block
                while (true)
                {
                    try
                    {
                        var context = listener.GetContext();
                        var request = context.Request;
                        string commands = GetRequestPostData(request);
                        var response = context.Response;
                        string responseString = PageGenerator(log, ref parser, commands, context);

                        var buffer = System.Text.Encoding.UTF8.GetBytes(responseString);

                        response.ContentLength64 = buffer.Length;

                        var output = response.OutputStream;

                        output.Write(buffer, 0, buffer.Length);

                        Console.WriteLine("In process...");

                        output.Close();   
                    }
                    catch (Exception e)
                    {
                        // Client disconnected or some other error - ignored for this example
                    }
                    log.WriteLog("End of log file");
                }
                
                listener.Stop();
                Console.ReadKey();
            }
            else
            {
                Console.WriteLine("Argument of application is wrong. Please recheck URL address.");
            }
        }
        
        //Get command form HTTPClient
        public static string GetRequestPostData(HttpListenerRequest request)
        {
            if (!request.HasEntityBody)
            {
                return null;
            }
            using (System.IO.Stream body = request.InputStream)
            {
                using (System.IO.StreamReader reader = new System.IO.StreamReader(body, request.ContentEncoding))
                {
                    return reader.ReadToEnd();
                }
            }
        }

        //Parse commands
        public static SortedDictionary<string, string> ParseCommandString(string commands)
        {
            if (commands == null)
                return null;
            if (commands.Length == 0)
                return null;
            SortedDictionary<string, string> commandMap = null;
            string[] splitCommands = commands.Split('&','=');
            if (splitCommands.Length < 2 || splitCommands.Length%2 == 1)
                return null;
            commandMap = new SortedDictionary<string, string>();
            for (int i = 0; i < splitCommands.Length; i=i+2)
            {
                commandMap.Add(splitCommands[i], splitCommands[i + 1]);
            }
            return commandMap;
        }
        
        //Prase file with contain trades and insert it to DB ot CSV file
        public static bool ParseTradeFile(Logger log, ref TradeParser parser, string path, string type, ref string errorMessage)
        {
            if (0 != path.Length && 0 != type.Length)
            {
                if (!Path.GetExtension(@path).Equals(".xml"))
                {
                    errorMessage = "Wrong file extension or format, accept only xml files!";
                    log.WriteLog(errorMessage);
                    return false;
                }
                if (!File.Exists(WebUtility.UrlDecode(path)))
                {
                    errorMessage = "Your selected file does not exist! Please recheck path or file name .";
                    log.WriteLog(errorMessage);
                    return false;
                }
                parser = new TradeParser(WebUtility.UrlDecode(path), type);
                log.WriteLog("Try to parse the following file - " + path);
                if (type.Equals("csv"))
                {
                    if (parser.CreateCSVFile(log, ref errorMessage))
                    {
                        if (parser.SavetoCSVFile(log, ref errorMessage))
                            return true;
                    }
                    else
                    {
                        errorMessage = "Cannot create csv file for saving trades!";
                        log.WriteLog(errorMessage);
                        return false;
                    }
                 }
                 else
                 {
                    if (parser.ConnectoToDB(log, ref errorMessage))
                    {
                        if (!parser.InsertToDB(log, ref errorMessage))
                        { 
                            if(0 == errorMessage.Length)
                            {
                                errorMessage = "While insertion error is occurs! Please contact with server ownner!";
                                log.WriteLog(errorMessage);                                
                            }
                            return false;
                        }
                        return true;
                    }
                    else
                    {
                        if (0 == errorMessage.Length)
                        {
                            errorMessage = "Cannot connect to DB please, something goes horably wrong!";
                            log.WriteLog(errorMessage);
                        }
                        return false;
                    }
                }
                
            }
            else
            {
                errorMessage = "Path is empty, please fill it first!";
                log.WriteLog(errorMessage);
                return false;
            }

            return false;
        }
        
        //Get trade by id
        public static string GetTradeByID(Logger log, TradeParser parser, string id, ref string errorMessage)
        {
            if (null != id && null != parser)
            {
                if (0 != id.Length)
                {
                    if (parser.IsNotEmpty())
                    {
                        string result = parser.SelectTradeByID(log, id, ref errorMessage);
                        return result;
                    }
                }
            }
            else
            {
                errorMessage = "Something geos wrong, seems DB is empty or deosn't exist. Or maybe you foggot to enter trade ID!";
                log.WriteLog(errorMessage);
            }
            return null;
        }
        
        //Delete trade by ID
        public static bool DeleteTradeByID(Logger log, TradeParser parser, string id, ref string errorMessage)
        {
            if (null != id && null != parser)
            {
                if (0 != id.Length)
                {
                    if (parser.IsNotEmpty())
                    {
                        if (parser.DeleteTradeByID(log, id, ref errorMessage))
                            return true;
                    }
                }
            }
            else
            {
                errorMessage = "Something geos wrong, seems DB is empty or deosn't exist. Or maybe you foggot to enter trade ID!";
                log.WriteLog(errorMessage);
            }
            return false;
        }

        //Upload file
        private static bool ReturnFile(HttpListenerContext context, string filePath)
        {
            try
            {
                context.Response.ContentType = "text/plain";
                var buffer = new byte[bufferSize];
                using (var fs = File.OpenRead(filePath))
                {
                    context.Response.ContentLength64 = fs.Length;
                    context.Response.SendChunked = false;
                    int read;
       
                    using (BinaryWriter bw = new BinaryWriter(context.Response.OutputStream))
                    {
                        while ((read = fs.Read(buffer, 0, buffer.Length)) > 0)
                        {
                            bw.Write(buffer, 0, read);
                            bw.Flush();
                        }
                        bw.Close();
                    }
                }

                context.Response.StatusCode = (int)HttpStatusCode.OK;
                context.Response.StatusDescription = "OK";
                context.Response.OutputStream.Close();
                return true;
            }
            catch
            {
                return false;
            }
        }
        //Generating index page with forms, forms are contain comands.
        public static string PageGenerator(Logger log, ref TradeParser parser, string commands, HttpListenerContext context)
        {
            string errorMessage = "";
            string successMessage = "";
            string htmlPage = "";
            string tradeList = "";
            string csvList = "";
            SortedDictionary<string, string> command = null;

            if (null != commands)
            {
                if (0 != commands.Length)
                {
                    command = ParseCommandString(commands);
                }
            }
            // find all csv files in folder
            string csvFolder = Path.Combine(Environment.CurrentDirectory, @"CSV files\");
            if (null == commands || null == command || 0 == commands.Length)
            {
                if (Directory.Exists(@csvFolder))
                {
                    DirectoryInfo csvDirectory = new DirectoryInfo(@csvFolder);
                    if (Directory.EnumerateFiles(@csvFolder).Any())
                    {
                        csvList = @"<table style = 'width:400px'>
                                    <tr>
                                        <th>File name</th>
                                        <th>Save</th>
                                        <th>Delete</th> 
                                    </tr>";
                        foreach (var file in csvDirectory.GetFiles("*.csv"))
                        {
                            if (file.Length > 0)
                            {
                                csvList += @"<tr>
                                        <td>" + file.Name + @"</td>
                                        <td>
                                            <form id='savetrade' method='post'>
                                            <input type='hidden' name='save' value='" + file.Name + @"'  style='height=0px;width=0px;'/>
                                            <input type='submit' name='submit' value='Save' />
                                            </form>
                                        </td> 
                                        <td>
                                            <form id='deletetrade' method='post'>
                                            <input type='hidden' name='delete' value='" + file.Name + @"'  style='height=0px;width=0px;'/>
                                            <input type='submit' name='submit' value='Delete' />
                                            </form>
                                        </td>
                                </tr>";
                            }
                            else 
                            {
                                file.Delete();
                            }
                        }
                        csvList += "</table>";
                    }
                }
                htmlPage = @"<html>
	                       <title>
		                        Processing a trades
	                       </title>
	                       <body>
		                        <div style='height:30px; width:100%;'> Welcome to trade procecing tool, this is command page.</div>
		                        <div id = 'message' style='height:30px; width:100%;font-color:red;'></div>
		                        <div id='main'>
			                        <div id='mainform' style='height:400px; width:400px;float:left'>
				                    <h3>Main commands</h3>
				                        <form name='maincommands' method='post'>
					                        <p><b>Trades path:</b><br>
						                     <input name = 'path' style='margin : 0px auto;width:360px' type='text'>
					                         </p>
					                         <p><b>Save type:</b><br>
						                        <input type='radio' name='type' value='sqlite' checked='checked'> SQLite <Br>
						                        <input type='radio' name='type' value='sqlserver'> SQL Server Compact<Br>
						                        <input type='radio' name='type' value='csv'> CSV file<Br>
					                        </p>
					                        <p><input type='submit' value='Parse'></p>
				                        </form>
			                        </div>
			                        <div id='showform' style='height:400px; width:400px;float:left'>
				                        <h3>Get trade by ID</h3>
				                        <form name='show' method='post'>
					                        <p><b>Enter trade ID:</b><br>
						                    <input name='showid' type='number' style='margin : 0px auto;width:360px' type='text'>
					                        </p>
					                        <p><input type='submit' value='Show'></p>
				                        </form>
			                        </div>
			                        <div id='delteform' style='height:400px; width:400px;float:left'>
				                        <h3>Delete trade by ID</h3>
				                        <form name='delete' method='post'>
					                        <p><b>Enter trade ID:</b><br>
						                    <input name='deleteid' type='number' style='margin : 0px auto;width:360px' type='text'>
					                        </p>
					                        <p><input type='submit' value='Delete'></p>
				                        </form>
			                        </div>
		                            <div id='trades'>" + tradeList +
                                    @"</div> 
                                    </div>
		                            <div id='csv'>" + csvList +
                                    @"</div>    
                            </div>
	                        </body>
                            </html>";
                
            }
            else
            {
                string path = "";
                string type = "";
                string deleteID = "";
                string showID = "";
                string csvFileName = "";
                if (command != null)
                {
                    if (command.TryGetValue("path", out path))
                    {
                        if (command.TryGetValue("type", out type))
                        {
                            if (!ParseTradeFile(log, ref parser, path, type, ref errorMessage))
                            {
                                if (0 == errorMessage.Length)
                                {
                                    errorMessage = "File path or type isn't correct, please re-check these rows!";
                                    log.WriteLog(errorMessage);
                                }
                            }
                            else
                            {
                                if (type.Equals("csv"))
                                {
                                    successMessage = "Referenced file was successfully parsed and saved in CSV file";
                                }
                                else
                                {
                                    successMessage = "Referenced file was successfully parsed and all trades was inserted to DB";
                                }
                            }
                        }
                    }
                    if (command.TryGetValue("showid", out showID))
                    {
                        if (0 == showID.Length)
                        {
                            errorMessage = "Please enter trdae ID at first, trade id is empty";
                            log.WriteLog(errorMessage);
                        }
                        else
                        {
                            string result = GetTradeByID(log, parser, showID, ref errorMessage);
                            if (result == null)
                            {
                                if (0 == errorMessage.Length)
                                {
                                    errorMessage = "Could not find trade with this ID, or entered ID is not valied!";
                                    log.WriteLog(errorMessage);
                                }
                            }
                            else
                            {
                                string[] resultArray = result.Split(',');
                                tradeList = @"<table style = 'width:400px'>
                                    <tr>
                                        <th>ID</th>
                                        <th>Account</th>
                                        <th>Volume</th> 
                                        <th>id</th> 
                                    </tr>";
                                tradeList += @"<tr>
                                        <td>" + resultArray[0] + @"</td>
                                        <td>" + resultArray[1] + @"</td>
                                        <td>" + resultArray[2] + @"</td>
                                        <td>" + resultArray[3] + @"</td>
                                </tr></table>";
                                successMessage = "Please check trade in below of the page!";
                            }
                        }
                    }
                    if (command.TryGetValue("deleteid", out deleteID))
                    {
                        if (0 == deleteID.Length)
                        {
                            errorMessage = "Please enter trdae ID at first, trade id is empty";
                            log.WriteLog(errorMessage);
                        }
                        else
                        {
                            if (!DeleteTradeByID(log, parser, deleteID, ref errorMessage))
                            {
                                if (0 == errorMessage.Length)
                                {
                                    errorMessage = "Could not delete trade with this ID, or entered ID is not valied!";
                                    log.WriteLog(errorMessage);
                                }
                            }
                            else
                            {
                                successMessage = "Trade was successfully deleted!";
                            }
                        }
                    }
                    if (command.TryGetValue("save", out csvFileName))
                    {
                        if (Directory.EnumerateFiles(csvFolder).Any())
                        {
                            string fullPath = Path.Combine(csvFolder, csvFileName);
                            if (File.Exists(fullPath))
                            {
                                ReturnFile(context, fullPath);
                            }
                            else
                            {
                                errorMessage = "CSV file with this name doesn't exist!";
                                log.WriteLog(errorMessage);
                            }
                        }
                        else
                        {
                            errorMessage = "CSV file with this name doesn't exist!";
                            log.WriteLog(errorMessage);
                        }
                    }
                    if (command.TryGetValue("delete", out csvFileName))
                    {
                        if (Directory.EnumerateFiles(csvFolder).Any())
                        {
                            string fullPath = Path.Combine(csvFolder, csvFileName);
                            if (File.Exists(fullPath))
                            {
                                try
                                {
                                    File.Delete(fullPath);
                                    successMessage = "Selected file successfully deleted!";
                                }
                                catch
                                {
                                    errorMessage = "Ops, cannot delete this file!";
                                    log.WriteLog(errorMessage);
                                }
                            }
                            else
                            {
                                errorMessage = "CSV file with this name doesn't exist!";
                                log.WriteLog(errorMessage);
                            }
                        }
                        else
                        {
                            errorMessage = "CSV file with this name doesn't exist!";
                            log.WriteLog(errorMessage);
                        }
                    }
                }
                else
                {
                    errorMessage = "Unexpected error! Please contact with server provider!";
                    log.WriteLog(errorMessage);
                }
                string messageSection = "";
                if (errorMessage.Length != 0)
                {
                    messageSection = @"<div id = 'message' style='height:30px; width:100%;'><font color='red'>"
                                                                                    + errorMessage + "</font></div>";
                }
                if (successMessage.Length != 0)
                {
                    messageSection = @"<div id = 'message' style='height:30px; width:100%'><font color='greem'>"
                                                                                    + successMessage + "</font></div>";
                }
                if (errorMessage.Length == 0 && successMessage.Length == 0)
                {
                    messageSection = @"<div id = 'message' style='height:30px; width:100%;'></div>";
                }

               if (Directory.Exists(@csvFolder))
               {
                    DirectoryInfo csvDirectory = new DirectoryInfo(@csvFolder);
                    if (Directory.EnumerateFiles(@csvFolder).Any())
                    {
                       csvList = @"<table style = 'width:400px'>
                            <tr>
                            <th>File name</th>
                            <th>Save</th>
                            <th>Delete</th> 
                                 </tr>";
                        foreach (var file in csvDirectory.GetFiles("*.csv"))
                        {
                            if (file.Length > 0)
                            {
                                csvList += @"<tr>
                                        <td>" + file.Name + @"</td>
                                        <td>
                                            <form id='savetrade' method='post'>
                                            <input type='hidden' name='save' value='" + file.Name + @"'  style='height=0px;width=0px;'/>
                                            <input type='submit' name='submit' value='Save' />
                                            </form>
                                        </td> 
                                        <td>
                                            <form id='deletetrade' method='post'>
                                            <input type='hidden' name='delete' value='" + file.Name + @"'  style='height=0px;width=0px;'/>
                                            <input type='submit' name='submit' value='Delete' />
                                            </form>
                                        </td>
                                </tr>";
                            }
                            else
                            {
                                file.Delete();
                            }
                        }
                        csvList += "</table>";
                    }
                }
                htmlPage = @"<html>
	                       <title>
		                        Processing a trades
	                       </title>
	                       <body>
		                        <div style='height:30px; width:100%;'> Welcome to trade procecing tool, this is command page.</div>" +
                                messageSection +
                                @"<div id='main'>
			                        <div id='mainform' style='height:400px; width:400px;float:left'>
				                    <h3>Main commands</h3>
				                        <form name='maincommands' method='post'>
					                        <p><b>Trades path:</b><br>
						                     <input name = 'path' style='margin : 0px auto;width:360px' type='text'>
					                         </p>
					                         <p><b>Save type:</b><br>
						                        <input type='radio' name='type' value='sqlite' checked='checked'> SQLite <Br>
						                        <input type='radio' name='type' value='sqlserver'> SQL Server Compact<Br>
						                        <input type='radio' name='type' value='csv'> CSV file<Br>
					                        </p>
					                        <p><input type='submit' value='Parse'></p>
				                        </form>
			                        </div>
			                        <div id='showform' style='height:400px; width:400px;float:left'>
				                        <h3>Get trade by ID</h3>
				                        <form name='show' method='post'>
					                        <p><b>Enter trade ID:</b><br>
						                    <input name='showid' type='number' style='margin : 0px auto;width:360px' type='text'>
					                        </p>
					                        <p><input type='submit' value='Show'></p>
				                        </form>
			                        </div>
			                        <div id='delteform' style='height:400px; width:400px;float:left'>
				                        <h3>Delete trade by ID</h3>
				                        <form name='delete' method='post'>
					                        <p><b>Enter trade ID:</b><br>
						                    <input name='deleteid' type='number' style='margin : 0px auto;width:360px' type='text'>
					                        </p>
					                        <p><input type='submit' value='Delete'></p>
				                        </form>
			                        </div>
		                            <div id='trades'>" + tradeList +
                                    @"</div> 
                                    </div>
		                            <div id='csv'>" + csvList +
                                    @"</div>    
                            </div>
	                        </body>
                            </html>";
            }
            return htmlPage;
        }
    }
}
