using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data;
using System.Data.Common;
using System.Data.SQLite;
using System.Xml.Linq;
using System.Data.SqlClient;
using System.Data.SqlServerCe;


namespace BrokereeTest
{
    class TradeParser
    {
        string path = "";
        string type = "";

        //CSV file variables
        StringBuilder csv = new StringBuilder();
        string csvName = "";
        string csvPath = "";
        string csvFullPath = "";

        //SQLite variables
        private string SQLiteBaseName = "TradeDB.db";
        private SQLiteConnection SQLiteConnec;
        private SQLiteCommand SQLiteCommand;

        //SQLSercerCe variables
        private string SQLCeConnString;
        private SqlCeEngine SQLCeEngine;
        private string SQLServerCEBaseName = "TradeDB.sdf";
        private SqlCeConnection SQLCeConnec;
        private SqlCeCommand SQLCeCommand;

        //Variables for both databases
        private DataSet DS = new DataSet();
        private DataTable DT = new DataTable();

        public TradeParser()
        {
        }
        public TradeParser(string pPath, string pType)
        {
            path = pPath;
            type = pType;
        }

        public bool ConnectoToDB(Logger log, ref string errorMessage)
        {
            if(type.Equals("sqlite"))
            {
                SQLiteConnection.CreateFile(SQLiteBaseName);
                log.WriteLog("DB with this name was created - " + SQLiteBaseName);
                SQLiteFactory factory = (SQLiteFactory)DbProviderFactories.GetFactory("System.Data.SQLite");
                using (SQLiteConnec = (SQLiteConnection)factory.CreateConnection())
                {
                    SQLiteConnec.ConnectionString = "Data Source = " + SQLiteBaseName;
                    SQLiteConnec.Open();
                    using (SQLiteCommand = new SQLiteCommand(SQLiteConnec))
                    {
                        SQLiteCommand.CommandText = @"CREATE TABLE [trades] (
                            [id] integer PRIMARY KEY AUTOINCREMENT NOT NULL,
                            [trade_id] integer NOT NULL,
                            [account] integer NOT NULL,
                            [volume] double NOT NULL,
                            [comment] varchar(256)
                            );";
                        SQLiteCommand.CommandType = CommandType.Text;
                        SQLiteCommand.ExecuteNonQuery();
                        SQLiteCommand.CommandText = @"CREATE TABLE [headers] (
                            [id] integer PRIMARY KEY AUTOINCREMENT NOT NULL,
                            [version] integer NOT NULL,
                            [type] varchar(84)
                            );";
                        SQLiteCommand.CommandType = CommandType.Text;
                        SQLiteCommand.ExecuteNonQuery();
                        SQLiteConnec.Close();
                    }
                }
                return true;
            }
            else
            {
                // SQL Compact version
                if (File.Exists(SQLServerCEBaseName))
                {
                    File.Delete(SQLServerCEBaseName);
                }
                SQLCeConnString = string.Format("DataSource=\"{0}\"", SQLServerCEBaseName);
                SqlCeEngine engine = new SqlCeEngine(SQLCeConnString);
                engine.CreateDatabase();
                log.WriteLog("DB with this name was created - " + SQLCeConnString);
                using (SQLCeConnec = new SqlCeConnection(SQLCeConnString))
                {
                    SQLCeConnec.Open();
                    string SQLCeQuery = @"CREATE TABLE trades(
                            id int IDENTITY (100,1) PRIMARY KEY NOT NULL,
                            trade_id int NOT NULL,
                            account int NOT NULL,
                            volume float NOT NULL,
                            comment nvarchar (256)
                            );";
                    SQLCeCommand = new SqlCeCommand(SQLCeQuery, SQLCeConnec);
                    SQLCeCommand.ExecuteNonQuery();

                   SQLCeQuery = @"CREATE TABLE headers(
                            id int IDENTITY (100,1) PRIMARY KEY NOT NULL,
                            version int NOT NULL,
                            type nvarchar(84)
                            );";
                    SQLCeCommand = new SqlCeCommand(SQLCeQuery, SQLCeConnec);
                    SQLCeCommand.ExecuteNonQuery();
                    SQLCeConnec.Close();
                    
                }
                return true;
                
            }
            errorMessage = "Couldn't create DB or connect to it!";
            log.WriteLog(errorMessage);
            return false;
        }

        public bool InsertToDB(Logger log, ref string errorMessage)
        {

            if (type.Equals("sqlite"))
            {
                string[] header;
                string[] trade;
                SQLiteFactory factory = (SQLiteFactory)DbProviderFactories.GetFactory("System.Data.SQLite");
                using (SQLiteConnec = (SQLiteConnection)factory.CreateConnection())
                {
                    SQLiteConnec.ConnectionString = "Data Source = " + SQLiteBaseName;
                    SQLiteConnec.Open();
                    try
                    {
                        var xml = XDocument.Load(path);
                        var headerLinqQuery = from t in xml.Root.Descendants("header")
                                              select t.Element("version").Value + ", " +
                                                     t.Element("type").Value;
                        foreach (string headerRow in headerLinqQuery)
                        {
                            header = headerRow.Split(',');
                            InsertHeader(header, ref errorMessage);
                        }

                        var tradeLinqQuery = from t in xml.Root.Descendants("trade")
                                             select t.Element("id").Value + ", " +
                                                    t.Element("account").Value + ", " +
                                                    t.Element("volume").Value + ", " +
                                                    t.Element("comment").Value;

                        foreach (string tradeRow in tradeLinqQuery)
                        {
                            trade = tradeRow.Split(',');
                            InsertTrade(trade, ref errorMessage);
                        }
                        return true;
                    }
                    catch
                    {
                        return false;
                    }
                }
                
            }
            else
            {
                string[] header;
                string[] trade;
                using (SQLCeConnec = new SqlCeConnection(SQLCeConnString))
                {
                    SQLCeConnec.Open();
                    try
                    {
                        var xml = XDocument.Load(path);
                        var headerLinqQuery = from t in xml.Root.Descendants("header")
                                              select t.Element("version").Value + ", " +
                                                     t.Element("type").Value;
                        foreach (string headerRow in headerLinqQuery)
                        {
                            header = headerRow.Split(',');
                            InsertHeader(header, ref errorMessage);
                        }

                        var tradeLinqQuery = from t in xml.Root.Descendants("trade")
                                             select t.Element("id").Value + ", " +
                                                    t.Element("account").Value + ", " +
                                                    t.Element("volume").Value + ", " +
                                                    t.Element("comment").Value;

                        foreach (string tradeRow in tradeLinqQuery)
                        {
                            trade = tradeRow.Split(',');
                            InsertTrade(trade, ref errorMessage);
                        }
                        return true;
                    }
                    catch
                    {
                        if (0 == errorMessage.Length)
                            errorMessage = "Couldn't parse the selected file, please check format or correctness of data. Content must be in XML format!";
                        log.WriteLog(errorMessage);
                        return false;
                    }
                }
                // SQL Compact version
            }
        }

        public bool CreateCSVFile(Logger log, ref string errorMessage)
        {
            csvName = string.Format(@"{0}{1}.csv", @"CSV_", DateTime.Now.Ticks);
            csvPath = Path.Combine(Environment.CurrentDirectory, @"CSV files\");
            csvFullPath = Path.Combine(@csvPath, @csvName);
            if (0 != csvPath.Length)
            {
                if (!Directory.Exists(@csvPath))
                {
                    DirectoryInfo di = Directory.CreateDirectory(@csvPath);
                    Stream strim = File.Create(@csvFullPath);
                    strim.Close();
                }
                if (!File.Exists(@csvFullPath))
                {
                    Stream strim = File.Create(@csvFullPath);
                    strim.Close();
                }
                return true;
            }
            errorMessage = "Coundn't create csv file!";
            log.WriteLog(errorMessage);
            return false;
        }

        public bool SavetoCSVFile(Logger log, ref string errorMessage)
        {
            try
            {
                var xml = XDocument.Load(path);
                var headerQuery = from t in xml.Root.Descendants("header")
                                  select t.Element("version").Value + ", " +
                                         t.Element("type").Value;

                foreach (string headerCSV in headerQuery)
                {
                    csv.AppendLine(headerCSV);
                }

                var tradeQuery = from t in xml.Root.Descendants("trade")
                                 select t.Element("id").Value + ", " +
                                        t.Element("account").Value + ", " +
                                        t.Element("volume").Value + ", " +
                                        t.Element("comment").Value;


                foreach (string tradeCSV in tradeQuery)
                {
                    csv.AppendLine(tradeCSV);
                }
                File.WriteAllText(@csvFullPath, csv.ToString());
                return true;
            }
            catch(Exception e)
            {
                errorMessage = "Couldn't parse the selected file, please check format or correctness of data. Content must be in XML format!";
                log.WriteLog(errorMessage);
                return false;
            }
        }

        public bool IsNotEmpty()
        {
            if (path.Length > 0 || type.Length > 0 || SQLCeConnString.Length > 0 || SQLiteConnec.ConnectionString.Length > 0)
                return true;
            return false;
        }

        // Methods for SQLite
        private bool ExecuteQuery(string txtQuery)
        {
            if (type.Equals("sqlite"))
            {
                SQLiteFactory factory = (SQLiteFactory)DbProviderFactories.GetFactory("System.Data.SQLite");
                using (SQLiteConnec = (SQLiteConnection)factory.CreateConnection())
                {
                    SQLiteConnec.ConnectionString = "Data Source = " + SQLiteBaseName;
                    SQLiteConnec.Open();
                    SQLiteCommand = SQLiteConnec.CreateCommand();
                    SQLiteCommand.CommandText = txtQuery;
                    int status = SQLiteCommand.ExecuteNonQuery();
                    SQLiteConnec.Close();
                    if (status != 0)
                        return true;
                }
            }
            else
            {
                using (SQLCeConnec = new SqlCeConnection(SQLCeConnString))
                {
                    SQLCeConnec.Open();
                    SQLCeCommand = new SqlCeCommand(txtQuery, SQLCeConnec);
                    int status = SQLCeCommand.ExecuteNonQuery();
                    SQLCeConnec.Close();
                    if (status != 0)
                        return true;
                }
            }
            return false;
        }

        public string SelectTradeByID(Logger log, string id, ref string errorMessage)
        {
            string result = "";
            if (type.Equals("sqlite"))
            {
               SQLiteFactory factory = (SQLiteFactory)DbProviderFactories.GetFactory("System.Data.SQLite");
               using (SQLiteConnec = (SQLiteConnection)factory.CreateConnection())
               {
                   SQLiteConnec.ConnectionString = "Data Source = " + SQLiteBaseName;
                   SQLiteConnec.Open();
                   SQLiteCommand = new SQLiteCommand(SQLiteConnec);
                   SQLiteCommand.CommandText = "select * from trades where trade_id = " + id;
                   SQLiteCommand.CommandType = CommandType.Text;
                   SQLiteDataReader reader = SQLiteCommand.ExecuteReader();
                   if (reader.HasRows)
                   {
                       while (reader.Read())
                           result = reader["id"] + "," + reader["account"] + "," + reader["volume"] + "," + reader["comment"];
                   }
                   else
                   {
                       errorMessage = "Trade with this ID does not exist!";
                       log.WriteLog(errorMessage);
                       return null;
                   }
                   SQLiteConnec.Close();
                   return result;
               }
            }
            else
            {
                using (SQLCeConnec = new SqlCeConnection(SQLCeConnString))
                {
                    SQLCeConnec.Open();
                    string CommandText = "select * from trades where trade_id = " + id;
                    SQLCeCommand = new SqlCeCommand(CommandText, SQLCeConnec);
                    SqlCeDataReader reader = SQLCeCommand.ExecuteReader();
                    while (reader.Read())
                        result = reader["id"] + "," + reader["account"] + "," + reader["volume"] + "," + reader["comment"];
                    if (null == result || 0 == result.Length)
                    {
                        errorMessage = "Trade with this ID does not exist!";
                        log.WriteLog(errorMessage);
                        return null;
                    }
                    SQLCeConnec.Close();
                    return result;
                }
            }
            return null;
        }

        public bool DeleteTradeByID(Logger log, string id, ref string errorMessage)
        {
            if (type.Equals("sqlite"))
            {
               SQLiteFactory factory = (SQLiteFactory)DbProviderFactories.GetFactory("System.Data.SQLite");
               using (SQLiteConnec = (SQLiteConnection)factory.CreateConnection())
               {
                   SQLiteConnec.ConnectionString = "Data Source = " + SQLiteBaseName;
                   SQLiteConnec.Open();
                   string CommandText = "delete from trades where trade_id = " + id;
                   SQLiteCommand = SQLiteConnec.CreateCommand();
                   SQLiteCommand.CommandText = CommandText;
                   int status = SQLiteCommand.ExecuteNonQuery();
                   SQLiteConnec.Close();
                   if (status != 0)
                       return true;
               }               
            }
            else
            {
                using (SQLCeConnec = new SqlCeConnection(SQLCeConnString))
                {
                    SQLCeConnec.Open();
                    string CommandText = "delete from trades where trade_id = " + id;
                    SQLCeCommand = new SqlCeCommand(CommandText, SQLCeConnec);
                    int status = SQLCeCommand.ExecuteNonQuery();
                    SQLCeConnec.Close();
                    if (status != 0)
                        return true;
                }            
            }
            errorMessage = "Trade with this ID does not exist!";
            log.WriteLog(errorMessage);
            return false;
        }

        private void InsertTrade(string[] data, ref string errorMessage)
        {
            string txtSQLQuery = "insert into trades(trade_id,account,volume,comment) values (" + Int32.Parse(data[0]) + "," + Int32.Parse(data[1]) 
                + "," + Double.Parse(data[2]) + ",'" + data[3] + "')";
            if(!ExecuteQuery(txtSQLQuery))
            {
                errorMessage = "Could not insert to DB, maybe issue in data, if not contact with server owner!";
            }
        }

        private void InsertHeader(string[] data, ref string errorMessage)
        {
            string txtSQLQuery = "insert into headers(version,type) values (" + Int32.Parse(data[0]) + ",'" + data[1] + "')";
            if (!ExecuteQuery(txtSQLQuery))
            {
                errorMessage = "Could not insert to DB, maybe issue in data, if not contact with server owner!";
            }
        }
    }
}
