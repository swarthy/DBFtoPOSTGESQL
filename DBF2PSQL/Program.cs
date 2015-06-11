using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Web.Script.Serialization;
using SocialExplorer.IO.FastDBF;
using Npgsql;

namespace DBF2PSQL
{
    class Program
    {
        static StreamWriter log = null;
        static void help()
        {
            var oldColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine("DBFtoPSQL Converter v1.0");
            Console.WriteLine("Автор: Мочалин Александр\r\n");

            Console.WriteLine("Параметры запуска: DBFtoPSQL.exe -help|<yourConfig.json>\r\n");

            Console.ForegroundColor = oldColor;
        }
        static void BeginConvert(dynamic conf)
        {
            NpgsqlConnection pgConnection = new NpgsqlConnection(conf.pgConnection);
            pgConnection.Open();
            Console.WriteLine("pgServer:\r\nHost: {0} ({1})", pgConnection.Host, pgConnection.Port);
            Console.WriteLine("Версия: {0}\r\nБаза данных: {1}", pgConnection.ServerVersion, pgConnection.Database);
            foreach (var table in conf.DBFTables)
                ProcessConvert(table, pgConnection);
            if (conf.skipVacuum == null)
            {
                var vacuum = pgConnection.CreateCommand();
                Console.WriteLine("VACUUM FULL! Может занять продолжительное время");
                vacuum.CommandText = "VACUUM FULL;";
                vacuum.CommandTimeout = 60 * 60;//1 hour
                vacuum.ExecuteNonQuery();
            }
            
            if (conf.execAfter != null)
            {
                foreach (var script in conf.execAfter)
                {
                    var command = pgConnection.CreateCommand();
                    Console.WriteLine("SCRIPT: {0}", script);
                    command.CommandText = script;
                    command.CommandTimeout = 60 * 60;//1 hour
                    command.ExecuteNonQuery();
                }
            }
            pgConnection.Clone();
        }
        static void clearPgTable(string tableName, NpgsqlConnection pgConnection)
        {
            var command = pgConnection.CreateCommand();
            command.CommandText = string.Format("DELETE FROM \"{0}\"", tableName);
            command.ExecuteNonQuery();
            Console.WriteLine("Таблица {0} очищена!", tableName);
        }

        static string dumpRecord(DbfRecord record)
        {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < record.ColumnCount; i++)
                sb.AppendFormat("{0} = {1}\r\n", record.Column(i).Name, record[i].Trim());
            return sb.ToString();
        }

        static List<FieldItem> parseFields(dynamic fields)
        {
            List<FieldItem> result = new List<FieldItem>();
            for (int i = 0; i < fields.Count; i++)
            {
                var field = new FieldItem();
                if (fields[i] is string)
                {
                    field.dbfName = field.pgName = fields[i];
                }
                else
                {
                    field.dbfName = fields[i].dbf;
                    field.pgName = fields[i].pg;
                }
                result.Add(field);
            }
            return result;
        }

        static void ProcessConvert(dynamic info, NpgsqlConnection pgConnection)
        {
            bool errLog = log != null;
            if (info.skip != null)
                return;
            var fields = parseFields(info.fields);
            string insertCommandText = createInsertCommandString(info.pgTableName, fields);
            if (info.clearPgTable != null && info.clearPgTable)
                clearPgTable(info.pgTableName, pgConnection);
            var encoding = info.dbfEncoding != null ? info.dbfEncoding : "utf8";

            DbfFile f = new DbfFile(Encoding.GetEncoding(encoding));
            f.Open(info.path, FileMode.Open);
            DbfRecord record = new DbfRecord(f.Header);
            Console.Write("Dbf header: ");
            for (int i = 0; i < f.Header.ColumnCount; i++)
                Console.Write("{0} {1}({2}) ", f.Header[i].Name, f.Header[i].ColumnTypeChar, f.Header[i].Length);
            Console.WriteLine();
            int count = 0;
            NpgsqlCommand command;
            var start = DateTime.Now;
            //var transaction = pgConnection.BeginTransaction();
            while (f.ReadNext(record))
            {
                if (record.IsDeleted)
                    continue;
                command = pgConnection.CreateCommand();
                //command.Transaction = transaction;
                command.CommandText = insertCommandText;

                for (int i = 0; i < fields.Count; i++)
                {
                    var value = record[fields[i].dbfName].Trim();
                    if (value == "" || (f.Header[fields[i].dbfName].ColumnType == DbfColumn.DbfColumnType.Date && value == "00-1-1-1")) //empty date bug
                        value = null;
                    else
                        if (f.Header[fields[i].dbfName].ColumnType == DbfColumn.DbfColumnType.Date)
                        {
                            string date = value as String;
                            value = string.Format("{0}-{1}-{2} 01:00:00+06", date.Substring(0, 4), date.Substring(4, 2), date.Substring(6, 2)); //гори в аду javascript
                        }
                    command.Parameters.AddWithValue("@" + fields[i].pgName, value);
                }
                try
                {
                    command.ExecuteNonQuery();
                }
                catch (Exception e)
                {
                    var oldColor = Console.ForegroundColor;
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("Запись [{0}]: {1}", record.RecordIndex, dumpRecord(record));
                    Console.WriteLine("Ошибка сохранения: {0}", e.Message);
                    Console.ForegroundColor = oldColor;
                    if (errLog)
                    {
                        log.WriteLine("{0} [{1}]: {2}", info.pgTableName, record.RecordIndex, dumpRecord(record));
                        log.WriteLine("Ошибка: {0}", e.Message);
                        log.WriteLine("==============================================================================================================");
                    }
                }
                count++;
                if (count % 50 == 0)
                {
                    Console.Write("\rОбработано: {0}/{1} ({2:F2}%)", count, f.Header.RecordCount, (float)(count * 100) / f.Header.RecordCount);
                }
            }
            /*try
            {
                transaction.Commit();
            }
            catch (Exception e)
            {
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine("Ошибка транзакции: {0}", e.Message);
                Console.ForegroundColor = oldColor;
            }*/
            f.Close();
            Console.WriteLine("\rТаблица \"{0}\": {1} записей [{2}]", info.pgTableName, count, DateTime.Now - start);
        }
        static string createInsertCommandString(string tableName, List<FieldItem> fields)
        {
            var valueStr = new StringBuilder();
            var fieldsStr = new StringBuilder();
            for (int i = 0; i < fields.Count; i++)
            {
                valueStr.AppendFormat("@{0}", fields[i].pgName);
                fieldsStr.AppendFormat("\"{0}\"", fields[i].pgName);
                if (i < fields.Count - 1)
                {
                    valueStr.Append(", ");
                    fieldsStr.Append(", ");
                }
            }
            return string.Format("INSERT INTO \"{0}\" ({1}) VALUES ({2})", tableName, fieldsStr, valueStr);
        }

        static void Main(string[] args)
        {
            const string defaultConfig = "config.json";
            Console.Title = "DBF2PostgreSQL Converter v1.0";
            Console.ForegroundColor = ConsoleColor.Green;
            string config = args.Length > 0 ? args[0] : defaultConfig;
            if (config == "-help")
            {
                help();
                return;
            }

            if (!File.Exists(config))
            {
                Console.WriteLine("Конфигурационный файл {0} не найден. Аварийное завершение.", config);
                return;
            }

            Console.WriteLine("Файл конфигурации: {0}", config);

            string data = File.ReadAllText(config);

            var serializer = new JavaScriptSerializer();
            serializer.RegisterConverters(new[] { new DynamicJsonConverter() });
            //try
            //{
            dynamic obj = null;
            try
            {
                obj = serializer.Deserialize(data, typeof(object));
            }catch(Exception e)
            {
                Console.WriteLine("Ошибка чтения конфига: {0}", e);
                Console.ReadLine();
                return;
            }

            if (obj.errorlog != null)
                log = File.CreateText(obj.errorlog);
            var start = DateTime.Now;
            BeginConvert(obj);
            Console.WriteLine("Преобразование завершено. Время: {0}", DateTime.Now - start);

            if (log != null)
                log.Close();
            if (obj.pause != null)
            {
                Console.WriteLine("Нажмите Enter для выхода");
                Console.ReadLine();
            }
            /*}
            catch (Exception e)
            {
                Console.WriteLine("Ошибка! Аварийное завершение. {0}", e);
                Console.ReadLine();
                return;
            }*/
#if DEBUG
            Console.ReadLine();
#endif
        }
        struct FieldItem
        {
            public string dbfName, pgName;
        }
    }
}
