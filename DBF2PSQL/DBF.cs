using System;
using System.Collections.Generic;
using System.Data;
using System.Data.OleDb;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBF2PSQL
{
    public static class DBF
    {
        internal static List<ColumnStruct> getTableStruct(String tableName, OleDbConnection connection)
        {
            List<ColumnStruct> res = new List<ColumnStruct>();

            DataTable tableColumns = connection.GetOleDbSchemaTable(OleDbSchemaGuid.Columns, new object[] { null, null, tableName, null });
            foreach (DataRow row in tableColumns.Rows)
            {
                var info = new ColumnStruct();
                info.ColumnName = row["COLUMN_NAME"].ToString();
                switch ((int)row["DATA_TYPE"])
                {
                    case 3:
                        info.Type = ColumnType.Integer;
                        break;
                    case 129:
                        info.Type = ColumnType.Character;
                        info.length = (long)row[13];                        
                        break;
                    case 131:
                        info.Type = ColumnType.Float;
                        info.param1 = (long)row[15];
                        info.param2 = (long)row[16];
                        break;
                    case 133:
                        info.Type = ColumnType.Date;
                        break;
                }
                res.Add(info);
            }

            return res;
        }
    }
}
