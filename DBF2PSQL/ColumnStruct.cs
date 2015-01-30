using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DBF2PSQL
{
    public struct ColumnStruct
    {
        public string ColumnName;
        public ColumnType Type;
        public long param1, param2, length;
        public override string ToString()
        {
            return string.Format("{0} : {1}(p1: {2}, p2: {3}, len: {4})", ColumnName, Type, param1, param2, length);
        }
        public string GetFDBCreateString()
        {
            return string.Format("{0} {1}", ColumnName, GetFDBType());
        }
        public string GetFDBType()
        {
            switch (Type)
            {
                case ColumnType.Boolean:
                    return "CHAR(1)";
                case ColumnType.Character:
                    return string.Format("varchar({0})", length);
                case ColumnType.Date:
                    return "Date";
                case ColumnType.Double:
                    return "DOUBLESUKABLAT";
                case ColumnType.Float:
                    return "FLOATSUKABLAT";
                case ColumnType.Integer:
                    return "INTROTEBAL";
                default:
                    return "VNIMANIE! PROGRAMMU NUJNO DOPISAT!";
            }
        }
    }
    public enum ColumnType
    {
        Character, Integer, Date, Boolean, Float, Double
    }
}
