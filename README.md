# DBF to Postgresql Converter
##
### DBF format
dBASE III/IV
####FoxPro not supported! Convert to dBase III/IV first!
### config.json example

```
{
    "pgConnection": "Server=127.0.0.1;Port=5432;Database=dbname;User Id=postgres;Password=admin;",
    "DBFTables": [{
        "path": "path/to/table1.dbf",
        "dbfEncoding": 866,
        "pgTableName": "table1",
        "clearPgTable": true,
        "fields": [{
            "dbf": "CODE",
            "pg": "code"
        }, {
            "dbf": "NAME",
            "pg": "name"
        }, {
            "dbf": "ADDRESS",
            "pg": "address"
        }]
    }, {
        "path": "path/to/table2.dbf",
        "dbfEncoding": 1251,
        "pgTableName": "secondtable",
        "clearPgTable": false,
        "fields": [{
            "dbf": "CODE",
            "pg": "code"
        }, {
            "dbf": "NAME",
            "pg": "name"
        }]
    }],
    "execAfter": [
        "UPDATE table1 SET name = false"
    ],
    "errorlog": "error.txt",
    "skipVacuum": true,
    "pause": true
}
```
