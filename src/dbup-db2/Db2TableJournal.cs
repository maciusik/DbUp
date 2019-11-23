using System;
using System.Data;
using System.Globalization;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;

namespace DbUp.Db2
{
    public class Db2TableJournal : TableJournal
    {
        /// <summary>
        /// Creates a new Db2 table journal.
        /// </summary>
        /// <param name="connectionManager">The Db2 connection manager.</param>
        /// <param name="logger">The upgrade logger.</param>
        /// <param name="schema">The name of the schema the journal is stored in.</param>
        /// <param name="table">The name of the journal table.</param>
        public Db2TableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string schema, string table)
            : base(connectionManager, logger, new Db2ObjectParser(), schema, table)
        {
            fqSchemaTableName = string.IsNullOrEmpty(SchemaTableSchema) ? UnquotedSchemaTableName : $"{ SchemaTableSchema}.{UnquotedSchemaTableName}";
        }

        public static CultureInfo English = new CultureInfo("en-US", false);

        /// <summary>
        /// Fully qualified schema table name, includes schema and is non-quoted.
        /// </summary>
        /// 
        private string fqSchemaTableName;
        protected override string CreateSchemaTableSql(string quotedPrimaryKeyName)
        {
            return
                $@" CREATE TABLE {fqSchemaTableName} 
                (
                    schemaversionid INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                    scriptname VARCHAR(255) NOT NULL,
                    applied TIMESTAMP NOT NULL,
                    CONSTRAINT PK_{ UnquotedSchemaTableName } PRIMARY KEY (schemaversionid) 
                )";
        }

        protected override string GetInsertJournalEntrySql(string scriptName, string applied)
        {
            return $"insert into {fqSchemaTableName} (ScriptName, Applied) values ({scriptName}, {applied})";
        }

        protected override string GetJournalEntriesSql()
        {
            return $"select scriptname from {fqSchemaTableName} order by scriptname";
        }

        protected override string DoesTableExistSql()
        {
            var unquotedSchemaTableName = UnquotedSchemaTableName.ToUpper(English);
            return string.IsNullOrEmpty(SchemaTableSchema)
             ? $"select 1 from syscat.tables where tabname = '{unquotedSchemaTableName}' FETCH FIRST 1 ROWS ONLY" :
             $"select 1 from syscat.tables where tabschema='{SchemaTableSchema}' and tabname = '{unquotedSchemaTableName}' FETCH FIRST 1 ROWS ONLY"; ;
        }
    }

}


