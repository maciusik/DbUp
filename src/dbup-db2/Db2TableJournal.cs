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
        bool journalExists;
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
            var fqSchemaTableName = UnquotedSchemaTableName;
            return
                $@" CREATE TABLE {FqSchemaTableName} 
                (
                    schemaversionid INT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
                    scriptname VARCHAR(255) NOT NULL,
                    applied TIMESTAMP NOT NULL,
                    CONSTRAINT PK_{ fqSchemaTableName } PRIMARY KEY (schemaversionid) 
                )";
        }

        //protected string CreateSchemaTableSequenceSql()
        //{
        //    return $@" CREATE SEQUENCE {fqSchemaTableName}_sequence";
        //}

        //protected string CreateSchemaTableTriggerSql()
        //{
        //    return $@" CREATE OR REPLACE TRIGGER {UnquotedSchemaTableName}_on_insert
        //            BEFORE INSERT ON {fqSchemaTableName}
        //            FOR EACH ROW
        //            BEGIN
        //                SELECT {fqSchemaTableName}_sequence.nextval
        //                INTO :new.schemaversionid
        //                FROM dual;
        //            END;
        //        ";
        //}

        protected override string GetInsertJournalEntrySql(string scriptName, string applied)
        {
            return $"insert into {FqSchemaTableName} (ScriptName, Applied) values ({scriptName}, {applied})";
        }

        protected override string GetJournalEntriesSql()
        {
            var unquotedSchemaTableName = UnquotedSchemaTableName.ToUpper(English);
            return $"select scriptname from {unquotedSchemaTableName} order by scriptname";
        }

        protected override string DoesTableExistSql()
        {
            var unquotedSchemaTableName = UnquotedSchemaTableName.ToUpper(English);
            return string.IsNullOrEmpty(SchemaTableSchema)
             ? $"select tabname from syscat.tables where tabname = '{unquotedSchemaTableName}' FETCH FIRST 1 ROWS ONLY" :
             $"select tabname from syscat.tables where tabschema='{SchemaTableSchema}' and tabname = '{unquotedSchemaTableName}' FETCH FIRST 1 ROWS ONLY"; ;
        }

        //protected IDbCommand GetCreateTableSequence(Func<IDbCommand> dbCommandFactory)
        //{
        //    var command = dbCommandFactory();
        //    command.CommandText = CreateSchemaTableSequenceSql();
        //    command.CommandType = CommandType.Text;
        //    return command;
        //}

        //protected IDbCommand GetCreateTableTrigger(Func<IDbCommand> dbCommandFactory)
        //{
        //    var command = dbCommandFactory();
        //    command.CommandText = CreateSchemaTableTriggerSql();
        //    command.CommandType = CommandType.Text;
        //    return command;
        //}

        //public override void EnsureTableExistsAndIsLatestVersion(Func<IDbCommand> dbCommandFactory)
        //{
        //    if (!journalExists && !DoesTableExist(dbCommandFactory))
        //    {
        //        Log().WriteInformation(string.Format("Creating the {0} table", FqSchemaTableName));

        //        // We will never change the schema of the initial table create.
        //        using (var command = GetCreateTableSequence(dbCommandFactory))
        //        {
        //            command.ExecuteNonQuery();
        //        }

        //        // We will never change the schema of the initial table create.
        //        using (var command = GetCreateTableCommand(dbCommandFactory))
        //        {
        //            command.ExecuteNonQuery();
        //        }

        //        //// We will never change the schema of the initial table create.
        //        //using (var command = GetCreateTableTrigger(dbCommandFactory))
        //        //{
        //        //    command.ExecuteNonQuery();
        //        //}

        //        Log().WriteInformation(string.Format("The {0} table has been created", FqSchemaTableName));

        //        OnTableCreated(dbCommandFactory);
        //    }

        //    journalExists = true;
        //}
    }

}


