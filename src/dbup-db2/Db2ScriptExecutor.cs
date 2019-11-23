using System;
using System.Collections.Generic;
using System.Data;
using System.Globalization;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support;

#if NET451
    using IBM.Data.DB2;
#endif

#if NETSTANDARD2_0
using IBM.Data.DB2.Core;
#endif

namespace DbUp.Db2
{
    public class Db2ScriptExecutor : ScriptExecutor
    {
        /// <summary>
        /// Initializes an instance of the <see cref="Db2ScriptExecutor"/> class.
        /// </summary>
        /// <param name="connectionManagerFactory"></param>
        /// <param name="log">The logging mechanism.</param>
        /// <param name="schema">The schema that contains the table.</param>
        /// <param name="variablesEnabled">Function that returns <c>true</c> if variables should be replaced, <c>false</c> otherwise.</param>
        /// <param name="scriptPreprocessors">Script Preprocessors in addition to variable substitution</param>
        /// <param name="journalFactory">Database journal</param>
        public Db2ScriptExecutor(Func<IConnectionManager> connectionManagerFactory, Func<IUpgradeLog> log, string schema, Func<bool> variablesEnabled,
            IEnumerable<IScriptPreprocessor> scriptPreprocessors, Func<IJournal> journalFactory)
            : base(connectionManagerFactory, new Db2ObjectParser(), log, schema, variablesEnabled, scriptPreprocessors, journalFactory)
        {

        }

        protected override string GetVerifySchemaSql(string schema)
        {
            throw new NotSupportedException();
        }

        /// <summary>
        /// Executes the specified script against a database at a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        public override void Execute(SqlScript script)
        {
            Execute(script, null);
        }

        protected override void ExecuteCommandsWithinExceptionHandler(int index, SqlScript script, Action executeCommand)
        {
            try
            {
                executeCommand();
            }


            catch (DB2Exception exception)
            {
                var code = exception.ErrorCode;

                Log().WriteInformation("Db2 exception has occured in script: '{0}'", script.Name);
                Log().WriteError("Db2 error code: {0}; Number {1}; Message: {2}", index, code, exception.ErrorCode, exception.Message);
                Log().WriteError(exception.ToString());
                throw;
            }
        }
    }

}


