using System;
using System.Collections.Generic;
using System.Text;
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
    public class Db2ConnectionManager : DatabaseConnectionManager
    {
        /// <summary>
        /// Creates a new DB2 database connection.
        /// </summary>
        /// <param name="connectionString">The DB2 connection string.</param>
        public Db2ConnectionManager(string connectionString)
            : base(new DelegateConnectionFactory(l => new DB2Connection(connectionString)))
        {
        }

        public override IEnumerable<string> SplitScriptIntoCommands(string scriptContents)
        {
            var commandSplitter = new Db2CommandSplitter();
            var scriptStatements = commandSplitter.SplitScriptIntoCommands(scriptContents);
            return scriptStatements;
        }
    }
}


