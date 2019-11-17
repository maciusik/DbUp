using System.Linq;
using DbUp.Builder;
using DbUp.Engine.Transactions;

namespace DbUp.Db2
{
    public static class Db2Extensions
    {
        public static UpgradeEngineBuilder Db2Database(this SupportedDatabases supported, string connectionString)
        {
            foreach (var pair in connectionString.Split(';').Select(s => s.Split('=')).Where(pair => pair.Length == 2).Where(pair => pair[0].ToLower() == "database"))
            {
                return Db2Database(new Db2ConnectionManager(connectionString), pair[1]);
            }

            return Db2Database(new Db2ConnectionManager(connectionString));
        }

        /// <summary>
        /// Creates an upgrader for Db2 databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionString">Db2 database connection string.</param>
        /// <param name="schema">Which Db2 schema to check for changes</param>
        /// <returns>
        /// A builder for a database upgrader designed for Db2 databases.
        /// </returns>
        public static UpgradeEngineBuilder Db2Database(this SupportedDatabases supported, string connectionString, string schema)
        {
            return Db2Database(new Db2ConnectionManager(connectionString), schema);
        }

        /// <summary>
        /// Creates an upgrader for Db2 databases.
        /// </summary>
        /// <param name="supported">Fluent helper type.</param>
        /// <param name="connectionManager">The <see cref="Db2ConnectionManager"/> to be used during a database upgrade.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Db2 databases.
        /// </returns>
        public static UpgradeEngineBuilder Db2Database(this SupportedDatabases supported, IConnectionManager connectionManager)
            => Db2Database(connectionManager);

        /// <summary>
        /// Creates an upgrader for Db2 databases.
        /// </summary>
        /// <param name="connectionManager">The <see cref="Db2ConnectionManager"/> to be used during a database upgrade.</param>
        /// <returns>
        /// A builder for a database upgrader designed for Db2 databases.
        /// </returns>
        public static UpgradeEngineBuilder Db2Database(IConnectionManager connectionManager)
        {
            return Db2Database(connectionManager, null);
        }

        /// <summary>
        /// Creates an upgrader for Db2 databases.
        /// </summary>
        /// <param name="connectionManager">The <see cref="Db2ConnectionManager"/> to be used during a database upgrade.</param>
        /// /// <param name="schema">Which Db2 schema to check for changes</param>
        /// <returns>
        /// A builder for a database upgrader designed for Db2 databases.
        /// </returns>
        public static UpgradeEngineBuilder Db2Database(IConnectionManager connectionManager, string schema)
        {
            var builder = new UpgradeEngineBuilder();
            builder.Configure(c => c.ConnectionManager = connectionManager);
            builder.Configure(c => c.ScriptExecutor = new Db2ScriptExecutor(() => c.ConnectionManager, () => c.Log, null, () => c.VariablesEnabled, c.ScriptPreprocessors, () => c.Journal));
            builder.Configure(c => c.Journal = new Db2TableJournal(() => c.ConnectionManager, () => c.Log, schema, "schemaversions"));
            builder.WithPreprocessor(new Db2Preprocessor());
            return builder;
        }
    }

}


