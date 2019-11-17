using System;
using System.Reflection;
using DbUp;
using DbUp.Db2;

namespace Db2SampleApplication
{
    class Program
    {
        static void Main(string[] args)
        {
            var connectionString = @"Server=localhost:50000;Database=testdb;UID=db2inst1;PWD=admin;";

            var upgradeEngineBuilder = DeployChanges.To
                .Db2Database(connectionString, "AP")
                .WithScriptsEmbeddedInAssembly(Assembly.GetExecutingAssembly())
                .LogToConsole();

            var upgrader = upgradeEngineBuilder.Build();
            var result = upgrader.PerformUpgrade();

            // Display the result
            if (result.Successful)
            {
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine("Success!");
            }
            else
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(result.Error);
                Console.WriteLine("Failed!");
            }
            Console.ReadKey();
        }
    }
}
