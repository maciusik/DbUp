using System.Collections.Generic;

namespace DbUp.Db2
{
    public class Db2CommandSplitter
    {
        /// <summary>
        /// Splits a script with multiple delimited commands into commands
        /// </summary>
        /// <param name="scriptContents"></param>
        /// <returns></returns>
        public IEnumerable<string> SplitScriptIntoCommands(string scriptContents)
        {
            using (var reader = new Db2CommandReader(scriptContents))
            {
                var commands = new List<string>();
                reader.ReadAllCommands(c => commands.Add(c));
                return commands;
            }
        }
    }

}
