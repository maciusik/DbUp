using DbUp.Support;

namespace DbUp.Db2
{
    public class Db2ObjectParser : SqlObjectParser
    {
        public Db2ObjectParser() : base("\"", "\"")
        {
        }
    }
}


