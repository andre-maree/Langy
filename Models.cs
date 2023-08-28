using System.Collections.Generic;

namespace Langy
{
    public class NewLanguage
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public Dictionary<string, string> LanguageItems { get; set; }
    }

    public class GroupObject
    {
        public string Group { get; set; }
        public List<string> Keys { get; set; }
    }

    public class MetaData
    {
        public List<GroupObject> Groups { get; set; }
        public List<string> Codes { get; set; }
    }
}
