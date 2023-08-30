using System.Collections.Generic;

namespace Langy
{
    public class NewLanguage
    {
        public string Code { get; set; }
        public string Name { get; set; }
        public List<LanguageItem> LanguageItems { get; set; }
    }

    public class LanguageItem
    {
        public string Key { get; set; }
        public string Text { get; set; }
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
