using System.Text;

namespace Cortex.Mediator.SourceGenerator.Emitters
{
    internal sealed class SourceTextBuilder
    {
        private readonly StringBuilder _sb = new StringBuilder();
        private int _indent;

        public SourceTextBuilder Indent()
        {
            _indent++;
            return this;
        }

        public SourceTextBuilder Unindent()
        {
            if (_indent > 0) _indent--;
            return this;
        }

        public SourceTextBuilder AppendLine(string line = "")
        {
            if (string.IsNullOrEmpty(line))
            {
                _sb.AppendLine();
            }
            else
            {
                _sb.Append(new string(' ', _indent * 4));
                _sb.AppendLine(line);
            }
            return this;
        }

        public SourceTextBuilder OpenBrace()
        {
            AppendLine("{");
            Indent();
            return this;
        }

        public SourceTextBuilder CloseBrace(string suffix = "")
        {
            Unindent();
            AppendLine("}" + suffix);
            return this;
        }

        public override string ToString() => _sb.ToString();
    }
}
