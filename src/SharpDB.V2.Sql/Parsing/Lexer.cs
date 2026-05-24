namespace SharpDB.V2.Sql.Parsing;

public enum TokenType
{
    Select, Insert, Update, Delete, From, Where, Join, Inner, Left, Right,
    On, Order, By, Asc, Desc, Limit, Into, Values, Set,
    Identifier, Number, String, Star,
    Comma, Dot, LParen, RParen, Semicolon,
    Eq, Neq, Lt, Gt, Lte, Gte, And, Or, Not,
    Eof
}

public readonly record struct Token(TokenType Type, string Text, int Position);

public sealed class Lexer
{
    public Lexer(string input) { }
    public IReadOnlyList<Token> Tokenize() => throw new NotImplementedException();
}
