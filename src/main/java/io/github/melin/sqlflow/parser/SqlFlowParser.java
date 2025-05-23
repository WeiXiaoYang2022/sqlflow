package io.github.melin.sqlflow.parser;

import io.github.melin.sqlflow.parser.antlr4.SqlFlowLexer;
import io.github.melin.sqlflow.parser.antlr4.SqlFlowParserBaseListener;
import io.github.melin.sqlflow.tree.Node;
import io.github.melin.sqlflow.tree.statement.Statement;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static io.github.melin.sqlflow.parser.AntlrCaches.RELEASE_ANTLR_CACHE_AFTER_PARSING;
import static io.github.melin.sqlflow.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static java.util.Objects.requireNonNull;

/**
 * huaixin 2021/12/18 11:04 PM
 */
public class SqlFlowParser {
    private static final BiConsumer<SqlFlowLexer, io.github.melin.sqlflow.parser.antlr4.SqlFlowParser> DEFAULT_PARSER_INITIALIZER =
            (SqlFlowLexer lexer, io.github.melin.sqlflow.parser.antlr4.SqlFlowParser parser) -> {
    };

    private final BiConsumer<SqlFlowLexer, io.github.melin.sqlflow.parser.antlr4.SqlFlowParser> initializer;

    public SqlFlowParser() {
        this(DEFAULT_PARSER_INITIALIZER);
    }

    public SqlFlowParser(BiConsumer<SqlFlowLexer, io.github.melin.sqlflow.parser.antlr4.SqlFlowParser> initializer) {
        this.initializer = requireNonNull(initializer, "initializer is null");
    }

    public Statement createStatement(String sql) {
        try {
            return (Statement) invokeParser("statement", sql,
                    io.github.melin.sqlflow.parser.antlr4.SqlFlowParser::singleStatement,
                    new ParsingOptions(AS_DECIMAL));
        } catch (ParseException e) {
            if(StringUtils.isNotBlank(e.getCommand())) {
                throw e;
            } else {
                throw e.withCommand(sql);
            }
        }
    }

    private Node invokeParser(String name, String sql,
                              Function<io.github.melin.sqlflow.parser.antlr4.SqlFlowParser, ParserRuleContext> parseFunction,
                              ParsingOptions parsingOptions) {
        try {
            UpperCaseCharStream charStream = new UpperCaseCharStream(CharStreams.fromString(sql));
            SqlFlowLexer lexer = new SqlFlowLexer(charStream);
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            io.github.melin.sqlflow.parser.antlr4.SqlFlowParser parser = new io.github.melin.sqlflow.parser.antlr4.SqlFlowParser(tokenStream);
            AbstractSqlParser.installCaches(parser);
            initializer.accept(lexer, parser);

            // Override the default error strategy to not attempt inserting or deleting a token.
            // Otherwise, it messes up error reporting
            parser.setErrorHandler(new DefaultErrorStrategy() {
                @Override
                public Token recoverInline(Parser recognizer)
                        throws RecognitionException {
                    if (nextTokensContext == null) {
                        throw new InputMismatchException(recognizer);
                    } else {
                        throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                    }
                }
            });

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames()), parser));

            lexer.removeErrorListeners();
            lexer.addErrorListener(new ParseErrorListener());

            parser.removeErrorListeners();
            parser.addErrorListener(new ParseErrorListener());

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply(parser);
            } catch (Exception ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply(parser);
            } finally {
                String releaseAntlrCache = System.getenv(RELEASE_ANTLR_CACHE_AFTER_PARSING);
                if (releaseAntlrCache == null || "true".equals(releaseAntlrCache)) {
                    AbstractSqlParser.refreshParserCaches();
                }
            }

            return new AstBuilder(parsingOptions).visit(tree);
        } catch (StackOverflowError e) {
            throw new ParsingException(name + " is too large (stack overflow while parsing)");
        }
    }

    private static class PostProcessor extends SqlFlowParserBaseListener {
        private final List<String> ruleNames;
        private final io.github.melin.sqlflow.parser.antlr4.SqlFlowParser parser;

        public PostProcessor(List<String> ruleNames, io.github.melin.sqlflow.parser.antlr4.SqlFlowParser parser) {
            this.ruleNames = ruleNames;
            this.parser = parser;
        }

        @Override
        public void exitNonReserved(io.github.melin.sqlflow.parser.antlr4.SqlFlowParser.NonReservedContext context) {
            // we can't modify the tree during rule enter/exit event handling unless we're dealing with a terminal.
            // Otherwise, ANTLR gets confused and fires spurious notifications.
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

            // replace nonReserved words with IDENT tokens
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            Token newToken = new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    SqlFlowLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex());

            context.getParent().addChild(parser.createTerminalNode(context.getParent(), newToken));
        }
    }
}
