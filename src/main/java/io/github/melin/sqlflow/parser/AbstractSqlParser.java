package io.github.melin.sqlflow.parser;

import io.github.melin.sqlflow.parser.antlr4.SqlFlowParser;

import java.util.concurrent.atomic.AtomicReference;

public class AbstractSqlParser {
    private static AtomicReference<AntlrCaches> parserCaches =
        new AtomicReference<>(new AntlrCaches(SqlFlowParser._ATN));

    /**
     * Install the parser caches into the given parser.
     *
     * This method should be called before parsing any input.
     */
    public static void installCaches(SqlFlowParser parser) {
        parserCaches.get().installCaches(parser);
    }

    /**
     * Drop the existing parser caches and create a new one.
     *
     * ANTLR retains caches in its parser that are never released. This speeds
     * up parsing of future input, but it can consume a lot of memory depending
     * on the input seen so far.
     *
     * This method provides a mechanism to free the retained caches, which can
     * be useful after parsing very large SQL inputs, especially if those large
     * inputs are unlikely to be similar to future inputs seen by the driver.
     */
    public static void refreshParserCaches() {
        parserCaches.set(new AntlrCaches(SqlFlowParser._ATN));
    }
}
