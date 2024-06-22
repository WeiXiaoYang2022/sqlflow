package io.github.melin.sqlflow.parser.presto;

import io.github.melin.sqlflow.analyzer.Analysis;
import io.github.melin.sqlflow.analyzer.OutputColumn;
import io.github.melin.sqlflow.analyzer.StatementAnalyzer;
import io.github.melin.sqlflow.metadata.QualifiedObjectName;
import io.github.melin.sqlflow.parser.AbstractSqlLineageTest;
import io.github.melin.sqlflow.parser.SqlFlowParser;
import io.github.melin.sqlflow.tree.statement.Statement;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;

import java.util.Optional;

import static java.util.Collections.emptyMap;

/**
 * huaixin 2021/12/18 11:13 PM
 */
public class PrestoSqlLineageTest extends AbstractSqlLineageTest {

    protected static final SqlFlowParser SQL_PARSER = new SqlFlowParser();

    @Test
    public void testInsertInto() throws Exception {
        String sql = "insert into Demo select concat(a.COL1, '-', a.COL2), " +
                "sum(a.row_num * 1.00000) as num " +
                "from test a where ds='201912' group by type";
        Statement statement = SQL_PARSER.createStatement(sql);

        Analysis analysis = new Analysis(statement, emptyMap());
        StatementAnalyzer statementAnalyzer = new StatementAnalyzer(
                analysis, new SimplePrestoMetadataService(), SQL_PARSER, false);
        
        statementAnalyzer.analyze(statement, Optional.empty());

        //System.out.println(MapperUtils.toJSONString(analysis.getTarget().get()));

        assertLineage(analysis, new OutputColumn("NAME", ImmutableSet.of(
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.test"), "COL1"),
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.test"), "COL2")
        )), new OutputColumn("row_num", ImmutableSet.of(
                new Analysis.SourceColumn(QualifiedObjectName.valueOf("default.test"), "row_num")
        )));
    }
}
