package io.github.melin.sqlflow.parser.flink

import io.github.melin.sqlflow.analyzer.Analysis
import io.github.melin.sqlflow.analyzer.StatementAnalyzer
import io.github.melin.sqlflow.parser.SqlFlowParser
import io.github.melin.sqlflow.util.JsonUtils
import org.junit.Test
import java.util.*

class FlinkSqlLineageTest {

    protected val SQL_PARSER = SqlFlowParser()

    @Test
    @Throws(Exception::class)
    fun testInsertInto() {
        val sql = """
            INSERT INTO PROCESSED_MDM_PRODUCT_ENRICHMENT(PROD_ID, ENRICHMENT_ID)
            select if(trim(ITEM) = '',cast(null as string), ITEM) as PROD_ID, ENRICHMENT_ID 
            from RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT
        """.trimIndent()
        val statement = SQL_PARSER.createStatement(sql)
        val analysis = Analysis(statement, emptyMap())
        val statementAnalyzer = StatementAnalyzer(
            analysis,
            SimpleFlinkMetadataService(), SQL_PARSER
        )
        statementAnalyzer.analyze(statement, Optional.empty())

        System.out.println(JsonUtils.toJSONString(analysis.getTarget().get()));
    }
}