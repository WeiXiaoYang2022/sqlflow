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
            select if(ITEM IS NOT NULL, 1, 0) as PROD_ID, ENRICHMENT_ID 
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

    @Test
    @Throws(Exception::class)
    fun testInsertInto1() {
        val sql = """
            INSERT INTO PROCESSED_MDM_PRODUCT_ENRICHMENT(PROD_ID, ENRICHMENT_ID)
            SELECT CASE
                WHEN TRIM(A.ITEM) = '' THEN CAST(NULL AS STRING)
                ELSE ITEM END AS PROD_ID, 
                ENRICHMENT_ID
            FROM RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT A
            LEFT JOIN MDM_DIM_XX_ITEM_ATTR_TRANSLATE_LOOKUPMAP_ORACLE_DIM B
            ON A.ITEM = B.PROD_ID
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

    @Test
    @Throws(Exception::class)
    fun testInsertInto2() {
        val sql = """
            INSERT INTO PROCESSED_MDM_PRODUCT_ENRICHMENT(PROD_ID, ENRICHMENT_ID)
            SELECT ITEM AS PROD_ID, 
                ENRICHMENT_ID
            FROM RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT 
            union
            SELECT ITEM PROD_ID, 
                UDA_VALUE_ID ENRICHMENT_ID
            FROM MDM_DIM_XX_ITEM_ATTR_TRANSLATE_LOOKUPMAP_ORACLE_DIM 
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

    @Test
    @Throws(Exception::class)
    fun testInsertInto3() {
        val sql = """
            INSERT INTO PROCESSED_MDM_PRODUCT_ENRICHMENT(PROD_ID, ENRICHMENT_ID)
            SELECT B.PROD_ID, A.ATTRIB_ID AS ENRICHMENT_ID FROM mdm_dim_product_attrib_type_lookupmap_mysql A
            LEFT JOIN (
                SELECT ITEM AS PROD_ID, 
                    ENRICHMENT_ID
                FROM RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT 
                union
                SELECT ITEM PROD_ID, 
                    UDA_VALUE_ID ENRICHMENT_ID
                FROM MDM_DIM_XX_ITEM_ATTR_TRANSLATE_LOOKUPMAP_ORACLE_DIM 
            ) B 
            ON A.PROD_ID = B.PROD_ID
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

    @Test
    @Throws(Exception::class)
    fun testInsertInto4() {
        val sql = """
            INSERT IGNORE INTO PROCESSED_MDM_PRODUCT_ENRICHMENT(PROD_ID, ENRICHMENT_VALUE)
            SELECT ITEM AS PROD_ID, GROUP_CONCAT(LANG SEPARATOR '; ') AS ENRICHMENT_VALUE 
            FROM RETEK_XX_ITEM_ATTR_TRANSLATE_PRODUCT_ENRICHMENT
            GROUP BY ITEM
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