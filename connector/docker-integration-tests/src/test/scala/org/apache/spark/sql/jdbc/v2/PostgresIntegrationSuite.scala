/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.jdbc.v2

import java.sql.Connection

import com.databricks.spark.sql.jdbc.DatabricksPostgresDockerConfig

import org.apache.spark.SparkConf
import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * *** DBR DEPRECATED ***
 * To run this test suite for a specific version (e.g., postgres:16.2)
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:16.2
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.PostgresIntegrationSuite"
 * }}}
 * *** DBR DEPRECATED ***
 * We have moved to using databricks registry for the docker images in DBR, so we cannot
 * specify arbitrary docker images anymore.
 */
@DockerTest
class PostgresIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest
  with DatabricksPostgresDockerConfig {
  override val catalogName: String = "postgresql"

  override def url: String = getJdbcUrl(dockerIp, externalPort)

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.postgresql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.postgresql.url", url)
    .set("spark.sql.catalog.postgresql.pushDownTableSample", "true")
    .set("spark.sql.catalog.postgresql.pushDownLimit", "true")
    .set("spark.sql.catalog.postgresql.pushDownAggregate", "true")
    .set("spark.sql.catalog.postgresql.pushDownOffset", "true")

  override def tablePreparation(connection: Connection): Unit = {
    connection.prepareStatement(
      "CREATE TABLE employee (dept INTEGER, name VARCHAR(32), salary NUMERIC(20, 2)," +
        " bonus double precision)").executeUpdate()
    connection.prepareStatement(
      s"""CREATE TABLE pattern_testing_table (
         |pattern_testing_col VARCHAR(50)
         |)
                   """.stripMargin
    ).executeUpdate()
    // BEGIN EDGE
    connection.prepareStatement(
      "CREATE MATERIALIZED VIEW mv_employee AS SELECT * FROM employee").executeUpdate()
    // END EDGE
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType().add("ID", StringType, true, defaultMetadata())
    assert(t.schema === expectedSchema)
    // Update column type from STRING to INTEGER
    val sql1 = s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER"
    checkError(
      exception = intercept[AnalysisException] {
        sql(sql1)
      },
      condition = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"STRING\"",
        "newType" -> "\"INT\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 60)
    )
  }

  override def testCreateTableWithProperty(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INT)" +
      s" TBLPROPERTIES('TABLESPACE'='pg_default')")
    val t = spark.table(tbl)
    val expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
  }

  override def supportsTableSample: Boolean = true

  override def supportsIndex: Boolean = true

  override def indexOptions: String = "FILLFACTOR=70"

  override protected val timestampNTZType: String = "timestamp without time zone"
  override protected val timestampTZType: String = "timestamp with time zone"

  test("SPARK-42964: SQLState: 42P07 - duplicated table") {
    val t1 = s"$catalogName.t1"
    val t2 = s"$catalogName.t2"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1(c int)")
      sql(s"CREATE TABLE $t2(c int)")
      checkError(
        exception = intercept[TableAlreadyExistsException](sql(s"ALTER TABLE $t1 RENAME TO t2")),
        condition = "TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> "`t2`")
      )
    }
  }

// BEGIN EDGE
  test("SC-128095: Show tables include materialized views") {
    val tables = sql(s"SHOW TABLES in $catalogName").collect()
    assert(tables.toSet.contains(Row("", "employee", false)))
    assert(tables.toSet.contains(Row("", "mv_employee", false)))
  }
}

/**
 * Test PostgresqlTableCatalog.
 */
@DockerTest
class PostgresTableCatalogIntegrationSuite extends PostgresIntegrationSuite {
  override val catalogName: String = "postgresTableCatalog"

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.postgresTableCatalog",
      DataSource.lookupCatalogPlugin("postgresql"))
    .set("spark.sql.catalog.postgresTableCatalog.host", dockerIp)
    .set("spark.sql.catalog.postgresTableCatalog.port", externalPort.toString)
    .set("spark.sql.catalog.postgresTableCatalog.database", "postgres")
    .set("spark.sql.catalog.postgresTableCatalog.user", "postgres")
    .set("spark.sql.catalog.postgresTableCatalog.password", "rootpass")

  override def tablePreparation(connection: Connection): Unit = {
    super.tablePreparation(connection)
    connection.prepareStatement(
      s"""CREATE TABLE partition_test (id INT)
         |PARTITION BY RANGE(id)""".stripMargin).executeUpdate()
    connection.prepareStatement(
      s"""CREATE TABLE partition_test_0to1000 PARTITION OF partition_test
         |FOR VALUES FROM (0) TO (1000)""".stripMargin).executeUpdate()
    connection.prepareStatement(
      s"""CREATE TABLE partition_test_1000to2000 PARTITION OF partition_test
         |FOR VALUES FROM (1000) TO (2000)""".stripMargin).executeUpdate()
  }

  test("test partitioned table") {
    val tables = spark.sql("SHOW TABLES in postgresTableCatalog")
      .collect()
      .map(_.getString(1)).toSet
    assert(tables.contains("partition_test"))
    assert(tables.contains("partition_test_0to1000"))
    assert(tables.contains("partition_test_1000to2000"))
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType().add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType().add("ID", StringType, true, defaultMetadata())
    assert(t.schema === expectedSchema)
    // Update column type from STRING to INTEGER
    val sql1 = s"ALTER TABLE $tbl ALTER COLUMN id TYPE INTEGER"
    checkError(
      exception = intercept[AnalysisException] {
        sql(sql1)
      },
      condition = "NOT_SUPPORTED_CHANGE_COLUMN",
      parameters = Map(
        "originType" -> "\"STRING\"",
        "newType" -> "\"INT\"",
        "newName" -> "`ID`",
        "originName" -> "`ID`",
        "table" -> s"`$catalogName`.`alt_table`"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 70)
    )
  }
}
// END EDGE
