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

import org.apache.spark.{SparkConf, SparkSQLException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.datasources.v2.jdbc.JDBCTableCatalog
import org.apache.spark.sql.jdbc.DatabaseOnDocker
import org.apache.spark.sql.types._
import org.apache.spark.tags.DockerTest

/**
 * To run this test suite for a specific version (e.g., postgres:17.0-alpine)
 * {{{
 *   ENABLE_DOCKER_INTEGRATION_TESTS=1 POSTGRES_DOCKER_IMAGE_NAME=postgres:17.0-alpine
 *     ./build/sbt -Pdocker-integration-tests "testOnly *v2.PostgresIntegrationSuite"
 * }}}
 */
@DockerTest
class PostgresIntegrationSuite extends DockerJDBCIntegrationV2Suite with V2JDBCTest {
  override val catalogName: String = "postgresql"
  override val db = new DatabaseOnDocker {
    override val imageName = sys.env.getOrElse("POSTGRES_DOCKER_IMAGE_NAME", "postgres:17.0-alpine")
    override val env = Map(
      "POSTGRES_PASSWORD" -> "rootpass"
    )
    override val usesIpc = false
    override val jdbcPort = 5432
    override def getJdbcUrl(ip: String, port: Int): String =
      s"jdbc:postgresql://$ip:$port/postgres?user=postgres&password=rootpass"
  }
  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.postgresql", classOf[JDBCTableCatalog].getName)
    .set("spark.sql.catalog.postgresql.url", db.getJdbcUrl(dockerIp, externalPort))
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

    connection.prepareStatement("CREATE TABLE array_test_table (int_array int[]," +
      "float_array FLOAT8[], timestamp_array TIMESTAMP[], string_array TEXT[]," +
      "datetime_array TIMESTAMPTZ[], array_of_int_arrays INT[][])").executeUpdate()

    val query =
      """
        INSERT INTO array_test_table (int_array, float_array, timestamp_array, string_array, datetime_array, array_of_int_arrays)
        VALUES
        (
            ARRAY[1, 2, 3],                       -- Array of integers
            ARRAY[1.1, 2.2, 3.3],                 -- Array of floats
            ARRAY['2023-01-01 12:00'::timestamp, '2023-06-01 08:30'::timestamp],  -- Array of timestamps
            ARRAY['hello', 'world'],              -- Array of strings
            ARRAY['2023-10-04 12:00:00+00'::timestamptz, '2023-12-01 14:15:00+00'::timestamptz], -- Array of datetimes with time zone
            ARRAY[ARRAY[1, 2]]    -- Array of arrays of integers
        ),
        (
            ARRAY[10, 20, 30],                    -- Another set of data
            ARRAY[10.5, 20.5, 30.5],
            ARRAY['2022-01-01 09:15'::timestamp, '2022-03-15 07:45'::timestamp],
            ARRAY['postgres', 'arrays'],
            ARRAY['2022-11-22 09:00:00+00'::timestamptz, '2022-12-31 23:59:59+00'::timestamptz],
            ARRAY[ARRAY[10, 20]]
        );
      """
    connection.prepareStatement(query).executeUpdate()

    connection.prepareStatement("CREATE TABLE array_int (col int[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_bigint(col bigint[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_smallint (col smallint[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_boolean (col boolean[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_float (col real[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_double (col float8[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_text (col text[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_timestamp (col timestamp[])").executeUpdate()
    connection.prepareStatement("CREATE TABLE array_timestamptz (col timestamptz[])")
      .executeUpdate()

    connection.prepareStatement("INSERT INTO array_int VALUES (array[array[10]])").executeUpdate()
    connection.prepareStatement("INSERT INTO array_bigint VALUES (array[array[10]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_smallint VALUES (array[array[10]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_boolean VALUES (array[array[true]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_float VALUES (array[array[10.5]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_double VALUES (array[array[10.1]])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_text VALUES (array[array['helo world']])")
      .executeUpdate()
    connection.prepareStatement("INSERT INTO array_timestamp VALUES (" +
      "array[array['2022-01-01 09:15'::timestamp]])").executeUpdate()
    connection.prepareStatement("INSERT INTO array_timestamptz VALUES " +
      "(array[array['2022-01-01 09:15'::timestamptz]])").executeUpdate()
  }

  test("Test multi-dimensional column types") {
    val df = spark.read.format("jdbc")
      .option("url", jdbcUrl)
      .option("dbtable", "array_test_table")
      .load()
    df.collect()


    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_int")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_bigint")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_smallint")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_boolean")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_float")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_double")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_timestamp")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_timestamptz")
        .load()
      df.collect()
    }

    intercept[SparkSQLException] {
      val df = spark.read.format("jdbc")
        .option("url", jdbcUrl)
        .option("dbtable", "array_text")
        .load()
      df.collect()
    }
  }

  override def testUpdateColumnType(tbl: String): Unit = {
    sql(s"CREATE TABLE $tbl (ID INTEGER)")
    var t = spark.table(tbl)
    var expectedSchema = new StructType()
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
    sql(s"ALTER TABLE $tbl ALTER COLUMN id TYPE STRING")
    t = spark.table(tbl)
    expectedSchema = new StructType()
      .add("ID", StringType, true, defaultMetadata())
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
    val expectedSchema = new StructType()
      .add("ID", IntegerType, true, defaultMetadata(IntegerType))
    assert(t.schema === expectedSchema)
  }

  override def supportsTableSample: Boolean = true

  override def supportsIndex: Boolean = true

  override def indexOptions: String = "FILLFACTOR=70"

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
}
