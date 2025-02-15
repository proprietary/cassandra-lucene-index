/*
 * Copyright (C) 2014 Stratio (http://stratio.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.stratio.cassandra.lucene.mapping

import java.math.{BigDecimal, BigInteger}
import java.nio.ByteBuffer
import java.util.{Date, UUID}
import com.google.common.collect.Lists
import com.stratio.cassandra.lucene.BaseScalaTest
import com.stratio.cassandra.lucene.BaseScalaTest._
import com.stratio.cassandra.lucene.column.{Column, Columns}
import com.stratio.cassandra.lucene.mapping.ColumnsMapper._
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.db.rows.{BufferCell, Cell}
import org.apache.cassandra.schema.ColumnMetadata
import org.apache.cassandra.utils.{TimeUUID, UUIDGen}
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner

import scala.jdk.CollectionConverters._

/** Tests for [[ColumnsMapper]].
  *
  * @author Andres de la Pena `adelapena@stratio.com`
  */
@RunWith(classOf[JUnitRunner])
class ColumnsMapperTest extends BaseScalaTest {

  test("columns from plain cells") {
    def test[A](abstractType: AbstractType[_], value: ByteBuffer) = {
      val column = Column("cell")
      ColumnsMapper.columns(column, abstractType, value) shouldBe
        Columns(column.withValue(abstractType.getSerializer.deserialize(value)))
    }

    test(ascii, ascii.getSerializer.serialize("Ab"))
    test(utf8, utf8.getSerializer.serialize("Ab"))
    test(int32, int32.getSerializer.serialize(7.asInstanceOf[Integer]))
    test(float, float.getSerializer.serialize(7.3f.asInstanceOf[java.lang.Float]))
    test(long, long.getSerializer.serialize(7l.asInstanceOf[java.lang.Long]))
    test(double, double.getSerializer.serialize(7.3d.asInstanceOf[java.lang.Double]))
    test(integer, integer.getSerializer.serialize(new BigInteger("7")))
    test(decimal, decimal.getSerializer.serialize(new BigDecimal("7.3")))
    test(uuid, uuid.getSerializer.serialize(UUID.randomUUID))
    test(lexicalUuid, lexicalUuid.getSerializer.serialize(UUID.randomUUID))
    test(timeUuid, TimeUUID.Generator.nextTimeUUID().toBytes)
    test(timestamp, timestamp.getSerializer.serialize(new Date))
    test(boolean, boolean.getSerializer.serialize(true.asInstanceOf[java.lang.Boolean]))
  }

  test("columns from frozen set") {
    val column = Column("cell")
    val `type` = set(utf8, multiCell = true)
    val bb = `type`.getSerializer.serialize(Set("a", "b").asJava)
    columns(column, `type`, bb) shouldBe Columns(column.withValue("b"), column.withValue("a"))
  }

  test("columns from frozen list") {
    val column = Column("cell")
    val `type` = list(utf8, multiCell = false)
    val bb = `type`.decompose(List("a", "b").asJava)
    columns(column, `type`, bb) shouldBe Columns(column.withValue("b"), column.withValue("a"))
  }

  test("columns from list of lists") {
    val column = Column("cell")
    val `type` = list(list(utf8, multiCell = false), multiCell = false)
    val bb = `type`.serializer.serialize(List(List("a", "b").asJava, List("c", "d").asJava).asJava)

  }

  test("columns from list of sets") {
    val column = Column("cell")
    val `type` = list(set(utf8, multiCell = true), multiCell = false)
    val bb = `type`.getSerializer.serialize(List(Set("a", "b").asJava, Set("c", "d").asJava).asJava)
    val cols: Columns = columns(column, `type`, bb)

    cols.toSet.contains(column.withValue("a")) shouldBe true
    cols.toSet.contains(column.withValue("b")) shouldBe true
    cols.toSet.contains(column.withValue("c")) shouldBe true
    cols.toSet.contains(column.withValue("d")) shouldBe true
  }

  test("columns from set of sets") {
    val column = Column("cell")
    val `type` = set(set(utf8, multiCell = true), multiCell = true)
    val bb = `type`.getSerializer.serialize(Set(Set("a", "b").asJava, Set("c", "d").asJava).asJava)

    val cols: Columns = columns(column, `type`, bb)
    cols.toSet.contains(column.withValue("a")) shouldBe true
    cols.toSet.contains(column.withValue("b")) shouldBe true
    cols.toSet.contains(column.withValue("c")) shouldBe true
    cols.toSet.contains(column.withValue("d")) shouldBe true
  }

  test("columns from set of lists") {
    val column = Column("cell")
    val `type` = set(list(utf8, multiCell = false), multiCell = true)
    val bb = `type`.getSerializer.serialize(Set(List("a", "b").asJava, List("c", "d").asJava).asJava)

    val cols: Columns = columns(column, `type`, bb)
    cols.toSet.contains(column.withValue("a")) shouldBe true
    cols.toSet.contains(column.withValue("b")) shouldBe true
    cols.toSet.contains(column.withValue("c")) shouldBe true
    cols.toSet.contains(column.withValue("d")) shouldBe true
  }

  test("columns from tuple") {
    val column = Column("cell")
    val `type` = new TupleType(Lists.newArrayList(utf8, utf8))
    val bb = TupleType.buildValue(Array(utf8.decompose("a"), utf8.decompose("b")))
    columns(column, `type`, bb) shouldBe
      Columns(column.withUDTName("0").withValue("a"), column.withUDTName("1").withValue("b"))
  }

  test("columns from frozen map") {
    val column = Column("cell")
    val `type` = map(utf8, utf8, multiCell = true)
    val bb = `type`.decompose(Map("k1" -> "v1", "k2" -> "v2").asJava)

    val result = columns(column, `type`, bb)

    result shouldBe
      Columns(
        column.withUDTName(Column.MAP_KEY_SUFFIX).withValue("k2"),
        column.withUDTName(Column.MAP_VALUE_SUFFIX).withValue("v2"),
        column.withMapName("k2").withValue("v2"),
        column.withUDTName(Column.MAP_KEY_SUFFIX).withValue("k1"),
        column.withUDTName(Column.MAP_VALUE_SUFFIX).withValue("v1"),
        column.withMapName("k1").withValue("v1"))
  }

  test("columns from list of maps") {
    val column = Column("cell")
    val `type` = list(map(utf8, utf8, multiCell = true), multiCell = false)
    val bb = `type`.decompose(List(Map[String,String]("k1" -> "v1", "k2" -> "v2").asJava).asJava)

    val result = columns(column, `type`, bb)

    result shouldBe
      Columns(
        column.withUDTName(Column.MAP_KEY_SUFFIX).withValue("k2"),
        column.withUDTName(Column.MAP_VALUE_SUFFIX).withValue("v2"),
        column.withMapName("k2").withValue("v2"),
        column.withUDTName(Column.MAP_KEY_SUFFIX).withValue("k1"),
        column.withUDTName(Column.MAP_VALUE_SUFFIX).withValue("v1"),
        column.withMapName("k1").withValue("v1"))
  }

  test("columns from set of maps") {
    val column = Column("cell")
    val `type` = set(map(utf8, utf8, multiCell = true), multiCell = false)
    val bb = `type`.decompose(Set(Map[String,String]("k1" -> "v1", "k2" -> "v2").asJava).asJava)

    val result = columns(column, `type`, bb)

    result shouldBe
      Columns(
        column.withUDTName(Column.MAP_KEY_SUFFIX).withValue("k2"),
        column.withUDTName(Column.MAP_VALUE_SUFFIX).withValue("v2"),
        column.withMapName("k2").withValue("v2"),
        column.withUDTName(Column.MAP_KEY_SUFFIX).withValue("k1"),
        column.withUDTName(Column.MAP_VALUE_SUFFIX).withValue("v1"),
        column.withMapName("k1").withValue("v1"))
  }
  test("columns from UDT") {
    val column = Column("cell")
    val `type` = udt(List("a", "b"), List(utf8, utf8))
    val bb = TupleType.buildValue(Array(utf8.decompose("1"), utf8.decompose("2")))

    columns(column, `type`, bb) shouldBe
      Columns(column.withUDTName("a").withValue("1"), column.withUDTName("b").withValue("2"))
  }

  test("columns from regular cell") {
    val columnDefinition = ColumnMetadata.regularColumn("ks", "cf", "cell", utf8)
    val cell = new BufferCell(
      columnDefinition,
      System.currentTimeMillis(),
      Cell.NO_TTL,
      Cell.NO_DELETION_TIME,
      utf8.decompose("a"),
      null)
    columns(cell) shouldBe Columns(Column("cell").withValue("a"))
  }
}