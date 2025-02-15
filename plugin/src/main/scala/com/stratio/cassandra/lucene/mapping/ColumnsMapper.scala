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

import java.nio.ByteBuffer

import com.stratio.cassandra.lucene.column.{Column, Columns}
import com.stratio.cassandra.lucene.schema.Schema
import com.stratio.cassandra.lucene.util.Logging
import org.apache.cassandra.db.marshal._
import org.apache.cassandra.db.rows.{Cell, ComplexColumnData, Row}
import org.apache.cassandra.db.{Clustering, DecoratedKey}
import org.apache.cassandra.schema.{ColumnMetadata, TableMetadata}
import org.apache.cassandra.serializers.{CollectionSerializer, MapSerializer}
import org.apache.cassandra.transport.ProtocolVersion
import org.apache.cassandra.transport.ProtocolVersion.CURRENT
import org.apache.cassandra.utils.ByteBufferUtil

import scala.jdk.CollectionConverters._

/** Maps Cassandra rows to [[Columns]].
  *
  * @param schema   a schema
  * @param metadata a table metadata
  * @author Andres de la Pena `adelapena@stratio.com`
  */
class ColumnsMapper(schema: Schema, metadata: TableMetadata) extends Logging {

  val mappedCells: Set[String] = schema.mappedCells().asScala.toSet

  val keyColumns: List[ColumnMetadata] = metadata.partitionKeyColumns.asScala
    .filter(definition => mappedCells.contains(definition.name.toString)).toList

  val clusteringColumns: List[ColumnMetadata] = metadata.clusteringColumns.asScala
    .filter(definition => mappedCells.contains(definition.name.toString)).toList

  /** Returns the mapped, not deleted at the specified time in seconds and not null [[Columns]]
    * contained in the specified row.
    *
    * @param key the partition key
    * @param row the row
    * @param now now in seconds
    */
  def columns(key: DecoratedKey, row: Row, now: Int): Columns = {
    columns(key) ++ columns(row.clustering()) ++ columns(row, now)
  }

  /** Returns the mapped [[Columns]] contained in the specified partition key. */
  private[mapping] def columns(key: DecoratedKey): Columns = {
    val components = metadata.partitionKeyType match {
      case c: CompositeType => c.split(key.getKey)
      case _ => Array[ByteBuffer](key.getKey)
    }
    (keyColumns :\ Columns()) ((definition, columns) => {
      val name = definition.name.toString
      val value = components(definition.position)
      val valueType = definition.cellValueType
      Column(name).withValue(value, valueType) :: columns
    })
  }

  /** Returns the mapped [[Columns]] contained in the specified clustering key. */
  private[mapping] def columns(clustering: Clustering[_]): Columns = {
    (clusteringColumns foldRight  Columns()) ((definition, columns_s) => {
      columns_s ++ ColumnsMapper.columns(Column(definition.name.toString),
                                         definition.`type`,
                                         clustering.getBufferArray()(definition.position()))
    })
  }

  /** Returns the mapped, not deleted at the specified time in seconds and not null [[Columns]]
    * contained in the regular columns of the specified row.
    *
    * @param row a row
    * @param now now in seconds
    */
  private[mapping] def columns(row: Row, now: Int): Columns = {
    (row.columns.asScala :\ Columns()) ((definition, columns) =>
                                          if (definition.isComplex) {
                                            this.columns(row.getComplexColumnData(definition), now) ++ columns
                                          } else {
                                            this.columns(row.getCell(definition).asInstanceOf[Cell[ByteBuffer]], now) ++ columns
                                          }
                                        )
  }

  /** Returns the mapped, not deleted at the specified time in seconds and not null [[Columns]]
    * contained in the specified complex column data.
    *
    * @param complexColumnData a complex column data
    * @param now               now in seconds
    */
  private[mapping] def columns(complexColumnData: ComplexColumnData, now: Int): Columns = {
    (complexColumnData.asScala :\ Columns()) ((cell, columns) => {
      this.columns(cell.asInstanceOf[Cell[ByteBuffer]], now) ++ columns
    })
  }

  /** Returns the mapped, not deleted at the specified time in seconds and not null [[Columns]]
    * contained in the specified cell.
    *
    * @param cell a cell
    * @param now  now in seconds
    */
  private[mapping] def columns(cell: Cell[ByteBuffer], now: Int): Columns =
    if (cell.isTombstone
      || cell.localDeletionTime <= now
      || !mappedCells.contains(cell.column.name.toString))
      Columns.empty()
    else ColumnsMapper.columns(cell)

}

/** Companion object for [[ColumnsMapper]]. */
object ColumnsMapper {

  /** Returns [[Columns]] contained in the specified cell.
    *
    * @param cell a cell
    */
  private[mapping] def columns(cell: Cell[ByteBuffer]): Columns = {
    if (cell == null) return Columns()
    val name = cell.column.name.toString
    val comparator = cell.column.`type`

    val column = Column(name)
    comparator match {
      case setType: SetType[_] if !setType.isFrozenCollection =>
        val itemComparator = setType.nameComparator
        val itemValue = cell.path.get(0)
        columns(column, itemComparator, itemValue)
      case listType: ListType[_] if !listType.isFrozenCollection =>
        val itemComparator = listType.valueComparator()
        columns(column, itemComparator, cell.buffer())
      case mapType: MapType[_, _] if !mapType.isFrozenCollection =>
        val valueType = mapType.valueComparator
        val keyValue = cell.path.get(0)
        val keyType = mapType.nameComparator
        val keyName = keyType.compose(keyValue).toString
        val keyColumn = column.withUDTName(Column.MAP_KEY_SUFFIX).withValue(keyValue, keyType)
        val buffer = cell.buffer()
        val valueColumn = columns(column.withUDTName(Column.MAP_VALUE_SUFFIX), valueType, buffer)
        val entryColumn = columns(column.withMapName(keyName), valueType, buffer)
        keyColumn + valueColumn ++ entryColumn
      case userType: UserType =>
        val cellPath = cell.path
        if (cellPath == null) {
          columns(column, comparator, cell.buffer())
        } else {
          val position = ByteBufferUtil.toShort(cellPath.get(0))
          val name = userType.fieldNameAsString(position)
          val typo = userType.`type`(position)
          columns(column.withUDTName(name), typo, cellPath.get(0))
        }
      case _ =>
        columns(column, comparator, cell.buffer())
    }
  }

  private[mapping] def columns(column: Column, serializer: AbstractType[_], value: ByteBuffer)
  : Columns = serializer match {
    case t: SetType[_] => columns(column, t, value)
    case t: ListType[_] => columns(column, t, value)
    case t: MapType[_, _] => columns(column, t, value)
    case t: UserType => columns(column, t, value)
    case t: TupleType => columns(column, t, value)
    case _ => Columns(column.withValue(value, serializer))
  }

  private[mapping] def columns(column: Column, set: SetType[_], value: ByteBuffer): Columns = {
    val nameType = set.nameComparator()
    val bb = ByteBufferUtil.clone(value)

    val n: Int = CollectionSerializer.readCollectionSize(value, CURRENT)
    var offset: Int = CollectionSerializer.sizeOfCollectionSize(n, CURRENT)

    ((0 until n) foldRight  Columns()) ((_, columns) => {
      val databb = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset, CURRENT)
      offset += CollectionSerializer.sizeOfValue(databb, ByteBufferAccessor.instance, CURRENT)
      this.columns(column, nameType, databb) ++ columns
    })
  }

  private[mapping] def columns(column: Column, list: ListType[_], value: ByteBuffer): Columns = {
    val valueType = list.valueComparator()
    val n: Int = CollectionSerializer.readCollectionSize(value, ProtocolVersion.V3)
    var offset: Int = CollectionSerializer.sizeOfCollectionSize(n, ProtocolVersion.V3)

    ((0 until n) foldRight  Columns()) ((_, columns) => {
      val databb = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset, CURRENT)
      offset += CollectionSerializer.sizeOfValue(databb, ByteBufferAccessor.instance, CURRENT)
      this.columns(column, valueType, databb) ++ columns
    })
  }

  private[mapping] def columns(column: Column, map: MapType[_, _], value: ByteBuffer): Columns = {
    val itemKeysType = map.nameComparator
    val itemValuesType = map.valueComparator

    val n: Int = CollectionSerializer.readCollectionSize(value, ProtocolVersion.V3)
    var offset: Int = CollectionSerializer.sizeOfCollectionSize(n, ProtocolVersion.V3)

    ((0 until n) :\  Columns()) ((_, columns) => {
      val itemKey = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset, CURRENT)
      offset += CollectionSerializer.sizeOfValue(itemKey, ByteBufferAccessor.instance, CURRENT)
      val itemValue = CollectionSerializer.readValue(value, ByteBufferAccessor.instance, offset, CURRENT)
      offset += CollectionSerializer.sizeOfValue(itemValue, ByteBufferAccessor.instance, CURRENT)
      val itemName = itemKeysType.compose(itemKey).toString
      val keyColumn = column.withUDTName(Column.MAP_KEY_SUFFIX).withValue(itemKey, itemKeysType)
      val valueColumn = this.columns(column.withUDTName(Column.MAP_VALUE_SUFFIX), itemValuesType, itemValue)
      val entryColumn = this.columns(column.withMapName(itemName), itemValuesType, itemValue)
      keyColumn + valueColumn ++ entryColumn ++ columns
    })
  }

  private[mapping] def columns(column: Column, udt: UserType, value: ByteBuffer): Columns = {
    val itemValues = udt.split(value)
    ((0 until udt.fieldNames.size) foldRight Columns()) ((i, columns) => {
      val itemValue = itemValues(i)
      if (itemValue == null) {
        columns
      } else {
        val itemName = udt.fieldNameAsString(i)
        val itemType = udt.fieldType(i)
        val itemColumn = column.withUDTName(itemName)
        this.columns(itemColumn, itemType, itemValue) ++ columns
      }
    })
  }

  private[mapping] def columns(column: Column, tuple: TupleType, value: ByteBuffer): Columns = {
    val itemValues = tuple.split(value)
    ((0 until tuple.size) foldRight  Columns()) ((i, columns) => {
      val itemValue = itemValues(i)
      if (itemValue == null) {
        columns
      } else {
        val itemName = i.toString
        val itemType = tuple.`type`(i)
        val itemColumn = column.withUDTName(itemName)
        this.columns(itemColumn, itemType, itemValue) ++ columns
      }
    })
  }
}