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

package org.apache.spark.sql.execution.datasources.hbase

import org.json4s.JsonAST.JObject
import org.json4s.jackson.JsonMethods._
import org.json4s._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory

object MyAdditions {

  /** Defines, if catalog is equal to existing Hbase table
   * returns false in Hbase table doesnt exist
   */
  def isEqual(cat: String): Boolean = {

    val conf = HBaseConfiguration.create()

    val connection = ConnectionFactory.createConnection(conf)

    val jObj = parse(cat).asInstanceOf[JObject]

    val col = for {
      JObject(column) <- jObj
      JField("cf", JString(cf)) <- column
    } yield cf

    val colBytes1 = col.tail.map(e => e.getBytes)
    val colBytes: java.util.Set[Array[Byte]] =
      new java.util.HashSet[Array[Byte]]()
    for (elem <- colBytes1) colBytes.add(elem)
    val tN = compact(render(jObj \ "table" \ "name"))

    val tableName = tN.substring(1, tN.length - 1)
    val admin = connection.getAdmin
    if (admin.tableExists(TableName.valueOf(tableName))) {
      val tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName))

      if (tableDesc.getFamiliesKeys.equals(colBytes)) {

        true
      } else {

        false
      }
    } else {

      false
    }
  }

  /** Defines, if existing Hbase table contains all the column families from catalog,
   * returns false in Hbase table doesnt exist
   */
  def contains(cat: String): Boolean = {

    val conf = HBaseConfiguration.create()

    val connection = ConnectionFactory.createConnection(conf)

    val jObj = parse(cat).asInstanceOf[JObject]
    val col = for {
      JObject(column) <- jObj
      JField("cf", JString(cf)) <- column
    } yield cf

    val colBytes1 = col.tail.map(e => e.getBytes)
    val colBytes: java.util.Set[Array[Byte]] =
      new java.util.HashSet[Array[Byte]]()
    for (elem <- colBytes1) colBytes.add(elem)
    val tN = compact(render(jObj \ "table" \ "name"))

    val tableName = tN.substring(1, tN.length - 1)

    val admin = connection.getAdmin
    if (admin.tableExists(TableName.valueOf(tableName))) {
      val tableDesc = admin.getTableDescriptor(TableName.valueOf(tableName))

      if (tableDesc.getFamiliesKeys.containsAll(colBytes)) {

        true
      } else {

        false
      }
    } else {

      false
    }
  }
}
