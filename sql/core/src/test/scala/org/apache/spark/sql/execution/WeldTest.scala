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

package org.apache.spark.sql.execution

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel

object WeldTest {
  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf()
    sparkConf.set("spark.executor.heartbeatInterval", "6000s")
    sparkConf.set("spark.network.timeout", "12000s")
    sparkConf.set("spark.master", "local[4]")
    sparkConf.set("spark.app.name", "WeldTest")
    // scalastyle:off
    val sqlContext = SQLContext.getOrCreate(new SparkContext(sparkConf))

    val start = System.currentTimeMillis()
    val input = sqlContext.read.parquet("/Users/shimingfei/IntellijProjects/temp/lineitem")
      .persist(StorageLevel.DISK_ONLY)
//    println(s"input size: ${input.count()}")
    input.registerTempTable("lineitem")

    sqlContext.sql(tpch_q6).foreach(_=>Unit)
    val end = System.currentTimeMillis()
    println(s"execution cost: ${end - start}")
    Thread.sleep(1000 * 1000)
  }

  val tpch_q1 =
    """
      |select
      |	l_returnflag,
      |	l_linestatus,
      |	sum(l_quantity) as sum_qty,
      |	sum(l_extendedprice) as sum_base_price,
      |	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
      |	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
      |	avg(l_quantity) as avg_qty,
      |	avg(l_extendedprice) as avg_price,
      |	avg(l_discount) as avg_disc,
      |	count(*) as count_order
      |from
      |	lineitem
      |where
      |	l_shipdate <= date '1998-12-01' - interval '120' day
      |group by
      |	l_returnflag,
      |	l_linestatus
      |order by
      |	l_returnflag,
      |	l_linestatus
    """.stripMargin

  val tpch_q6 =
    """
      |select
      |	sum(l_extendedprice * l_discount) as revenue
      |from
      |	lineitem
      |where
      |	l_shipdate >= date '1995-01-01'
      |	and l_shipdate < date '1995-01-01' + interval '1' year
      |	and l_discount between 0.04 - 0.01 and 0.04 + 0.01
      |	and l_quantity < 24
    """.stripMargin
  // scalastyle:on

}
