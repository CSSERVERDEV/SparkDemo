package com.spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

abstract class  StringCount extends UserDefinedAggregateFunction{
  def inputSchema:StructType={StructType{(Array(StructField("name",StringType,true)))}}

  override def bufferSchema:StructType={StructType{(Array(StructField("count",IntegerType,true)))}}
  def dateministic:Boolean={true}
  override def initialize(buffer:MutableAggregationBuffer):Unit={
    buffer(0)=0
  }
  override def update(buffer:MutableAggregationBuffer,input:Row):Unit={
    buffer(0)=buffer.getAs[Int](0)+1
  }
  override def merge(buffer1:MutableAggregationBuffer,buffer2:Row):Unit={
    buffer1(0)=buffer1.getAs[Int](0)+buffer2.getAs[Int](0)
  }
  override def dataType:DataType={
    IntegerType
  }

  /**
    * 最后返回一个最终的聚合值，要和dataType的类型一一对应
    * @param buffer
    * @return
    */
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }
}
