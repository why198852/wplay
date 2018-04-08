package com.wplay.spark.streaming.core.format.avro

import com.wplay.spark.streaming.core.format.{MultipleOutputer, MultipleOutputsFormat}
import org.apache.avro.generic.GenericContainer
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.{AvroKeyOutputFormat, AvroMultipleOutputs}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.TaskInputOutputContext

/**
  *
  * Created by james on 2017/5/31.
  */
object MultipleAvroOutputsFormat {
  // This seems to be an unfortunate limitation of type inference of lambda defaults within constructor params.
  // If it would work I would just inline this function
  def amoMaker[T](io: TaskInputOutputContext[_, _, AvroKey[T], NullWritable]):
  MultipleOutputer[AvroKey[T], NullWritable] = new AvroMultipleOutputs(io)
}

class MultipleAvroOutputsFormat[T <: GenericContainer] extends MultipleOutputsFormat(
  new AvroKeyOutputFormat[T],
  (io: TaskInputOutputContext[_, _, AvroKey[T], NullWritable]) => MultipleAvroOutputsFormat.amoMaker(io)) {
}