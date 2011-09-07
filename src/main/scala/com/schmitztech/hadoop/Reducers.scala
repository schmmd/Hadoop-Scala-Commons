package com.schmitztech.hadoop._

import collection.JavaConversions._
import com.schmitztech.hadoop._

import org.apache.hadoop.mapreduce.lib.input._
import org.apache.hadoop.mapreduce.lib.output._
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.io._

/** Common Reducers */
object Reducers {
  class IdentityReducer[K, V] extends Reducer[K, V, K, V] {
    override def reduce(key: K, 
        values: java.lang.Iterable[V], 
        context:Reducer[K,V,K,V]#Context) {
      for (v <- values) {
        context write (key, v)
      }
    }
  }

  /** Alternative to org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer */
  class IntSumReducer[T] extends Reducer[T, IntWritable, T, IntWritable] {
    override def reduce(key: T, 
        values: java.lang.Iterable[IntWritable], 
        context:Reducer[T,IntWritable,T,IntWritable]#Context) {
      val sum = values.reduceRight(_+_)
      context write (key, new IntWritable(sum))
    }
  }

  /** Alternative to org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer */
  class LongSumReducer[T] extends Reducer[T, LongWritable, T, LongWritable] {
    override def reduce(key: T, 
        values: java.lang.Iterable[LongWritable], 
        context:Reducer[T,LongWritable,T,LongWritable]#Context) {
      val sum = values.reduceRight(_+_)
      context write (key, new LongWritable(sum))
    }
  }
}
