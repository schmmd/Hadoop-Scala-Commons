/* This file is derived from Jonhnny Weslley's SHadoop project
 * which is hosted at http://code.google.com/p/jweslley/.  A
 * number of the implicits were removed because they were 
 * from the deprecated Hadoop API.
 */

package com.schmitztech.hadoop

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{BooleanWritable, IntWritable, LongWritable, FloatWritable, Text}

/** Implicits for handling Hadoop objects conveniently.  Note that this 
  * should be use in conjunction with scala.collection.JavaConversions.]
  * In other words, in your project, use the following two imports.
  *
  *   import scala.collection.JavaConversions._
  *   import com.schmitztech.hadoop._
  *
  * Then you can, for example, handle java.lang.Iterable[Text] as 
  * scala.collection.Iterable[String]. */
object Implicits {

  implicit def writable2boolean(value: BooleanWritable) = value.get
  implicit def boolean2writable(value: Boolean) = new BooleanWritable(value)

  implicit def writable2int(value: IntWritable) = value.get
  implicit def int2writable(value: Int) = new IntWritable(value)

  implicit def writable2long(value: LongWritable) = value.get
  implicit def long2writable(value: Long) = new LongWritable(value)

  implicit def writable2float(value: FloatWritable) = value.get
  implicit def float2writable(value: Float) = new FloatWritable(value)

  implicit def text2string(value: Text) = value.toString
  implicit def string2text(value: String) = new Text(value)

  implicit def path2string(value: Path) = value.toString
  implicit def string2path(value: String) = new Path(value)
}
