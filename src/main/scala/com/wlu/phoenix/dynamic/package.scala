package com.wlu.phoenix

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
  * Created by Wei Lu on 3/31/18.
  */
package object dynamic {
  implicit def toDynamicRDDFunctions[A <: Product](rdd: RDD[A]): DynamicColumnsRDDFunctions[A] = {
    new DynamicColumnsRDDFunctions[A](rdd)
  }

  implicit def toDynamicDfFunctions(data: DataFrame): DynamicColumnsDataFrameFunctions = {
    new DynamicColumnsDataFrameFunctions(data)
  }

}
