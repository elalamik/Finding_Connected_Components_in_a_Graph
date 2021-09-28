// Databricks notebook source
// DBTITLE 0,MAP543 - Database Management
// MAGIC %md
// MAGIC # MAP543 - Database Management
// MAGIC Students: Khouloud EL ALAMI, Aya ERRAJRAJI, Ali EL ABBASSY
// MAGIC ***
// MAGIC CCF: Fast and Scalable Connected Component Computation in MapReduce
// MAGIC https://www.cse.unr.edu/~hkardes/pdfs/ccf.pdf

// COMMAND ----------

// DBTITLE 1,Imports
import org.apache.spark.sql.functions.{split, desc, collect_list, col, sort_array, array_min, coalesce, size, sum, explode}
import sqlContext.implicits._
import org.apache.spark.sql.DataFrame

// COMMAND ----------

// DBTITLE 1,Spark Configuration
spark.conf.set("spark.sql.shuffle.partitions", 50) //used to deal with memory congestion when using shuffling functions such as aggregations and union
spark.conf.set("spark.sql.inMemoryColumnarStorage.compressed", true) // to enable data compression in the memory
spark.conf.set("spark.sql.inMemoryColumnarStorage.batchSize", 10000) // large batch sizes can improve memory utilization and compression 
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", true) // to enable coalescing data into fewer partitions
spark.sparkContext.setLogLevel("OFF") //to turn off info logs 

// COMMAND ----------

// DBTITLE 1,Retrieving the two graphs
/* Toy graph from the paper
8 nodes and 6 edges */
val toy_graph = sc.parallelize(Array(("A", "B"), ("B", "C"), ("B", "D"), ("D","E"), ("F", "G"), ("G", "H"))).toDF("key", "value").cache()

/* Google web graph from http://snap.stanford.edu/data/web-Google.html (cleaned by removing the headers)
875K nodes and 5.1M edges */
val google = sc.textFile("/FileStore/tables/web_Google_clean.txt").map(_.split("\t")).map(x => (x(0).toInt, x(1).toInt)).toDF("key", "value").cache()

// COMMAND ----------

// DBTITLE 1,Implementing the CCF-Iterate and CCF-Dedup jobs + the CCF module
def CCF(df: DataFrame, secondary_sorting: Boolean) : (DataFrame, Int) = {
  
  var iteration = 0 
  var CounterNewPair = 0 // We instantiate the global NewPair counter
  var ccf_iterate_map = df 
  
  do {
    /* CCF-Iterate and CCF-Dedup jobs without secondary sorting */
    if (!(secondary_sorting)) {
      // CCF-Iterate Map phase :
      ccf_iterate_map = ccf_iterate_map.union(ccf_iterate_map.select("value", "key")).coalesce(5)
      // CCF-Iterate Reduce phase :
      /* First, since we only emit if the minimum value is smaller than the key, we filter whenever it's not the case
       Then we only keep the values that are different from the minimum */
      var ccf_iterate_reduce = ccf_iterate_map.groupBy("key").agg(collect_list("value").alias("value")).withColumn("min_val", coalesce(array_min(col("value")))).filter(col("min_val")<col("key")).cache()
      // We update the global NewPair counter
      CounterNewPair = ccf_iterate_reduce.withColumn("CounterNewPair", size(col("value"))-1).agg(sum("CounterNewPair")).collect()(0).getLong(0).toInt
      // We emit the union of the couples (key, min) and (value, min). The CCF-Dedup job is done by distinct()
      var emit1 = ccf_iterate_reduce.select(col("key"), col("min_val").alias("value"))
      var emit2 = ccf_iterate_reduce.select(explode(col("value")).alias("value"), col("min_val")).filter(col("value") =!= col("min_val"))
      ccf_iterate_map = emit1.union(emit2).distinct()
    }
    
    /* CCF-Iterate and CCF-Dedup jobs with secondary sorting */
    else {
      // CCF-Iterate Map phase :
      ccf_iterate_map = ccf_iterate_map.union(ccf_iterate_map.select("value", "key")).coalesce(5)
      // CCF-Iterate Reduce phase :
      /* First, we sort the values
       Then, since we only emit if the minimum value is smaller than the key, we filter whenever it's not the case
       Finally, we only keep the values that are different from the minimum */
      var ccf_iterate_reduce = ccf_iterate_map.groupBy("key").agg(collect_list("value").alias("value")).cache()
      ccf_iterate_reduce = ccf_iterate_reduce.select(col("key"), sort_array(col("value")).alias("value")).withColumn("min_val", col("value").getItem(0)).filter(col("min_val")<col("key"))
      // We update the global NewPair counter
      CounterNewPair = ccf_iterate_reduce.withColumn("CounterNewPair", size(col("value"))-1).agg(sum("CounterNewPair")).collect()(0).getLong(0).toInt
      // We emit the union of the couples (key, min) and (value, min). The CCF-Dedup job is done by distinct()
      var emit1 = ccf_iterate_reduce.select(col("key"), col("min_val").alias("value"))
      var emit2 = ccf_iterate_reduce.select(explode(col("value")).alias("value"), col("min_val")).filter(col("value") =!= col("min_val"))
      ccf_iterate_map = emit1.union(emit2).distinct() 
    }
    
    // We update the iteration number
    iteration += 1
  } while(CounterNewPair > 0) 
  return (ccf_iterate_map, iteration)
}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Experiments

// COMMAND ----------

// MAGIC %md
// MAGIC ##### I) Toy Graph

// COMMAND ----------

// DBTITLE 1,1 - Without secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_1) = CCF(toy_graph, secondary_sorting=false)
val timing_1 = ( System.nanoTime() - start ) / 1000000000.0
println(s"The total number of iterations is: $nb_iterations_1")
println(s"Run-time: $timing_1 (sec) \n")

var nb_cc = output_toy.select("value").distinct().count().toInt
var nodes_largest = output_toy.groupBy("value").count().sort(desc("count")).collect()(0).getLong(1).toInt+1
println(s"There are $nb_cc connected components in this graph")
println(s"There are $nodes_largest nodes in the largest connected component\n")

// COMMAND ----------

// DBTITLE 1,2 - With secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_2) = CCF(toy_graph, secondary_sorting=true)
val timing_2 = ( System.nanoTime() - start ) / 1000000000.0
println(s"The total number of iterations is: $nb_iterations_2")
println(s"Run-time: $timing_2 (sec) \n")

var nb_cc = output_toy.select("value").distinct().count().toInt
var nodes_largest = output_toy.groupBy("value").count().sort(desc("count")).collect()(0).getLong(1).toInt+1
println(s"There are $nb_cc connected components in this graph")
println(s"There are $nodes_largest nodes in the largest connected component\n")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### II) Google Graph

// COMMAND ----------

// DBTITLE 1,1 - Without secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_3) = CCF(google, secondary_sorting=false)
val timing_3 = ( System.nanoTime() - start ) / 1000000000.0
println(s"The total number of iterations is: $nb_iterations_3")
println(s"Run-time: $timing_3 (sec) \n")

var nb_cc = output_toy.select("value").distinct().count().toInt
var nodes_largest = output_toy.groupBy("value").count().sort(desc("count")).collect()(0).getLong(1).toInt+1
println(s"There are $nb_cc connected components in this graph")
println(s"There are $nodes_largest nodes in the largest connected component\n")

// COMMAND ----------

// DBTITLE 1,2 - With secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_4) = CCF(google, secondary_sorting=true)
val timing_4 = ( System.nanoTime() - start ) / 1000000000.0
println(s"The total number of iterations is: $nb_iterations_4")
println(s"Run-time: $timing_4 (sec) \n")

var nb_cc = output_toy.select("value").distinct().count().toInt
var nodes_largest = output_toy.groupBy("value").count().sort(desc("count")).collect()(0).getLong(1).toInt+1
println(s"There are $nb_cc connected components in this graph")
println(s"There are $nodes_largest nodes in the largest connected component\n")

// COMMAND ----------

// MAGIC %md
// MAGIC #### Results

// COMMAND ----------

// DBTITLE 1,Toy Graph - Performance Comparison 
var perf_toy = sc.parallelize(Array(("CCF w/o sec. sorting", nb_iterations_1, timing_1), ("CCF w/ sec. sorting", nb_iterations_2, timing_2))).toDF("CCF", "Number of iterations", "Run-time (sec)")
perf_toy.show()

// COMMAND ----------

// DBTITLE 1,Google Graph - Performance Comparison 
var perf_google = sc.parallelize(Array(("CCF w/o sec. sorting", nb_iterations_3, timing_3), ("CCF w/ sec. sorting", nb_iterations_4, timing_4))).toDF("CCF", "Number of iterations", "Run-time (sec)")
perf_google.show()
