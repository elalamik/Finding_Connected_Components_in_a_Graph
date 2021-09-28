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
import scala.math.{max, min}

// COMMAND ----------

// DBTITLE 1,Retrieving the two graphs
/* Toy graph from the paper
8 nodes and 6 edges */
val toy_graph = sc.parallelize(Array((1,2), (2,3), (3,4), (4,5), (6,7), (7,8)))

/* Google web graph from http://snap.stanford.edu/data/web-Google.html (cleaned by removing the headers)
875K nodes and 5.1M edges */
val rdd_google = sc.textFile("/FileStore/tables/web_Google_clean.txt").map(_.split("\t")).map(x => (x(0).toInt, x(1).toInt))

// COMMAND ----------

// DBTITLE 1,Implementing the CCF-Iterate and CCF-Dedup jobs + the CCF module
class CCF {
  
  // We instantiate the global NewPair counter
  var CounterNewPair = 0
  
  /* CCF-Iterate and CCF-Dedup jobs without secondary sorting */
  def iterate_and_dedup(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    val output_rdd = 
    // CCF-Iterate Map phase :
                         rdd.flatMap(x => List((x._1, x._2), (x._2, x._1))).groupByKey()
    // CCF-Iterate Reduce phase :
    /* First, since we only emit if the minimum value is smaller than the key, we filter whenever it's not the case
       Then we only keep the values that are different from the minimum */
                         .map(x => (x._1, x._2, min(x._1, x._2.min))).filter(x => x._1 != x._3).map(x => (x._1, x._2.filter(_ != x._3), x._3))
    // We update the global NewPair counter
    CounterNewPair = output_rdd.map(x => x._2.size).sum().toInt
    // We emit the union of the couples (key, min) and (value, min). The CCF-Dedup job is done by distinct()
    return output_rdd.flatMap(x => List((x._1, x._3)) ++ x._2.map(y => (y, x._3))).distinct()
  }
  
  /* CCF-Iterate and CCF-Dedup jobs with secondary sorting */
  def iterate_and_dedup_ss(rdd: RDD[(Int, Int)]): RDD[(Int, Int)] = {
    val output_rdd = 
    // CCF-Iterate Map phase :
                         rdd.flatMap(x => List((x._1, x._2), (x._2, x._1))).groupByKey()
    // CCF-Iterate Reduce phase :
    /* First, we sort the values
       Then, since we only emit if the minimum value is smaller than the key, we filter whenever it's not the case
       Finally, we only keep the values that are different from the minimum */
                         .mapValues(_.toList.sorted).filter(x => x._1 > x._2(0)).map(x => (x._1, x._2.slice(1, x._2.size), x._2(0)))
    // We update the global NewPair counter
    CounterNewPair = output_rdd.map(x => x._2.size).sum().toInt
    // We emit the union of the couples (key, min) and (value, min). The CCF-Dedup job is done by distinct()
    return output_rdd.flatMap(x => List((x._1, x._3)) ++ x._2.map(y => (y, x._3))).distinct() 
  }
  
  /* CCF module */
  def module(rdd: RDD[(Int, Int)], secondary_sorting: Boolean): (RDD[(Int, Int)], Int) = {
    var iteration = 0
    var output = rdd
    do {
      if (!(secondary_sorting)) {
        output = this.iterate_and_dedup(output) 
      } else {
        output = this.iterate_and_dedup_ss(output) 
      }
      // We update the iteration number
      iteration += 1
    } while(CounterNewPair > 0)
    return (output, iteration)
  }

}

// COMMAND ----------

// MAGIC %md
// MAGIC #### Experiments

// COMMAND ----------

val ccf = new CCF()

// COMMAND ----------

// MAGIC %md
// MAGIC ##### I) Toy Graph

// COMMAND ----------

// DBTITLE 1,1 - Without secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_1) = ccf.module(toy_graph, secondary_sorting=false)
val timing_1 = ( System.nanoTime() - start ) / 1000000000.0
println(s"Number of iterations: $nb_iterations_1")
println(s"Run-time: $timing_1 (sec) \n")

val nb_cc = output_toy.map(_._2).distinct().count()
val nodes_largest = output_toy.groupBy(_._2).map(_._2.size).max() + 1
println(s"There are $nb_cc connected components in this graph")
println(s"There are $nodes_largest nodes in the largest connected component\n")

// COMMAND ----------

// DBTITLE 1,2 - With secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_2) = ccf.module(toy_graph, secondary_sorting=true)
val timing_2 = ( System.nanoTime() - start ) / 1000000000.0
println(s"Number of iterations: $nb_iterations_2")
println(s"Run-time: $timing_2 (sec) \n")

val nb_cc = output_toy.map(_._2).distinct().count()
val nodes_largest = output_toy.groupBy(_._2).map(_._2.size).max() + 1
println(s"There are $nb_cc connected components in this graph")
println(s"There are $nodes_largest nodes in the largest connected component\n")

// COMMAND ----------

// MAGIC %md
// MAGIC ##### II) Google graph

// COMMAND ----------

// DBTITLE 1,1 - Without secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_3) = ccf.module(rdd_google, secondary_sorting=false)
val timing_3 = ( System.nanoTime() - start ) / 1000000000.0
println(s"Number of iterations: $nb_iterations_3")
println(s"Run-time: $timing_3 (sec) \n")

val nb_cc = output_toy.map(_._2).distinct().count()
val nodes_largest = output_toy.groupBy(_._2).map(_._2.size).max() + 1
println(s"There are $nb_cc connected components in this graph")
println(s"There are $nodes_largest nodes in the largest connected component\n")

// COMMAND ----------

// DBTITLE 1,2 - With secondary sorting
val start = System.nanoTime()
val (output_toy, nb_iterations_4) = ccf.module(rdd_google, secondary_sorting=true)
val timing_4 = ( System.nanoTime() - start ) / 1000000000.0
println(s"Number of iterations: $nb_iterations_4")
println(s"Run-time: $timing_4 (sec) \n")

val nb_cc = output_toy.map(_._2).distinct().count()
val nodes_largest = output_toy.groupBy(_._2).map(_._2.size).max() + 1
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
