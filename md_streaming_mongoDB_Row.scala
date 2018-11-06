import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import _root_.kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.hive.HiveContext
import java.util.Calendar
import org.apache.phoenix.spark._
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

import org.apache.spark._
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.hadoop.hbase.{HBaseConfiguration, HTableDescriptor}
import org.apache.hadoop.hbase.client.HBaseAdmin
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.fs.Path
import scala.util.Random
import scala.math._
import org.apache.spark.sql.functions._
//
import com.mongodb.spark._
import org.bson.Document
import com.mongodb.spark.config._
//
import org.apache.log4j.Logger
import org.apache.log4j.Level


//
object md_streaming_mongoDB_Row
{
  private var zookeeperUrl = "rhes75:2181"
  private var requestConsumerId = null
  private var impressionConsumerId = null
  private var clickConsumerId = null
  private var conversionConsumerId = null
  private var requestTopicName = null
  private var impressionTopicName = null
  private var clickTopicName = null
  private var conversionTopicName = null
  private var requestThreads = 0
  private var impressionThreads = 0
  private var clickThreads = 0
  private var conversionThreads = 0
  private var sparkAppName = "md_streaming_mongoDB_Row"
  private var sparkMasterUrl = "local[12]"
  private var sparkDefaultParllelism = null
  private var sparkDefaultParallelismValue = "12"
  private var sparkSerializer = null
  private var sparkSerializerValue = "org.apache.spark.serializer.KryoSerializer"
  private var sparkNetworkTimeOut = null
  private var sparkNetworkTimeOutValue = "3600"
  private var sparkStreamingUiRetainedBatches = null
  private var sparkStreamingUiRetainedBatchesValue = "5"
  private var sparkWorkerUiRetainedDrivers = null
  private var sparkWorkerUiRetainedDriversValue = "5"
  private var sparkWorkerUiRetainedExecutors = null
  private var sparkWorkerUiRetainedExecutorsValue = "30"
  private var sparkWorkerUiRetainedStages = null
  private var sparkWorkerUiRetainedStagesValue = "100"
  private var sparkUiRetainedJobs = null
  private var sparkUiRetainedJobsValue = "100"
  private var sparkJavaStreamingDurationsInSeconds = "10"
  private var sparkNumberOfSlaves = 14
  private var sparkRequestTopicShortName = null
  private var sparkImpressionTopicShortName = null
  private var sparkClickTopicShortName = null
  private var sparkConversionTopicShortName = null
  private var sparkNumberOfPartitions = 30
  private var sparkClusterDbIp = null
  private var clusterDbPort = null
  private var insertQuery = null
  private var insertOnDuplicateQuery = null
  private var sqlDriverName = null
        //  private var configFileReader: ConfigFileReader = null
  private var dbConnection = "mongodb"
  private var dbDatabase = "trading"
  private var dbPassword = "mongodb"
  private var dbUsername = "trading_user_RW"
  private var bootstrapServers = "rhes75:9092, rhes75:9093, rhes75:9094"
  private var schemaRegistryURL = "http://rhes75:8081"
  private var zookeeperConnect = "rhes75:2181" 
  private var zookeeperConnectionTimeoutMs = "10000"
  private var rebalanceBackoffMS = "15000"
  private var zookeeperSessionTimeOutMs = "15000"
  private var autoCommitIntervalMS = "12000"
  private var topicsValue = "final"
  private var memorySet = "F"
  private var enableHiveSupport = null
  private var enableHiveSupportValue = "true"
  private var sparkStreamingReceiverMaxRateValue = "0" 
  private var checkpointdir = "/checkpoint"
  private var mongodbHost = "rhes75"
  private var mongodbPort = "60100"
  private var zookeeperHost = "rhes75"
  private var zooKeeperClientPort = "2181"
  private var batchInterval = 2
  private var tickerWatch = "VOD"
  private var priceWatch: Double = 300.0
  private var op_type = 1
  private var currency = "GBP"
  private var tickerType = "short"
  private var tickerClass = "asset"
  private var tickerStatus = "valid"

  def main(args: Array[String])
  {
    // Create a StreamingContext with two working thread and batch interval of 2 seconds.

   var startTimeQuery = System.currentTimeMillis

    // Start mongoDB collection stuff
    val collectionName = "MARKETDATAMONGODBSPEED"
    val connectionString = dbConnection+"://"+dbUsername+":"+dbPassword+"@"+mongodbHost+":"+mongodbPort+"/"+dbDatabase+"."+collectionName             
   val sparkConf = new SparkConf().
             setAppName(sparkAppName).
             set("spark.driver.allowMultipleContexts", "true").
             set("spark.hadoop.validateOutputSpecs", "false")

             // change the values accordingly.
             sparkConf.set("sparkDefaultParllelism", sparkDefaultParallelismValue)
             sparkConf.set("sparkSerializer", sparkSerializerValue)
             sparkConf.set("sparkNetworkTimeOut", sparkNetworkTimeOutValue)
             // If you want to see more otherDetails of batches please increase the value
             // and that will be shown in UI.
             sparkConf.set("sparkStreamingUiRetainedBatches",
                           sparkStreamingUiRetainedBatchesValue)
             sparkConf.set("sparkWorkerUiRetainedDrivers",
                           sparkWorkerUiRetainedDriversValue)
             sparkConf.set("sparkWorkerUiRetainedExecutors",
                           sparkWorkerUiRetainedExecutorsValue)
             sparkConf.set("sparkWorkerUiRetainedStages",
                           sparkWorkerUiRetainedStagesValue)
             sparkConf.set("sparkUiRetainedJobs", sparkUiRetainedJobsValue)
             sparkConf.set("enableHiveSupport",enableHiveSupportValue)
             if (memorySet == "T")
             {
               sparkConf.set("spark.driver.memory", "18432M")
             }
             sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
             sparkConf.set("spark.streaming.receiver.writeAheadLog.enable", "true")
             sparkConf.set("spark.streaming.driver.writeAheadLog.closeFileAfterWrite", "true")
             sparkConf.set("spark.streaming.receiver.writeAheadLog.closeFileAfterWrite", "true")
             sparkConf.set("spark.streaming.backpressure.enabled","true")
             sparkConf.set("spark.streaming.receiver.maxRate",sparkStreamingReceiverMaxRateValue)
	     sparkConf.set("spark.mongodb.input.uri", connectionString)
	     sparkConf.set("spark.mongodb.output.uri", connectionString)

    val streamingContext = new StreamingContext(sparkConf, Seconds(batchInterval))
    val sparkContext  = streamingContext.sparkContext
    val sqlContext= new org.apache.spark.sql.SQLContext(sparkContext)
    import sqlContext.implicits._
    sparkContext.setLogLevel("ERROR")
    


       // Start mongoDB collection stuff
    val rdd = MongoSpark.load(sparkContext)
    val MARKETDATAMONGODBSPEED = rdd.toDF
    var rows = 0
    // get No of Docs in MongoDB collections
    rows = MARKETDATAMONGODBSPEED.count.toInt
    println("Documents in " + collectionName + ": " + rows)
    var sqltext = ""
    var totalPrices: Long = 0
    val runTime = 240
    //val HiveContext = new HiveContext(streamingContext.sparkContext)
    
    //streamingContext.checkpoint("checkpointdir")
    val writeConfig = WriteConfig(Map("collection" -> collectionName, "writeConcern.w" -> "majority"), Some(WriteConfig(sparkContext)))
    //val kafkaParams = Map[String, String]("bootstrap.servers" -> bootstrapServers, "schema.registry.url" -> schemaRegistryURL, "zookeeper.connect" -> zookeeperConnect, "group.id" -> sparkAppName )
    val kafkaParams = Map[String, String](
                                      "bootstrap.servers" -> bootstrapServers,
                                      "schema.registry.url" -> schemaRegistryURL,
                                       "zookeeper.connect" -> zookeeperConnect,
                                       "group.id" -> sparkAppName,
                                       "zookeeper.connection.timeout.ms" -> zookeeperConnectionTimeoutMs,
                                       "rebalance.backoff.ms" -> rebalanceBackoffMS,
                                       "zookeeper.session.timeout.ms" -> zookeeperSessionTimeOutMs,
                                       "auto.commit.interval.ms" -> autoCommitIntervalMS
                                     )
    //val topicsSet = topics.split(",").toSet
    val topics = Set(topicsValue)
    val dstream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](streamingContext, kafkaParams, topics)
    // This returns a tuple of key and value (since messages in Kafka are optionally keyed). In this case it is of type (String, String)
    dstream.cache()
    var current_timestamp = "current_timestamp"
    // Work on every Stream
    dstream.foreachRDD
    { pricesRDD =>
      if (!pricesRDD.isEmpty)  // data exists in RDD
      {
        val spark = SparkSessionSingleton.getInstance(pricesRDD.sparkContext.getConf)
        val sc = spark.sparkContext
        import spark.implicits._

         var endTimeQuery = System.currentTimeMillis
         // Check if running time > runTime exit
         if( (endTimeQuery - startTimeQuery)/(100000*60) > runTime)
         {
           println("\nDuration exceeded " + runTime + " minutes exiting")
           System.exit(0)
         }
         // Work on individual messages
         for(line <- pricesRDD.collect.toArray)
         {
           var key = line._2.split(',').view(0).toString
           var ticker =  line._2.split(',').view(1).toString
           var timeissued = line._2.split(',').view(2).toString
           var price = line._2.split(',').view(3).toString.toDouble
           var CURRENCY = "GBP"
          if (price > 90.0)
           {
            var op_time = System.currentTimeMillis.toString     
            var operation = new operationStruct(op_type, op_time)
            //println ("price > 90.0, saving to MongoDB collection!")
            var pricestruct = new priceStruct(key, ticker, timeissued, price, CURRENCY)
            var parralelRDD = sparkContext.parallelize((1 to 1).
                            map(p => priceDocument(pricestruct, operation)))
            val document = spark.createDataFrame(parralelRDD)
           //
           // Writing document to MongoDB collection
           //
            MongoSpark.save(document, writeConfig)

           // picking up individual arrays --> df.select('otherDetails.getItem("tickerQuotes")(0)) shows first element
           val lookups = document.filter('priceInfo.getItem("ticker") === tickerWatch && 'priceInfo.getItem("price") > priceWatch)
           if(lookups.count > 0) {
             println("High value tickers")         
             lookups.select('priceInfo.getItem("timeissued").as("Time issued"), 'priceInfo.getItem("ticker").as("Ticker"), 'priceInfo.getItem("price").cast("Double").as("Latest price")).show
           }
         } 
         }
      }
    }
    streamingContext.start() 
    streamingContext.awaitTermination() 
    //streamingContext.stop()
  }
}

case class operationStruct (op_type: Int, op_time: String)
case class tradeStruct (tickerType: String, tickerClass: String, tickerStatus: String, tickerQuotes: Array[Double])
case class priceStruct(key: String, ticker: String, timeissued: String, price: Double, currency: String)
case class priceDocument(priceInfo: priceStruct, operation: operationStruct)

class UsedFunctions {
  import scala.util.Random
  import scala.math._
  import org.apache.spark.sql.functions._
  def randomString(chars: String, length: Int): String =
     (0 until length).map(_ => chars(Random.nextInt(chars.length))).mkString
  def clustered(id : Int, numRows: Int) : Double  = (id - 1).floor/numRows
  def scattered(id : Int, numRows: Int) : Double  = (id - 1 % numRows).abs
  def randomised(seed: Int, numRows: Int) : Double  = {
  var end = numRows
   if (end == 0) end = Random.nextInt(1000)
     return (Random.nextInt(seed) % end).abs
  }
  def padString(id: Int, chars: String, length: Int): String =
     (0 until length).map(_ => chars(Random.nextInt(chars.length))).mkString + id.toString
  def padSingleChar(chars: String, length: Int): String =
     (0 until length).map(_ => chars(Random.nextInt(chars.length))).mkString
}

/** Lazily instantiated singleton instance of SparkSession */
object SparkSessionSingleton {

  @transient  private var instance: SparkSession = _

  def getInstance(sparkConf: SparkConf): SparkSession = {
    if (instance == null) {
      instance = SparkSession
        .builder
        .config(sparkConf)
        .getOrCreate()
    }
    instance
  }
}
