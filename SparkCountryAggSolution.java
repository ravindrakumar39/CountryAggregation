package demo;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import com.spark.SparkCSVCassandra.ToDate;

import scala.Function1;
import scala.Tuple2;

public class SparkAggCust {

	public static void main(String[] args) {
		// C:\vks\others\spark
		
		SparkSession sparkSession = SparkSession.builder().master("local").appName("JavaSparkSQL")
              .getOrCreate();

      

      Dataset<Row> csvDf = sparkSession.read().format("csv")
              .option("header","true")
              .load("C:\\vks\\others\\spark\\*.csv");

//      csvDf.printSchema();
      csvDf.show();
      
      JavaPairRDD<String, String> counts = csvDf.javaRDD().mapToPair(new PairFunction<Row, String, String>() {

		@Override
		public Tuple2<String, String> call(Row t) throws Exception {
			// TODO Auto-generated method stub
			return new Tuple2(t.getAs(0), t.getAs(1));
		}

		
	}).reduceByKey(new Function2<String, String, String>() {
		
		@Override
		public String call(String v1, String v2) throws Exception {
			String[] lst = v1.split(";");
			
			ArrayList<String> arr=new ArrayList<String>();
			int i=0;
			
			for(String s: v2.split(";")) {
				
				arr.add(Integer.parseInt(s)+Integer.parseInt(lst[i])+"");
				i++;
			}
					
			return String.join(";", arr);
		}
	});

      counts.foreach(new VoidFunction<Tuple2<String,String>>() {
		
		@Override
		public void call(Tuple2<String, String> t) throws Exception {
			System.out.println(t._1+" "+t._2);
			
		}
	});
	}

}
