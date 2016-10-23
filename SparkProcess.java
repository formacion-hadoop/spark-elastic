package com.formacionhadoop;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import scala.Tuple2;

public class SparkProcess {

	static String fileName = "/tmp/clientesTienda";
	static String[] schema = { "cod", "nombre", "apellidos", "telefono"};

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkToElasticSearch");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> textFile = sc.textFile(fileName);

		JavaRDD<Map<String, ?>> fields = textFile
				.map(new Function<String, Map<String, ?>>() {
					private static final long serialVersionUID = 1L;

					public Map<String, ?> call(String line) throws Exception {
						Map<String, String> elasticFields = new HashMap<String, String>();
						String fieldSplit[] = line.split(",");

						for (int i = 1; i < fieldSplit.length; i++) {
							elasticFields.put(schema[i], fieldSplit[i]);
										
						}

						return elasticFields;
					}
				});

		JavaEsSpark.saveToEs(fields, "spark/clientesRDD", getConfElastic());

		JavaPairRDD<String, HashMap<String, Object>> rddFields = textFile
				.mapToPair(new PairFunction<String, String, HashMap<String, Object>>() {

					/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

					public Tuple2<String, HashMap<String, Object>> call(String s) {
						HashMap<String, Object> values = new HashMap<String, Object>();
						String lineValues[] = s.split(",");
						String id = "";
						for (int i = 0; i < schema.length; i++) {
							if (i < lineValues.length) {
								values.put(schema[i], lineValues[i]);
							}
						}
						id = lineValues[0];
						return new Tuple2<String, HashMap<String, Object>>(id,
								values);
					}
				});

		JavaEsSpark.saveToEsWithMeta(rddFields, "spark/clientesPairRDD",
				getConfElastic());
		sc.close();

	}

	public static Map<String, String> getConfElastic() {
		Map<String, String> conf = new HashMap<String, String>();
		conf.put("es.nodes", "127.0.0.1");
		conf.put("es.port", "9200");

		return conf;
	}

}
