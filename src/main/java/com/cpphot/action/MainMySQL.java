package com.cpphot.action;

import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class MainMySQL {
	public static void main(String[] args) {

		SparkConf sparkConf = new SparkConf();

		sparkConf.setAppName("Demo_Mysql");
		sparkConf.setMaster("local[5]");
		JavaSparkContext sc = null;

		sc = new JavaSparkContext(sparkConf);

		SQLContext sqlContext = new SQLContext(sc);

		// 一个条件表示一个分区
		String[] predicates = new String[] { "1=1" };

		String url = "jdbc:mysql://master:3306/test";
		String table1 = "car_type";
		String table2 = "car_data";
		Properties connectionProperties = new Properties();
		connectionProperties.setProperty("user", "root");// 设置用户名
		connectionProperties.setProperty("password", "123456");// 设置密码

		// 读取数据
		Dataset<Row> jdbcDF = sqlContext.read().jdbc(url, table1, predicates,
				connectionProperties);

		jdbcDF.createOrReplaceTempView("car_type");

		jdbcDF = sqlContext.read().jdbc(url, table2, predicates,
				connectionProperties);
		
		jdbcDF.createOrReplaceTempView("car_data");
		
		sqlContext.sql("select car.car_num,car.car_num_color,type.name from car_type as type, car_data as car where car.car_type=type.id").show();
	}
}
