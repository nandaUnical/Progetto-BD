package com.myspark;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.GBTRegressionModel;
import org.apache.spark.ml.regression.GBTRegressor;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressionModel;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.functions;




public class SparkCreateModel {

    private static final SparkSession SESSION = sparkSessionBuilder();

    public static String[] features_train = new String[]{
        "date",
        "store_nbr",        
        "sales",
        "onpromotion",
        "store_cluster",
        "family_num"
    };
    public static String[] features_test = new String[]{
        "date",
        "store_nbr",        
        "onpromotion",
        "store_cluster",
        "family_num"
    };

    public static void run(String PATH) {
        
        ///1.IMPORT AND SPLIT DATA
        //Import train content
        Dataset<Row> dataset = importDataCSV(SESSION, PATH+"/train.csv");
        //Import test content
        Dataset<Row> testData = importDataCSV(SESSION, PATH+"/test.csv");

        //Split data into train and validation sequentially
        String splitDate = "2016-01-01";
        Dataset<Row> trainData = dataset.filter("date < '" + splitDate + "'");
        Dataset<Row> valData = dataset.filter("date >= '" + splitDate + "'");
        
        //Converting all data to float and handle dates
        trainData = Convert2Float(trainData,features_train);
        valData = Convert2Float(valData,features_train);
        testData = Convert2Float(testData, features_test);
        String[] features = new String[]{"store_nbr", "onpromotion", "store_cluster", "family_num", "year", "month", "day"};

        ///2.PREPARE DATA TO BE USED BY SPARKML
        trainData = prepareDataset(trainData, features);
        valData = prepareDataset(valData, features);
        testData = prepareDataset(testData, features);

        ///3.CREATE MODEL
        //Create evaluator
        RegressionEvaluator evaluator = new RegressionEvaluator()
            .setLabelCol("sales")               
            .setPredictionCol("prediction")
            .setMetricName("rmse");
        
        //double rmse = evaluator.evaluate(predictions);

        //Create classifier
        RandomForestRegressor classifier = new RandomForestRegressor();
        classifier.setLabelCol("sales");
        classifier.setPredictionCol("prediction");
        classifier.setFeaturesCol("features");

        //training
        RandomForestRegressionModel model = classifier.fit(trainData);

        //RMSE over validation dataset
        Dataset<Row> valPredictions = model.transform(valData);
        double rmse = evaluator.evaluate(valPredictions);

        System.out.println("validation rmse: "+ rmse);
        valPredictions.select("year", "month", "day","sales","prediction").show(false);

        Dataset<Row> testPredictions = model.transform(testData);
        testPredictions.select("year", "month", "day","prediction").show(false);
    }

    private static SparkSession sparkSessionBuilder() {
        SparkSession session = SparkSession.builder().master("local").appName("buildModels").getOrCreate();
        session.sparkContext().setLogLevel("error");
        return session;
    }

    public static Dataset<Row> importDataCSV(SparkSession spark, String csvPath) {
        // Read CSV and load data into a DataFrame
        Dataset<Row> data = spark.read().option("header", "true").csv(csvPath);   
        //data.printSchema();
        //data.show();
        return data;
    }

    private static Dataset<Row> Convert2Float(Dataset<Row> dataset, String[] features){
        for (String f : features) {
            if (f == "date"){
                dataset = dataset
                    .withColumn("year", functions.year(dataset.col("date")))
                    .withColumn("month", functions.month(dataset.col("date")))
                    .withColumn("day", functions.dayofweek(dataset.col("date")));
                dataset = dataset.drop("date");
            }
            else {
                dataset = dataset.withColumn(f, dataset.col(f).cast(DataTypes.DoubleType));
            }
        }
        
        return dataset;
    }

     

    private static Dataset<Row> prepareDataset(Dataset<Row> dataset, String[] features){
        VectorAssembler vectorAssembler = new VectorAssembler()
            .setInputCols(features)
            .setOutputCol("features")
            .setHandleInvalid("skip");
        return vectorAssembler.transform(dataset);
    }


    
}
