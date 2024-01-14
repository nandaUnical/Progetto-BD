package com.myspark;

import com.myspark.Main;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.Row;
import org.apache.spark.ml.feature.StringIndexer;

import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;

public class SparkImportData {

    private static final SparkSession SESSION = sparkSessionBuilder();

    public static void run(String PATH) {
        //Remove previous content in the output
        Main.deleteFolderContent("temporal_data", false);
        //Import content
        Dataset<Row> salesInfo = importDataCSV(SESSION, PATH);
        //Transform the data
        salesInfo = featureEngineering( salesInfo, "family", "family_num");
        //Save data in .csv
        saveDatasetAsCSV(salesInfo.toDF(), "temporal_data");
        //Copy data to final directory
        copyCsvFiles("temporal_data", "csv_data", PATH);
    }

    
    private static SparkSession sparkSessionBuilder() {
        SparkSession session = SparkSession.builder().master("local").appName("importData").getOrCreate();
        session.sparkContext().setLogLevel("error");
        return session;
    }

    private static void saveDatasetAsCSV(Dataset<Row> dataset, String path) {
        dataset.coalesce(1).write().option("header", "true").csv(path);
    }
    
    public static Dataset<Row> importDataCSV(SparkSession spark, String csvPath) {
        // Read CSV and load data into a DataFrame
        Dataset<Row> data = spark.read().option("header", "true").csv(csvPath);   
        //data.printSchema();
        //data.show();
        return data;
    }

    public static Dataset<Row> featureEngineering(Dataset<Row> data, String columnString, String newColName) {
        StringIndexer indexer = new StringIndexer().setInputCol(columnString).setOutputCol(newColName);

        data = indexer.fit(data).transform(data);
        data = data.drop(columnString);
        //data = data.withColumn("family_num", data.col("family_num").cast(DataTypes.IntegerType));

        //data.show();
        return data;
    }

    public static void copyCsvFiles(String sourceDir, String destDir, String newName) {
        File sourceDirectory = new File(sourceDir);
        File destinationDirectory = new File(destDir);

        try {            
            if (!sourceDirectory.exists() || !sourceDirectory.isDirectory()) {
                throw new IllegalArgumentException("No such file or directory.");
            }

            if (!destinationDirectory.exists()) {
                destinationDirectory.mkdirs();
            }

            File[] csvFiles = sourceDirectory.listFiles((dir, name) -> name.endsWith(".csv"));

            for (File csvFile : csvFiles) {
                String newFileName = newName.substring(6);
                File destinationFile = new File(destinationDirectory, newFileName);
                FileUtils.copyFile(csvFile, destinationFile);
            }     
        
        } catch (IOException e) {
            System.err.println("Error:" + e.getMessage());
        }
    }

}