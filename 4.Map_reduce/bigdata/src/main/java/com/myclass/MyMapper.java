package com.myclass;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import com.myclass.Main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class MyMapper extends Mapper<Object, Text, Text, Text> {

    HashMap<String, String> storeDataMap = new HashMap<>();

    @Override
    protected void setup(Mapper<Object, Text, Text, Text>.Context context) {
        System.out.println("Mapping phase started...");
        buildDataMapFromCSV(context);
        System.out.println("CSV file has been read...");
    }
    @Override
    protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        // Parse the input line from train.csv
        //id,date,store_nbr,family,sales,onpromotion
        String[] trainValues = value.toString().split(",");
        String trainStore = trainValues[2];

        String storeValue = storeDataMap.getOrDefault(trainStore, "");
        
        String outputValue = String.join(",", trainValues) + "," + storeValue;
        context.write(new Text(trainStore), new Text(outputValue));
    }

    @Override
    protected void cleanup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        System.out.println("Mapping phase ended...");
    }

    //Build a map using the smaller .csv
    private void buildDataMapFromCSV(Context context) {
        try (BufferedReader br = new BufferedReader(new FileReader("input/stores.csv"))) {
            String line;
            //line: store_nbr,city,state,type,cluster
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                String primKey = values[0];
                String storeCluster = values.length > 1 ? values[4] : "";
                this.storeDataMap.put(primKey, storeCluster);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}