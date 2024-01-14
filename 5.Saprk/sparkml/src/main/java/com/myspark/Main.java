package com.myspark;

import com.myspark.SparkImportData;
import com.myspark.SparkCreateModel;

import java.io.File;
import java.io.IOException;

public class Main {
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        //deleteFolderContent("csv_data", true);

        //SparkImportData.run("input/train.csv");
        //SparkImportData.run("input/test.csv");
        
        SparkCreateModel.run("csv_data");
    } 

    public static void deleteFolderContent(String path, boolean preserveRoot) {
        File folder = new File(path);
        File[] files = folder.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isDirectory())
                    deleteFolderContent(f.getPath(), false);
                else
                    f.delete();
            }
        }
        if (!preserveRoot)
            folder.delete();
    }
}