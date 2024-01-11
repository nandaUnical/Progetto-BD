package com.myclass;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.myclass.MyCombiner;
import com.myclass.MyMapper;
import java.io.File;
import java.io.IOException;
import java.io.IOException;


public class Main {

    public static String HEADER = "";

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        HEADER = "id,date,store_nbr,family,sales,onpromotion,store_cluster";
        runJob("input/train.csv", "train_output");
        HEADER = "id,date,store_nbr,family,onpromotion,store_cluster";
        runJob("input/test.csv", "test_output");
    }

    public static void runJob(String inputCSV, String outputFolder) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("Starting mapreduce...");
        org.apache.log4j.BasicConfigurator.configure();

        //Delete the output file from previous runs
        Main.deleteFolderContent(outputFolder, false);

        //Job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "job");

        //Clases
        job.setJarByClass(Main.class);
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyCombiner.class);

        //Job output types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        //Job input/output path
        FileInputFormat.addInputPath(job, new Path(inputCSV));//larger .csv
        FileOutputFormat.setOutputPath(job, new Path(outputFolder));

        job.waitForCompletion(true);
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