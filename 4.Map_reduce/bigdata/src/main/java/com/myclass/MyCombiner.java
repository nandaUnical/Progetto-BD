package com.myclass;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import com.myclass.Main;

public class MyCombiner extends Reducer<Text, Text, Text, Text> {

    private static Text nullText = new Text();

    @Override
    protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //header
        Text header = new Text(Main.HEADER);
        context.write(header, nullText);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        //key -> data
        //values -> riga_train,prezzo_olio

        for (Text value : values) {
            context.write(value, nullText);
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
        System.out.println("Combiner phase ended...");
    }

}