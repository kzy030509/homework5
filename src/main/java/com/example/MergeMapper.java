package com.example;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MergeMapper extends Mapper<Object, Text, Text, Text> {
    private Text outputKey = new Text();
    private Text outputValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String index = fields[0];
        String component = fields[1];
        outputKey.set(index);
        outputValue.set(component);
        context.write(outputKey, outputValue);

    }
}