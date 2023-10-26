package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Set;
import java.util.TreeSet;

public class MergeReducer extends Reducer<Text, Text, Text, Text> {

    private Text outputKey = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        outputKey.set(key);

        Set<String> uniqueValues = new TreeSet<>();

        // 将值添加到集合中，自动去重并按字典序排序
        for (Text value : values) {
        uniqueValues.add(value.toString());
        }   

        // 输出排序后的结果
        for (String sortedValue : uniqueValues) {
        context.write(key, new Text(sortedValue));
        }
    }
}