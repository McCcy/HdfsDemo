package com.it18zhang.hdfs.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * WCMapper
 */
public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
//    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        Text keyOut = new Text();
//        IntWritable valueOut = new IntWritable();
//        String[] arr = value.toString().split(" ");
//        for(String s : arr){
//            keyOut.set(s);
//            valueOut.set(1);
//            context.write(keyOut,valueOut);
//        }
//    }
private final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        FileSplit split = (FileSplit) context.getInputSplit();
        String path = split.getPath().toString();
        System.err.println("---------------------------------------------------------------");
        System.err.println(path);
        System.err.println("---------------------------------------------------------------");
        String line = value.toString().replace("\"", "");
        StringTokenizer token = new StringTokenizer(line);
        int i = 0;
        String level = "等级：";
        String type = "类型：";
        String total = "总死亡人数：";
        int count = 0;
        while (token.hasMoreTokens()) {
            word.set(token.nextToken());
            if(i == 0) {
                level =  level + word.toString()+ " 死亡人数：";
            }
            if(i == 1) {
                type =  type + word.toString() + " 死亡人数：";
            }
            if (i ==2 ){
                try {
                    count =count + Integer.parseInt(word.toString().replace("\"", ""));
                }catch (Exception e) {
                    System.out.println("转型错了"+word.toString().replace("\"", ""));
                }
                continue;
            }
            context.write(word, one);
            i++;
        }
        word.set(level);
        context.write(word, new IntWritable(count));
        word.set(type);
        context.write(word, new IntWritable(count));
        word.set(total);
        context.write(word, new IntWritable(count));

    }
}
