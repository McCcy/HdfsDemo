package com.it18zhang.hdfs.mr;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 */
public class MyPartitioner extends Partitioner<Text, IntWritable>{
    public int getPartition(Text text, IntWritable intWritable, int numPartitions) {
//        System.out.println("~~~~~~~~~~~~~~"+text);
        int result =0;
        if (text.toString().contains("总死亡人数")) {
            result = 2 % numPartitions;
        } else if (text.toString().contains("等级") || text.toString().contains("类型")) {
            result = 1 % numPartitions;
        } else {
            result = 0 % numPartitions;
        }
        return result;
    }
}
