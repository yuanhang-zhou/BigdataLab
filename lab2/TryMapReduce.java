package com.example; //替换成你自己的项目名

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TryMapReduce {
    
    public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

        private Text word = new Text(); // 单词
        private final static IntWritable one = new IntWritable(1); // 值为1的IntWritable对象
        private Text docId = new Text(); // 文档ID

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); // 获取文件名
            String cleanedValue = value.toString().replaceAll("\\p{Punct}", " ").toLowerCase(); // 去除标点符号并转为小写
            StringTokenizer itr = new StringTokenizer(cleanedValue); // 分词器
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken()); // 设置单词
                docId.set(fileName + ":" + one); // 设置文档ID
                context.write(word, docId); // 输出键值对
            }
        }
    }

    public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
        private Text result = new Text(); // 结果文本对象

        public void reduce(Text key, Iterable<Text> values,
                           Context context
                           ) throws IOException, InterruptedException {
            Map<String, Integer> map = new HashMap<>(); // 存储文档和对应计数的映射
            int sum = 0; // 总计数
            int docCount = 0; // 文档数量

            for (Text val : values) {
                String[] docCountPair = val.toString().split(":"); // 拆分文档ID和计数
                String doc = docCountPair[0]; // 文档ID
                Integer count = Integer.parseInt(docCountPair[1]); // 计数
                map.put(doc, map.getOrDefault(doc, 0) + count); // 更新文档计数
                sum += count; // 更新总计数
            }

            docCount = map.size(); // 计算文档数量
            double average = sum / (double) docCount; // 计算平均值
            StringBuilder stringBuilder = new StringBuilder(); // 用于构建结果字符串
            stringBuilder.append(String.format("%.2f", average)); // 添加平均值

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                stringBuilder.append(", ").append(entry.getKey()).append(":").append(entry.getValue()); // 添加文档和计数
            }

            result.set(stringBuilder.toString()); // 设置结果字符串
            context.write(key, result); // 输出键值对
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration(); // 配置对象
        Job job = Job.getInstance(conf, "inverted index"); // 创建作业对象
        job.setJarByClass(TryMapReduce.class); // 设置主类
        job.setMapperClass(TokenizerMapper.class); // 设置Mapper类
        job.setReducerClass(IntSumReducer.class); // 设置Reducer类
        job.setOutputKeyClass(Text.class); // 设置输出键类型
        job.setOutputValueClass(Text.class); // 设置输出值类型
        FileInputFormat.addInputPath(job, new Path(args[0])); // 设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1])); // 设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1); // 提交作业并等待完成
    }
}