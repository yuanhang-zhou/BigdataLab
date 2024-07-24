package example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Random;

//计算每个点的度
public class OutDegreeStat {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        conf.set("mapreduce.reduce.memory.mb", "2048");  //设置reduce container的内存大小
        conf.set("mapreduce.reduce.java.opts", "-Xmx2048m");  //设置reduce任务的JVM参数

        //输入参数为输入文件以及记录度的输出文件
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        Path tempdir = new Path("OutDegreeStat-temp-"
                + Integer.toString(new Random().nextInt(Integer.MAX_VALUE)));
        Job job = Job.getInstance(conf, "outdegree");
        job.setJarByClass(OutDegreeStat.class);
        //设置Mapper
        job.setMapperClass(OutDegreeStatMapper.class);
        //
        job.setReducerClass(OutDegreeStatReducer.class);
        job.setCombinerClass(OutDegreeStatCombiner.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        SequenceFileOutputFormat.setOutputPath(job, tempdir);
        job.setNumReduceTasks(Main.ReducerNum);
        //提交第一个作业并等待完成，对度数进行排序
        if (job.waitForCompletion(true)) {
            Job sortedJob = Job.getInstance(conf, "outdegree sort");
            sortedJob.setJarByClass(OutDegreeStat.class);

            sortedJob.setMapperClass(InverseMapper.class);
            sortedJob.setMapOutputKeyClass(IntWritable.class);
            sortedJob.setMapOutputValueClass(IntWritable.class);
            sortedJob.setOutputKeyClass(IntWritable.class);
            sortedJob.setOutputValueClass(IntWritable.class);
            sortedJob.setInputFormatClass(SequenceFileInputFormat.class);
            sortedJob.setOutputFormatClass(SequenceFileOutputFormat.class);
            SequenceFileInputFormat.addInputPath(sortedJob, tempdir);
            SequenceFileOutputFormat.setOutputPath(sortedJob, new Path(otherArgs[1]));
            sortedJob.setNumReduceTasks(1);
            boolean flag = sortedJob.waitForCompletion(true);
            // 删除临时目录
            FileSystem.get(conf).delete(tempdir, true);
            if (!flag) {
                System.exit(1);
            }
        }
    }
    //Mapper类：计算每个顶点的度数
    public static class OutDegreeStatMapper extends Mapper<IntWritable, IntWritable, IntWritable, IntWritable> {
        private final IntWritable one = new IntWritable(1);

        @Override
        public void map(IntWritable key, IntWritable value, Context context)
                throws IOException, InterruptedException {
            if (key.get() != value.get()) {
                context.write(key, one);
                context.write(value, one);
            }
        }
    }
    // Reducer类：汇总每个顶点的度数
    public static class OutDegreeStatReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
    // Combiner类：局部汇总每个顶点的度数，减少数据传输量
    public static class OutDegreeStatCombiner extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val: values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }
}