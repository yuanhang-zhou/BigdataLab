package example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;


public class DataPreprocessUtil {

    public static int Discrete2Sequence(Configuration conf, String inputPath, String outputPath) {
        // 存储顶点及其对应的唯一ID
        Map<String, Integer> existedVertex = new HashMap<String, Integer>();
        //序号
        int cnt = 0;

        try {
            FileSystem fs = FileSystem.get(new URI(inputPath), conf);
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(new Path(inputPath))));
            IntWritable key = new IntWritable();
            IntWritable value = new IntWritable();
            //输出文件
            SequenceFile.Writer writer = SequenceFile.createWriter(conf, SequenceFile.Writer.file(new Path(outputPath)),
                    SequenceFile.Writer.keyClass(key.getClass()), SequenceFile.Writer.valueClass(value.getClass()));
            String line;
            String vertex1, vertex2;
            int a, b;
            //逐行读取文件
            while ((line = reader.readLine()) != null) {
                //分词器
                StringTokenizer itr = new StringTokenizer(line, ", ");
                //读取两个顶点
                vertex1 = itr.nextToken();
                vertex2 = itr.nextToken();
                //为顶点1分配ID
                if (!existedVertex.containsKey(vertex1)) {
                    existedVertex.put(vertex1, cnt);
                    a = cnt;
                    cnt++;
                } else {
                    a = existedVertex.get(vertex1);
                }
                //为顶点2分配ID
                if (!existedVertex.containsKey(vertex2)) {
                    existedVertex.put(vertex2, cnt);
                    b = cnt;
                    cnt++;
                } else {
                    b = existedVertex.get(vertex2);
                }
                //设置键值对
                key.set(a);
                value.set(b);
                writer.append(key, value);
            }
            reader.close();
            writer.close();
        }catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return cnt;
    }
}