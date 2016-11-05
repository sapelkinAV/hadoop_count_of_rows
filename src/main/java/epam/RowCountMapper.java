package epam;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by alexander on 17.10.16.
 */
public class RowCountMapper extends Mapper<LongWritable,Text,Text,IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        StringTokenizer lineTokenizer = new StringTokenizer(line, "\n");
        while (lineTokenizer.hasMoreTokens()) {
            StringTokenizer wordTokenizer = new StringTokenizer(lineTokenizer.nextToken(), "\t");
            for (int i = 0; i < 3; i++) {
                wordTokenizer.nextToken();
            }

            context.write(new Text(wordTokenizer.nextToken()), new IntWritable(1));
        }
    }
}
