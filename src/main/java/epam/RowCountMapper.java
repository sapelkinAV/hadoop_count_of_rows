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
        StringTokenizer wordTokenizer = new StringTokenizer(line, " ");
        while (wordTokenizer.hasMoreTokens()) {
                for (int i = 0; i < 2; i++) {
                    //идем до поля iPinyou ID(*)
                    if(wordTokenizer.hasMoreTokens())
                    wordTokenizer.nextToken();
                }
            context.write(new Text(wordTokenizer.nextToken()), new IntWritable(1));
            for(int i=0;i<18;i++){
                if(wordTokenizer.hasMoreTokens())
                wordTokenizer.nextToken();
            }
        }
    }
}
