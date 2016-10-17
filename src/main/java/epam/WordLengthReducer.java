package epam;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Created by alexander on 17.10.16.
 */
public class WordLengthReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
    int maxLength = 0;
    Iterable<Text> longestWords;
    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        if(key.get()>maxLength){
            maxLength = key.get();
            longestWords = values;
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text word :
                longestWords) {
            context.write(new IntWritable(maxLength), word);
        }
    }
}
