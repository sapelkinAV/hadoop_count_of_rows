package epam;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Created by Alexey Suprun
 */
public class Main extends Configured implements Tool {
    public static void main(String... args) throws Exception {
        System.exit(ToolRunner.run(new Main(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        Job job = JobBuilder.parseInputAndOutput(this, getConf(), args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(RowCountMapper.class);
        job.setReducerClass(RowCountReducer.class);

        job.setNumReduceTasks(1);
        job.waitForCompletion(true);
        TaskReport[] mapReports = job.getTaskReports(TaskType.MAP);
        TaskReport[] reduceReports = job.getTaskReports(TaskType.REDUCE);
        printReport(mapReports,"MAP");
        printReport(reduceReports,"REDUCE");
        return 0;

    }

    private void printReport(TaskReport[] reports,String type) {
        long sum_time = 0;
        for(TaskReport report : reports) {
            long time = report.getFinishTime() - report.getStartTime();
            sum_time += time;
            System.out.println(report.getTaskId() + " took " + time + " millis!");
        }
        System.out.println("final time for "+type +" "+String.valueOf(sum_time));
    }


}
