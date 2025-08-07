import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class FinancialDriver {

    public static void main(String[] args) throws Exception {

        // Check arguments
        if (args.length != 2) {
            System.err.println("Usage: FinancialDriver <input path> <output path>");
            System.exit(-1);
        }

        // Create Hadoop job configuration
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Retail Financial Analysis");

        // Set Jar class
        job.setJarByClass(FinancialDriver.class);

        // Set Mapper and Reducer class
        job.setMapperClass(FinancialMapper.class);
        job.setReducerClass(FinancialReducer.class);

        // Set output key/value types
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Submit the job and wait for completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}