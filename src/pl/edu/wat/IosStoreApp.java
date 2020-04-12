package pl.edu.wat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import pl.edu.wat.mapper.*;
import pl.edu.wat.partitionera.IosStoreCategoryPricePartitioner;
import pl.edu.wat.reducer.*;

import static java.lang.System.err;

public class IosStoreApp extends Configuration {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        String[] jobArgs = {
                "/user/wawrzynimaci/iodatatest1.csv",
                "/user/wawrzynimaci/WynikJob1",
                "/user/wawrzynimaci/WynikJob2",
                "/user/wawrzynimaci/WynikJob3",
                "/user/wawrzynimaci/WynikJob4",
                "/user/wawrzynimaci/WynikJob5"
        };

        if (jobArgs.length != 6) {
            err.println("Usage: pl.edu.wat.IosStoreApp <in> <outJob1> <outJob2> <outJob3> <outJob4> <outJob5>");
            System.exit(2);
        }

        Path outPutJob = new Path(jobArgs[1]);

        Job job = Job.getInstance(conf, "IosStoreApp");
        job.setJarByClass(IosStoreApp.class);
        job.setMapperClass(IosStoreCategoryPriceMapper.class);
        job.setPartitionerClass(IosStoreCategoryPricePartitioner.class);
        job.setReducerClass(IosStoreCategoryPriceReducer.class);
        job.setNumReduceTasks(23);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(jobArgs[0]));
        FileOutputFormat.setOutputPath(job, outPutJob);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        boolean result = job.waitForCompletion(true);
        if (!result) {
            System.exit(1);
        }

        Path outPutSecJob = new Path(jobArgs[2]);

        Configuration conf2 = new Configuration();
        Job secJob = Job.getInstance(conf2, "IosStoreApps");
        secJob.setJarByClass(IosStoreApp.class);
        secJob.setMapperClass(IosStoreAppsMapper.class);
        secJob.setReducerClass(IosStoreAppsReducer.class);
        secJob.setOutputKeyClass(Text.class);
        secJob.setOutputValueClass(Text.class);
        secJob.setMapOutputKeyClass(Text.class);
        secJob.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(secJob, new Path(jobArgs[0]));
        FileOutputFormat.setOutputPath(secJob, outPutSecJob);
        secJob.setInputFormatClass(TextInputFormat.class);
        secJob.setOutputFormatClass(TextOutputFormat.class);

        boolean resultSecJob = secJob.waitForCompletion(true);
        if (!resultSecJob) {
            System.exit(1);
        }

        Configuration conf3 = new Configuration();
        Job thrJob = Job.getInstance(conf3, "IosStoreAppLang");
        thrJob.setJarByClass(IosStoreApp.class);
        thrJob.setMapperClass(IosStoreLangsMapper.class);
        thrJob.setReducerClass(IosStoreLangsReducer.class);
        thrJob.setOutputKeyClass(Text.class);
        thrJob.setOutputValueClass(Text.class);
        thrJob.setMapOutputKeyClass(Text.class);
        thrJob.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(thrJob, outPutJob);
        FileInputFormat.addInputPath(thrJob, outPutSecJob);
        FileOutputFormat.setOutputPath(thrJob, new Path(jobArgs[3]));
        thrJob.setInputFormatClass(TextInputFormat.class);
        thrJob.setOutputFormatClass(TextOutputFormat.class);


        boolean resultThrJob = thrJob.waitForCompletion(true);
        if (!resultThrJob) {
            System.exit(1);
        }

        Configuration conf4 = new Configuration();
        Job fourJob = Job.getInstance(conf4, "IosStoreAppFinalLang");
        fourJob.setJarByClass(IosStoreApp.class);
        fourJob.setMapperClass(IosStoreFinalLangsMapper.class);
        fourJob.setReducerClass(IosStoreFinalLangsReducer.class);
        fourJob.setOutputKeyClass(Text.class);
        fourJob.setOutputValueClass(IntWritable.class);
        fourJob.setMapOutputKeyClass(Text.class);
        fourJob.setMapOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(fourJob, new Path(jobArgs[3]));
        FileOutputFormat.setOutputPath(fourJob, new Path(jobArgs[4]));
        fourJob.setInputFormatClass(TextInputFormat.class);
        fourJob.setOutputFormatClass(TextOutputFormat.class);

        boolean resultFourJob = fourJob.waitForCompletion(true);
        if (!resultFourJob) {
            System.exit(1);
        }

        Configuration conf5 = new Configuration();
        Job fivJob = Job.getInstance(conf5, "Get 3 Lang for cat");
        fivJob.setJarByClass(IosStoreApp.class);
        fivJob.setMapperClass(IosStoreGetThreeLangsMapper.class);
        fivJob.setReducerClass(IosStoreGetThreeLangsReducer.class);
        fivJob.setOutputKeyClass(Text.class);
        fivJob.setOutputValueClass(Text.class);
        fivJob.setMapOutputKeyClass(Text.class);
        fivJob.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(fivJob, new Path(jobArgs[4]));
        FileOutputFormat.setOutputPath(fivJob, new Path(jobArgs[5]));
        fivJob.setInputFormatClass(TextInputFormat.class);
        fivJob.setOutputFormatClass(TextOutputFormat.class);

        boolean resultFivJob = fivJob.waitForCompletion(true);
        if (resultFivJob) {
            System.exit(1);
        }
    }
}
