package pl.edu.wat.reducer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IosStoreFinalLangsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int counter = 0;

        for (IntWritable i : values) {
            counter++;
        }
        context.write(key, new IntWritable(counter));
    }
}
