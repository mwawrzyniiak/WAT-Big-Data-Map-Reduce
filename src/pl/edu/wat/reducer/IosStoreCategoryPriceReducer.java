package pl.edu.wat.reducer;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class IosStoreCategoryPriceReducer extends Reducer<Text, DoubleWritable, Text, Text> {
    private Text avgPrice = new Text();
    private final String AVG = "AVG_";

    @Override
    public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        double sum = 0;
        double len = 0;
        double avg = 0;
        String avgName = "";

        for (DoubleWritable i : values) {
            sum += i.get();
            len++;
        }

        if (len > 0) {
            avg = sum / len;
        } else {
            avg = 0;
        }

        avgName = AVG + avg;
        avgPrice.set(avgName);

        context.write(key, avgPrice);
    }
}
