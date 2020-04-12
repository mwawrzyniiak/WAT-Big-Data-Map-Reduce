package pl.edu.wat.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static pl.edu.wat.mapper.RegexEnum.*;

public class IosStoreGetThreeLangsMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(TAB_REGEX.getStr());
        String[] helper = values[0].split(UNDERSCORE_REGEX.getStr());
        context.write(new Text(helper[0]), new Text(helper[1] + "_" + values[1]));
    }
}
