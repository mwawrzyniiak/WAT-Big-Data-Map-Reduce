package pl.edu.wat.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static pl.edu.wat.mapper.RegexEnum.*;

public class IosStoreLangsMapper extends Mapper<Object, Text, Text, Text> {

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(TAB_REGEX.getStr());
        context.write(new Text(values[0]), new Text(values[1]));
    }
}

