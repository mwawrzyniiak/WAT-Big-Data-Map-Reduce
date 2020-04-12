package pl.edu.wat.reducer;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pl.edu.wat.parser.LangParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

import static pl.edu.wat.mapper.RegexEnum.*;

public class IosStoreGetThreeLangsReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        ArrayList<LangParser> langBank = new ArrayList<>();

        for(Text v: values) {
            String[] val = v.toString().split(UNDERSCORE_REGEX.getStr());
            try
            {
                langBank.add(new LangParser(val[0], Integer.parseInt(val[1])));
            } catch (Exception ex) {
                ex.fillInStackTrace();
                System.err.println(ex.getMessage());
                ex.printStackTrace();
            }
        }

        Collections.sort(langBank);

        int counter = 1;
        for(int i = langBank.size() - 1; i >= 0; i--) {
            context.write(key, new Text(langBank.get(i).getLang() + "_" + langBank.get(i).getCounts()));
            if(counter == 3) {
                return;
            }
            counter++;
        }
    }
}
