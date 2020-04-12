package pl.edu.wat.mapper;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import static pl.edu.wat.mapper.ColumnEnum.*;
import static pl.edu.wat.mapper.RegexEnum.*;

import java.io.IOException;

public class IosStoreAppsMapper extends Mapper<Object, Text, Text, Text> {
    private final String PRIMARY_GENRE_COLUMN_NAME = "Primary_Genre";

    private Text category = new Text();
    private Text langs = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(QUOTES_REGEX.getStr());

        if (isRecordHasSixteenColumns(values)) {
            if (values[CATEGORY_COLUMN_NUMBER.getVal()].equals(PRIMARY_GENRE_COLUMN_NAME)) {
                return;
            }
            category.set(values[CATEGORY_COLUMN_NUMBER.getVal()]);

            String languages = values[PRICE_COLUMN_NUMBER.getVal()] + "_" + getLanguages(values[LANGUAGE_COLUMN_NUMBER.getVal()]);
            langs.set(languages);

            context.write(category, langs);
        }
    }

    private boolean isRecordHasSixteenColumns(String[] values) {
        return values.length == 16;
    }

    /*
    Column with languages have a two types:
    first, if it is only one languages - ['LN']
    second, 2 or more languages - "['EN','CH','FR']"
     */
    public static String getLanguages(String str) {
        String languages = "";
        if (str.substring(0, 1).equals("[") && str.length() == 6) {
            return str.substring(2, 4);
        } else if (str.substring(0, 1).equals("\"")) {
            String helper = str.substring(2, str.length() - 2);
            String[] langs = helper.split(",");

            for (int i = 0; i < langs.length; i++) {
                langs[i] = langs[i].replaceAll("\\s+", "");
            }

            for (int i = 0; i < langs.length; i++) {
                if (i == langs.length - 1) {
                    languages = languages + langs[i].substring(1, 3);
                } else {
                    languages = languages + langs[i].substring(1, 3) + "_";
                }
            }
            return languages;
        } else {
            return "CONVERTER_ERROR";
        }
    }
}
