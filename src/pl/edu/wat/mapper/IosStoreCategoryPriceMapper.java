package pl.edu.wat.mapper;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import static java.lang.System.err;
import static pl.edu.wat.mapper.ColumnEnum.CATEGORY_COLUMN_NUMBER;
import static pl.edu.wat.mapper.ColumnEnum.PRICE_COLUMN_NUMBER;
import static pl.edu.wat.mapper.RegexEnum.*;

public class IosStoreCategoryPriceMapper extends Mapper<Object, Text, Text, DoubleWritable> {
    private Text category = new Text();
    private DoubleWritable price = new DoubleWritable();

    private final String PRIMARY_GENRE_COLUMN_NAME = "Primary_Genre";

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split(QUOTES_REGEX.getStr());

        if (isRecordHasSixteenColumns(values)) {
            if (isTitleRecord(values[CATEGORY_COLUMN_NUMBER.getVal()])) {
                return;
            }

            category.set(values[CATEGORY_COLUMN_NUMBER.getVal()]);
            try {
                price.set(Double.parseDouble(values[PRICE_COLUMN_NUMBER.getVal()]));
            } catch (NumberFormatException ex) {
                err.println(ex.getMessage());
                return;
            }
            context.write(category, price);
        }
    }

    private boolean isTitleRecord(String value1) {
        return value1.equals(PRIMARY_GENRE_COLUMN_NAME);
    }

    private boolean isRecordHasSixteenColumns(String[] values) {
        return values.length == 16;
    }
}

