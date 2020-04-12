package pl.edu.wat.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pl.edu.wat.parser.IosStoreLangParser;

import java.io.IOException;
import java.util.ArrayList;

import static pl.edu.wat.mapper.RegexEnum.*;

public class IosStoreFinalLangsMapper extends Mapper<Object, Text, Text, IntWritable> {
    public ArrayList<IosStoreLangParser> priceAndLang = new ArrayList<>();
    public ArrayList<IosStoreLangParser> avgCat = new ArrayList<>();
    public ArrayList<IosStoreLangParser> elementsToDelete = new ArrayList<>();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split(TAB_REGEX.getStr());

        IosStoreLangParser val = new IosStoreLangParser(values[0], values[1]);

        try {
            if (val.isAvgPrice()) {
                avgCat.add(val);

                RemoveSmaller(val.getCategory(), val.getPrice());
                for (IosStoreLangParser out : priceAndLang) {
                    if (out.getCategory().equals(val.getCategory())) {
                        for (int i = 0; i < out.getLangs().size(); i++) {
                            context.write(new Text(out.getCategory() + "_" + out.getLangs().get(i)), new IntWritable(1));
                        }
                        elementsToDelete.add(out);
                    }
                }
                for (IosStoreLangParser del : elementsToDelete) {
                    priceAndLang.remove(del);
                }
                elementsToDelete.clear();
            } else {
                priceAndLang.add(val);
                for (IosStoreLangParser v : avgCat) {
                    if (v.getCategory().equals(val.getCategory())) {
                        RemoveSmaller(val.getCategory(), v.getPrice());
                        for (IosStoreLangParser out : priceAndLang) {
                            if (out.getCategory().equals(val.getCategory())) {
                                for (int i = 0; i < out.getLangs().size(); i++) {
                                    context.write(new Text(out.getCategory() + "_" + out.getLangs().get(i)), new IntWritable(1));
                                }
                                elementsToDelete.add(out);
                            }
                        }
                        for (IosStoreLangParser del : elementsToDelete) {
                            priceAndLang.remove(del);
                        }
                        elementsToDelete.clear();
                    }
                }
            }
        } catch (Exception e) {
            e.fillInStackTrace();
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void RemoveSmaller(String category, Double price) {
        priceAndLang.removeIf(v -> v.getCategory().equals(category) && v.getPrice() <= price);
    }
}