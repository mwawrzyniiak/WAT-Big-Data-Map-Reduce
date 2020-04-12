package pl.edu.wat.partitionera;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class IosStoreCategoryPricePartitioner extends Partitioner<Text, DoubleWritable> {
    @Override
    public int getPartition(Text key, DoubleWritable value, int numReduceTasks) {
        switch (key.toString()) {
            case "Book":
                return 0;
            case "Business":
                return 1;
            case "Education":
                return 2;
            case "Finance":
                return 3;
            case "Food & Drink":
                return 4;
            case "Games":
                return 5;
            case "Health & Fitness":
                return 6;
            case "Magazines & Newspapers":
                return 7;
            case "Medical":
                return 8;
            case "Music":
                return 9;
            case "Navigation":
                return 10;
            case "News":
                return 11;
            case "Photo & Video":
                return 12;
            case "Productivity":
                return 13;
            case "Reference":
                return 14;
            case "Shopping":
                return 15;
            case "Social Networking":
                return 16;
            case "Sports":
                return 17;
            case "Stickers":
                return 18;
            case "Travel":
                return 19;
            case "Utilities":
                return 20;
            case "Weather":
                return 21;
            default:
                return 22;
        }
    }
}
