package pl.edu.wat.parser;

import pl.edu.wat.mapper.RegexEnum;

import java.util.ArrayList;

public class IosStoreLangParser {
    private String category;
    private boolean isAvgPrice;
    private double price;
    ArrayList<String> langs = new ArrayList<>();

    public IosStoreLangParser(String category, String str) {
        this.category = category;
        SetPriceAndLangs(str);
    }

    public String getCategory() {
        return category;
    }

    public boolean isAvgPrice() {
        return isAvgPrice;
    }

    public double getPrice() {
        return price;
    }

    public ArrayList<String> getLangs() {
        return langs;
    }

    public void SetPriceAndLangs(String str) {

        try {
            ArrayList<String> langs = new ArrayList<>();
            String[] strLangs = str.split(RegexEnum.UNDERSCORE_REGEX.getStr());

            if(strLangs[1].equals("CONVERTER") || strLangs[1].equals("CONVERTER_ERROR")) {
                this.price = 0.0;
                this.category = "BAD";
            }

            if(!strLangs[0].equals("AVG")) {
                this.price = Double.parseDouble(strLangs[0]);
                this.isAvgPrice = false;

                for(int i = 1; i <= strLangs.length-1; i++) {
                    this.langs.add(strLangs[i]);
                }
            } else {
                this.isAvgPrice = true;
                this.price = Double.parseDouble(strLangs[1]);
            }
        } catch (Exception e) {
            e.fillInStackTrace();
            System.err.println(e.getMessage());
            e.printStackTrace();
        }
    }
}
