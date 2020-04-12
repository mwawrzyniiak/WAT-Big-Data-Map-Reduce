package pl.edu.wat.parser;

public class LangParser implements Comparable {
    private String lang;
    private int counts;

    public LangParser(String lang, int counts) {
        this.lang = lang;
        this.counts = counts;
    }

    public String getLang() {
        return lang;
    }

    public int getCounts() {
        return counts;
    }

    @Override
    public int compareTo(Object o) {
        int val = ((LangParser)o).getCounts();
        return this.counts-val;
    }
}
