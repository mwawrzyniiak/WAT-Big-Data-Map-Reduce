package pl.edu.wat.mapper;

public enum RegexEnum {
    QUOTES_REGEX(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"), TAB_REGEX("\t"), UNDERSCORE_REGEX("_");

    private String str;

    RegexEnum(String str) {
        this.str = str;
    }

    public String getStr() {
        return str;
    }
}
