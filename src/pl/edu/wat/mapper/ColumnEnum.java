package pl.edu.wat.mapper;

public enum ColumnEnum {
    PRICE_COLUMN_NUMBER(12), CATEGORY_COLUMN_NUMBER(13), LANGUAGE_COLUMN_NUMBER(15);
    private int val;

    private ColumnEnum(int val) {
        this.val = val;
    }

    public int getVal() {
        return val;
    }
}
