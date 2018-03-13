package feilongbk.earlybirds.technicaltest;

public class InputRow {
    String userId;
    String productId;
    float rating;
    long timeStamp;

    public InputRow(String userId, String productId, float rating, long timeStamp) {
        this.userId = userId;
        this.productId = productId;
        this.rating = rating;
        this.timeStamp = timeStamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public float getRating() {
        return rating;
    }

    public void setRating(float rating) {
        this.rating = rating;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(long timeStamp) {
        this.timeStamp = timeStamp;
    }
}
