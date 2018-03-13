package feilongbk.earlybirds.technicaltest;

public class UserProductPair {
    int intUserId;
    int intProductId;


    public UserProductPair(int intUserId, int intProductId) {
        this.intUserId = intUserId;
        this.intProductId = intProductId;
    }


    public int getIntUserId() {
        return intUserId;
    }

    public void setIntUserId(int intUserId) {
        this.intUserId = intUserId;
    }

    public int getIntProductId() {
        return intProductId;
    }

    public void setIntProductId(int intProductId) {
        this.intProductId = intProductId;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        UserProductPair that = (UserProductPair) o;

        if (intUserId != that.intUserId) return false;
        return intProductId == that.intProductId;
    }

    @Override
    public int hashCode() {
        int result = intUserId;
        result = 31 * result + intProductId;
        return result;
    }
}
