
public class MeasurementResult {

    private IndexResponse.LookupItem lookupItem;

    private long callLookUpStartTime;
    private long callLookUpStopTime;
    private long callLookUpElapsedTime;

    private long downloadStartTime;
    private long downloadStopTime;

    private long downloadElapsedTime;


    public IndexResponse.LookupItem getLookupItem() {
        return lookupItem;
    }

    public void setLookupItem(IndexResponse.LookupItem lookupItem) {
        this.lookupItem = lookupItem;
    }

    public long getCallLookUpStartTime() {
        return callLookUpStartTime;
    }

    public void setCallLookUpStartTime(long callLookUpStartTime) {
        this.callLookUpStartTime = callLookUpStartTime;
    }

    public long getCallLookUpStopTime() {
        return callLookUpStopTime;
    }

    public void setCallLookUpStopTime(long callLookUpStopTime) {
        this.callLookUpStopTime = callLookUpStopTime;
    }

    public long getCallLookUpElapsedTime() {
        return callLookUpElapsedTime;
    }

    public void setCallLookUpElapsedTime(long callLookUpElapsedTime) {
        this.callLookUpElapsedTime = callLookUpElapsedTime;
    }

    public long getDownloadStartTime() {
        return downloadStartTime;
    }

    public void setDownloadStartTime(long downloadStartTime) {
        this.downloadStartTime = downloadStartTime;
    }

    public long getDownloadStopTime() {
        return downloadStopTime;
    }

    public void setDownloadStopTime(long downloadStopTime) {
        this.downloadStopTime = downloadStopTime;
    }

    public long getDownloadElapsedTime() {
        return downloadElapsedTime;
    }

    public void setDownloadElapsedTime(long downloadElapsedTime) {
        this.downloadElapsedTime = downloadElapsedTime;
    }
}
