
public class Job {

    protected int jobID;
    protected String title;
    protected Review[] reviews;
    protected Result[] results;
    protected String outputFileName;
    protected int remainingResponse;



    public Job(String title, Review[] reviews) {
        this.title = title;
        this.reviews = reviews;
        this.remainingResponse = results.length;
    }


}
