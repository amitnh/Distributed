
public class Job {

    protected String jobOwner;
    protected int jobID;
    protected String title;
    protected Review[] reviews;
    protected Result[] results;
    protected String outputFileName;
    protected int remainingResponses;

    public Job(String jobOwner, int jobID, String title, Review[] reviews, Result[] results, String outputFileName) {
        this.jobOwner = jobOwner;
        this.jobID = jobID;
        this.title = title;
        this.reviews = reviews;
        this.remainingResponses = results.length;
        this.results = new Result[remainingResponses];
        this.outputFileName = outputFileName;
    }



}
