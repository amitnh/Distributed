import java.util.List;

public class Job {

    protected String jobOwner;
    protected int jobID;
    protected String title;
    protected List<Review> reviews;
    protected Result[] results;
    protected String outputFileName;
    protected int remainingResponses;

    public Job(String jobOwner, int jobID, String title, List<Review> reviews,  String outputFileName) {
        this.jobOwner = jobOwner;
        this.jobID = jobID;
        this.title = title;
        this.reviews = reviews;
        this.remainingResponses = reviews.size();
        this.results = new Result[remainingResponses];
        this.outputFileName = outputFileName;
    }



}
