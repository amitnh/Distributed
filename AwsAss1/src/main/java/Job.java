import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Job{

    public String jobOwner;
    public int jobID;
    public String title;
    public List<Review> reviews;
    public Result[] results;
    public String outputFileName;
    public AtomicInteger remainingResponses;

    public Job(String jobOwner, int jobID, String title, List<Review> reviews,  String outputFileName) {
        this.jobOwner = jobOwner;
        this.jobID = jobID;
        this.title = title;
        this.reviews = reviews;
        this.remainingResponses = new AtomicInteger(reviews.size());
        this.results = new Result[remainingResponses.get()];
        this.outputFileName = outputFileName;
    }


    public String getJobOwner() {
        return jobOwner;
    }

    public void setJobOwner(String jobOwner) {
        this.jobOwner = jobOwner;
    }

    public int getJobID() {
        return jobID;
    }

    public void setJobID(int jobID) {
        this.jobID = jobID;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public List<Review> getReviews() {
        return reviews;
    }

    public void setReviews(List<Review> reviews) {
        this.reviews = reviews;
    }

    public Result[] getResults() {
        return results;
    }

    public void setResults(Result[] results) {
        this.results = results;
    }

    public String getOutputFileName() {
        return outputFileName;
    }

    public void setOutputFileName(String outputFileName) {
        this.outputFileName = outputFileName;
    }

    public AtomicInteger getRemainingResponses() {
        return remainingResponses;
    }

    public void setRemainingResponses(AtomicInteger remainingResponses) {
        this.remainingResponses = remainingResponses;
    }
    public int decrementRemainingResponses() {
        return this.remainingResponses.decrementAndGet();
    }




        public boolean isequal(Job otherJob) {
            return (this.jobOwner.equals(otherJob.jobOwner))&&(this.outputFileName.equals(otherJob.outputFileName));
        }
    }
