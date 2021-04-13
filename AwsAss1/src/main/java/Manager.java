import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;
import java.util.*;
import java.util.Base64;

public class Manager implements Runnable{
    static  Map<Integer, Job> jobs = new HashMap<>();
    static int numOfCurrWorkers=0;
    static int terminated = 0;
    static int nextJobID = 0;


    public static void main(String[] args) {

        final int n = Integer.parseInt(args[0]); // getting it throgh the localApplication args

        Manager responseThread = new Manager();
        Thread thread = new Thread(responseThread);
        thread.start();

        while(true) { // maybe sleep?
            String address = checkLocalAppSqs(); // check if SQS Queue has new msgs for me
            if (address != null && terminated !=1) { // if terminated dont add new Files, but still finish what he got so far
                Job job = downloadAndPharse(address,nextJobID);// jobs contains his JobID
                jobs.put(nextJobID++,job); // adds the Job to the jobs Map

                numOfCurrWorkers = createNewWorkers(job.reviews.length,numOfCurrWorkers); // if needed adds new worker instances, checks with SQS size
                sendReviewsToWorkersSqs(job);
            }
        }

    }

    @Override
    public void run() {
        while (true) {
            /// maybe another Thread
            Result result = checkResultsSqs();
            if (result != null) {
                int finishedJobID = saveResult(result); // if not duplicated, and if Reviews.length=Results.length returns jobID else return -1
            }

            if (finishedJobID != -1) {  // finished
                string address = uploadResultsToS3(finishedJobID);
                sendToLocalAppSQS(address); // updating the localapp sqs for the new result
                if (terminated == 1) {
                    terminateAllWorkers(); //numOfCurrWorkers
                    createResponseMsg();// not sure what that means
                    terminate();
                }
            }
        }
    }
}
