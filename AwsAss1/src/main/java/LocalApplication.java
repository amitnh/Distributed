import java.io.File;
import java.nio.file.Paths;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;


public class LocalApplication {

    private static S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
    private static String bucket_name = "bucket-amitandtal";
    private static int myID;
    //args[] = [inputfilename1, ..., inputfilenameN, outputfilename1,..., outputfilenameN, n, terminate]
    public static void main(String[] args) {
        int numOfFiles = (args.length-2)/2;

        //check if theres an instance running with TAG-MANAGER AKA big bo$$
        //if there is no manager running, run a new instance of a manager, and create an SQS queueueue
        myID = managerOnline(); //if there is no manager online, myID will be 1.
        if(myID == 1) {// im the first local! :)
            OpenSQS("sqsLocalsToManager");
            OpenS3();
            runManager();
        }
        OpenSQS("sqsManagerToLocal"+myID);

        //upload file from local folder to S3, receive a URL for the manager to use later
        //        upload_to_s3(args[0]);
        for(int i=0 ; i< numOfFiles; i++) {
            String key = args[i];
            uploadToS3("../../../Input files/0689835604.txt", key);
            //pushing job to SQS as a URL for the uploaded file
            String[] arguments = {key, String.valueOf(myID), args[numOfFiles+i], args[args.length - 2]};
            pushSQS("sqsLocalsToManager", arguments);
            // manager is now able to take the job from SQS and process it. check the finished_SQS for massages of finished jobs.
        }
        //wait for all jobs to be ready at finished_SQS
        finish();
    }

    //create an SQS named SQS_name
    private static void OpenSQS(String SQS_name) {
    }

    //create an SQS named "sqsLocalsToManager", for ALL locals jobs
    private static void OpenSQSWithManajer(String jobsSQS) {
    }

    private static boolean managerOnline() {// by tag
        return false;
    }
    private static void runManager() {
        //open instance
        // start manager code
        // take from Amir
        // add manager tag

    }


    private static void finish() {
        // check the SQS for massages of finished jobs. (Sleep or somthing)
        //Result= checkSQS();
        System.out.println("finished");

        //makeHtmlFile(Result);
    }

    private static void pushSQS(String SQS_name,String[] arguments) {
        //push arguments to SQS_name
    }

    public static void uploadToS3(String path, String key)  {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket_name)
                        .key(key)
                        .build(),
                RequestBody.fromFile(new File(path)));
        System.out.println("File uploaded : " + key);
    }

}
