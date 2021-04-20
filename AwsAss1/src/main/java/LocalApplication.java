import java.io.File;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
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
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;


public class LocalApplication {


    private static long myID;
    private static int jobsCounter=-1;
    private static int terminated=0;
    public static String access_key_id = "";
    public static String secret_access_key_id = "";
    //private static AwsBasicCredentials awsCreds = AwsBasicCredentials.create(access_key_id, secret_access_key_id);
    private static String sqsManagerToLocal;
    private static String sqsLocalsToManager = "sqsLocalsToManager";
    private static String sqsTesting = "sqsTesting";

    //args[] = [inputfilename1, ..., inputfilenameN, outputfilename1,..., outputfilenameN, n, terminate]
    public static void main(String[] args) {
        System.out.println("Local Main");
        //AwsHelper.OpenSQS(sqsTesting);      //TODO remove, this SQS is for ALL locals to upload jobs for the manager

        int numOfFiles = (args.length-2)/2;
        jobsCounter=numOfFiles;
        //myID is the local's ID for the manager to use
        myID = System.currentTimeMillis();
        sqsManagerToLocal = "sqsManagerToLocal-"+myID;
        //check if theres an instance running with TAG-MANAGER AKA big bo$$
        //if there is no manager running, run a new instance of a manager, and create an SQS queueueue
        if(!AwsHelper.isManagerOnline()) {
            System.out.println("Manager not Online");// todo delete

            // im the first local! :)
            AwsHelper.OpenS3();       //open a new bucket, and upload manager and workers JAR files
            AwsHelper.uploadToS3("../Manager/AwsAss1.jar","Manager.jar");
            AwsHelper.uploadToS3("../Worker/AwsAss1.jar","Worker.jar");
            runManager();   //create a new instance of a manager

            AwsHelper.OpenSQS(sqsLocalsToManager);      //this SQS is for ALL locals to upload jobs for the manager
            //manager is now online and ready for jobs
        }
        AwsHelper.OpenSQS(sqsManagerToLocal);  //this SQS is for this local ONLY for messages about finished jobs from manager.
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\ntesting sqsTesting");// todo delete


        //upload file from local folder to S3, receive a URL for the manager to use later
        //        upload_to_s3(args[0]);
        for(int i=0 ; i< numOfFiles; i++) {
            String key = args[i];
            AwsHelper.uploadToS3("../Input files/"+key, key);

            //pushing job to SQS as a URL for the uploaded file
            //arguments  -> [address, jobOwner, outputFileName,n,[terminating]]
            if (numOfFiles-1 == i)
                terminated = Integer.parseInt(args[args.length - 1]); // only in the last file
            else
                terminated=0;

            String[] arguments = {key, String.valueOf(myID), args[numOfFiles+i], args[args.length - 2],String.valueOf(terminated)};
            List<Message> list = new LinkedList<>();
            list.add(AwsHelper.toMSG(arguments));

            AwsHelper.pushSQS(sqsLocalsToManager, list);
            // manager is now able to take the job from SQS and process it. check the finished_SQS for massages of finished jobs.
        }
        //wait for all jobs to be ready at finished_SQS
        finish();
    }



    private static void runManager() {
        System.out.println("running manager");

        AwsHelper.startInstance("Manager","Manager.jar");

    }


    private static void finish() {

        // check the SQS for massages of finished jobs. /TODO change from busy wait to somthing smarter..
        while(true){
            //Check SQS - sqsManagerToLocal for finished jobs(results)
            List<Message> resultsMsgs = CheckFinishedJobs();
            List<Result> results = AwsHelper.fromMSG(resultsMsgs,Result.class);
            //TODO only for testing, remove before flight
            List<Message> testlist = AwsHelper.popSQS(sqsTesting);

            //List<String> testMSG= AwsHelper.fromMSG(testlist,String.class);
            if (!testlist.isEmpty()) {
                String testMSG = testlist.get(0).body();
                System.out.println("testMSGs: " + testMSG);//.get(0)
            }
            if (!results.isEmpty())
                System.out.println("Results sentiment:"+results.get(0).sentiment + "\nentities: " + results.get(0).entities);


            AwsHelper.deletefromSQS("sqsTesting",testlist);
            AwsHelper.deletefromSQS(sqsManagerToLocal,resultsMsgs);

            if(jobsCounter==0)
                break;
        }
        System.out.println("finished");
        if (terminated==1){
            AwsHelper.deleteBucket();
        }
        //TODO makeHtmlFile(Result);

    }

    private static List<Message> CheckFinishedJobs() {
        List<Message> results = AwsHelper.popSQS(sqsManagerToLocal);
        if(results.size()>0)
            jobsCounter-= results.size();
        return results;
    }


}
