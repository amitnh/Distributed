import java.io.File;
import java.nio.file.Paths;
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

    private static SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
    private static S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
    private static String bucket_name = "bucket-amitandtal";
    private static long myID;
    private static int jobsCounter;
    private static int terminated=0;
    public static String access_key_id = "";
    public static String secret_access_key_id = "";
    private static AwsBasicCredentials awsCreds = AwsBasicCredentials.create(access_key_id, secret_access_key_id);
    private static String sqsManagerToLocal;
    private static String sqsLocalsToManager = "sqsLocalsToManager";
    private static String sqsTesting = "sqsTesting";

    //args[] = [inputfilename1, ..., inputfilenameN, outputfilename1,..., outputfilenameN, n, terminate]
    public static void main(String[] args) {

        int numOfFiles = (args.length-2)/2;
        terminated = Integer.parseInt(args[args.length - 1]);
        //myID is the local's ID for the manager to use
        myID = System.currentTimeMillis();
        sqsManagerToLocal = "sqsManagerToLocal-"+myID;
        //check if theres an instance running with TAG-MANAGER AKA big bo$$
        //if there is no manager running, run a new instance of a manager, and create an SQS queueueue
        if(managerOnline()) {
            // im the first local! :)
            OpenS3();       //open a new bucket, and upload manager and workers JAR files
            runManager();   //create a new instance of a manager

            OpenSQS(sqsLocalsToManager);      //this SQS is for ALL locals to upload jobs for the manager

            //manager is now online and ready for jobs
        }
        OpenSQS(sqsTesting);      //this SQS is for ALL locals to upload jobs for the manager

        OpenSQS(sqsManagerToLocal);  //this SQS is for this local ONLY for messages about finished jobs from manager.

        //upload file from local folder to S3, receive a URL for the manager to use later
        //        upload_to_s3(args[0]);
        for(int i=0 ; i< numOfFiles; i++) {
            String key = args[i];
            uploadToS3("../../../Input files/"+key, key);
            //pushing job to SQS as a URL for the uploaded file
            //arguments  -> [address, jobOwner, outputFileName,n,[terminating]]
            String[] arguments = {key, String.valueOf(myID), args[numOfFiles+i], args[args.length - 2],String.valueOf(terminated)};
            pushSQS("sqsLocalsToManager", arguments);
            // manager is now able to take the job from SQS and process it. check the finished_SQS for massages of finished jobs.
        }
        //wait for all jobs to be ready at finished_SQS
        finish();
    }

    private static void OpenS3() {
        S3Client s3Client;
        //open bucket
        Region region = Region.US_EAST_1;

        s3Client = S3Client.builder()
//				.credentialsProvider(StaticCredentialsProvider.create(credentials))
                .region(region)
                .build();

        s3Client.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket_name)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
//						.locationConstraint(region.id())
                                .build())
                .build());
        System.out.println("Bucket created: " + bucket_name);
    }

    //create an SQS named SQS_name
    private static void OpenSQS(String SQS_name) {
        qsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(SQS_name)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }

    }


    //manager online returns id for local manager.  returned id=0 means that this local is the first local. next
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

        // check the SQS for massages of finished jobs. /TODO change from busy wait to somthing smarter..
        while(true){
            Result result= popSQS(sqsManagerToLocal);
            String testMSG= popSQS(sqsTesting);
            System.out.println("Result: "+result);
            if(testMSG == "teminate")
                break;
        }
        System.out.println("finished");
        if (terminated==1){
            deleteBucket();
        }
        //TODO makeHtmlFile(Result);
    }

    private static void pushSQS(String SQS_name,String[] arguments) {
        String queueUrl = getSQSUrl(SQS_name);
        //send the msgs
        for (int j=0;j<arguments.length;j++){
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("" + arguments[j])
                    .delaySeconds(5)// Todo: remove ?
                    .build();
            sqs.sendMessage(send_msg_request);
        }
    }
    private static List<Message> popSQS(String SQS_name) {
        String queueUrl = getSQSUrl(SQS_name);
        // receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .build();
        return sqs.receiveMessage(receiveRequest).messages();
    }



    public static void deletefromSQS(String SQS_name,List<Message> msgs)  {
        String queueUrl = getSQSUrl(SQS_name);
        // delete messages from the queue
        for (Message m : msgs) {
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(m.receiptHandle())
                    .build();
            sqs.deleteMessage(deleteRequest);
        }
    }

    private static String getSQSUrl(String SQS_name) {
        //gets the URL
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(SQS_name)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

        public static void uploadToS3(String path, String key)  {
        s3Client.putObject(PutObjectRequest.builder()
                        .bucket(bucket_name)
                        .key(key)
                        .build(),
                RequestBody.fromFile(new File(path)));
        System.out.println("File uploaded : " + key);
    }
    public static void deleteBucket() {
        // -----------------empties the bucket-----------------
        // Get a list of all the files in the bucket
        ListObjectsV2Request listReq = ListObjectsV2Request.builder()
                .bucket(bucket_name)
//                .maxKeys(1)
                .build();

        ListObjectsV2Iterable listRes = s3Client.listObjectsV2Paginator(listReq);
        for (S3Object content : listRes.contents()) {
            DeleteObjectRequest deleteObjectRequest = DeleteObjectRequest.builder().bucket(bucket_name).key(content.key()).build();
            s3Client.deleteObject(deleteObjectRequest);
            System.out.println("File deleted:\tKey: " + content.key() + "\tsize = " + content.size());
        //------deletes the bucket--------------------
        DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket_name).build();
        s3Client.deleteBucket(deleteBucketRequest);

        System.out.println("Bucket deleted: " + bucket_name);
    }

    }
    public static void downloadFile(String key, String destination) {
        s3Client.getObject(GetObjectRequest.builder().bucket(bucket_name).key(key).build(),
                ResponseTransformer.toFile(Paths.get(destination)));
        System.out.println("File downloaded: " + key);
    }

}
