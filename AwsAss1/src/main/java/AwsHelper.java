import com.google.gson.Gson;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;

public class AwsHelper {
    public static  Gson gson = new Gson();
    public static SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
    public static S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
    public static String bucket_name = "bucket-amitandtal";
    public static int NumOfRetriveMSGs = 1;
    public static void OpenS3() {
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
    public static void OpenSQS(String SQS_name) {
        SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();

        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(SQS_name)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }

    }
    public static void pushSQS(String SQS_name,List<Message> msgs) {
        String queueUrl = getSQSUrl(SQS_name);
        //send the msgs
        for (Message m : msgs) {
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody("" + m)
                    .delaySeconds(5)// Todo: remove ?
                    .build();
            sqs.sendMessage(send_msg_request);
        }
    }
    public static List<Message> popSQS(String SQS_name) {
        String queueUrl = getSQSUrl(SQS_name);
        // receive messages from the queue
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(NumOfRetriveMSGs)  //TODO maybe more than one message per pop
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

    public static String getSQSUrl(String SQS_name) {
        //gets the URL
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(SQS_name)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
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



    //=============================================================================
    //Json Helpers-
    //=============================================================================

    //cast message to T. for example, cast message to Review
    public static <T> T fromMSG(Message m,Class<T> c){
        return gson.fromJson(m.body(), c);
    }

    public static <T> List<T> fromMSG(List<Message> Messages,Class<T> c){
        List<T> tList = new LinkedList<>();
        for(Message m: Messages)
            tList.add(fromMSG(m, c));
        return tList;
    }

    //cast object T to Message. for example, cast Review to message
    public static <T> Message toMSG(T t){
        String body =  gson.toJson(t);
        return Message.builder()
                .body(body)
                .build();
    }

    public static <T> List<Message> toMSG(List<T> tList){
        List<Message> mList = new LinkedList<>();
        for(T t: tList)
            mList.add(toMSG(t));
        return mList;
    }
}
