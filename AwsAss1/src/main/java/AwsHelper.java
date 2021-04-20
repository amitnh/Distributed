import com.google.gson.Gson;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedList;
import java.util.List;

public class AwsHelper {
    public static String amiId = "ami-0009c3f63fca71e34";//got java8
    public static  Gson gson = new Gson();
    public static SqsClient sqs = SqsClient.builder().region(Region.US_EAST_1).build();
    public static S3Client s3Client = S3Client.builder().region(Region.US_EAST_1).build();
    public static Ec2Client ec2 = Ec2Client.builder().region(Region.US_EAST_1).build();

    public static int protection=0; //todo remove later
    public static int maxNumOfInstances=5; //todo remove later

    public static String bucket_name = "bucket-amitandtal1";
    public static int NumOfRetriveMSGs = 1;
    public static String sqsTesting = "sqsTesting";
    public static String sqsLocalsToManager = "sqsLocalsToManager";

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
    public static void pushSQS(String SQS_name,String str) {
        String queueUrl = getSQSUrl(SQS_name);
        //send the msgs
            SendMessageRequest send_msg_request = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageBody(str)
                    .delaySeconds(5)// Todo: remove ?
                    .build();
            sqs.sendMessage(send_msg_request);

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
                        .acl(ObjectCannedACL.PUBLIC_READ_WRITE)
                        .build(),
                RequestBody.fromFile(new File(path)));
        System.out.println("File uploaded : " + key);
    }
    public static boolean doesFileExists(String key) {
        try {
            s3Client.getObject(GetObjectRequest.builder().bucket(bucket_name).key(key).build(),
                    ResponseTransformer.toFile(Paths.get(key)));
        } catch (Exception e) {
            return false;
            }
        return true;
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
        }
            //------deletes the bucket--------------------
            DeleteBucketRequest deleteBucketRequest = DeleteBucketRequest.builder().bucket(bucket_name).build();
            s3Client.deleteBucket(deleteBucketRequest);

            System.out.println("Bucket deleted: " + bucket_name);



    }
    public static void downloadFile(String key, String destination) {
        s3Client.getObject(GetObjectRequest.builder().bucket(bucket_name).key(key).build(),
                ResponseTransformer.toFile(Paths.get(destination)));
        System.out.println("File downloaded: " + key);
    }

    //=============================================================================
    //instance- start an instance and run jar file on S3 bucket
    //=============================================================================

    public static void startInstance(String nameTag,String jarAddress) {
        AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n startInstance. nameTag:" +nameTag); // todo delete

        if(++protection>maxNumOfInstances) {
            return;
        }
        try {
            IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder()
                    .name("EMR_EC2_DefaultRole") //arn:aws:iam::824286680564:role/EMR_EC2_DefaultRole
                    .build();
            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(InstanceType.T2_SMALL)
                    .maxCount(1)
                    .minCount(1)
                    .userData(getDataScript(jarAddress))
                    .iamInstanceProfile(role)
                    .keyName("amital")
                    .securityGroupIds("sg-7e7c937d")//,sg-5422235a
                    .build();
            RunInstancesResponse buildManagerResponse = ec2.runInstances(runRequest);
            String instanceId = buildManagerResponse.instances().get(0).instanceId();

            // Now we will add a tag
            Tag tag = Tag.builder().key("Name").value(nameTag).build();

            CreateTagsRequest tagsRequest = CreateTagsRequest.builder()
                    .resources(instanceId)
                    .tags(tag)
                    .build();
            ec2.createTags(tagsRequest);
        }
        catch(Exception e){
            AwsHelper.pushSQS(AwsHelper.sqsTesting,"\n startInstance. Error:" +e); // todo delete
        }

    }
    private static String getDataScript(String file) {
        String str = "";
        str+="#! /bin/bash\n";
        str+="wget https://" +bucket_name +".s3.amazonaws.com/"+ file + "\n";// todo change the s3address
        str+="java -jar -Xmx550m " + file + "\n";
        return Base64.getEncoder().encodeToString(str.getBytes());

    }

    public static boolean isManagerOnline() {
        DescribeInstancesRequest managerRequest = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse managerResponse = ec2.describeInstances(managerRequest);
        for (Reservation reservation : managerResponse.reservations())
            for (Instance instance : reservation.instances())
                for (Tag tag : instance.tags())
                    if (tag.value().startsWith("Manager")) // todo check
                        return true;
        return false;
    }
    public static void terminateInstancesByTag(String tagName) {
        DescribeInstancesRequest request = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse response = ec2.describeInstances(request);
        for (Reservation reservation : response.reservations()) {
            for (Instance instance : reservation.instances()) {
                for (Tag tag : instance.tags()) {
                    if ((tag.value().startsWith(tagName))) { // todo check
                        List<String> instanceIds = new LinkedList<>();
                        instanceIds.add(instance.instanceId());
                        ec2.terminateInstances(TerminateInstancesRequest.builder()
                                .instanceIds(instanceIds)
                                .build());
                    }
                }
            }
        }
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
