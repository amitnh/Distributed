import java.io.IOException;
import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.model.*;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.sqs.model.*;

public class AmirLocal {
    private static final Logger LOGGER_LocalApplication = LoggerFactory.getLogger(localApplication.class);

    private static int n = 0;

    public static void main(String args[]) throws IOException {

        final String NAME = UUID.randomUUID().toString(); //Generating universally unique IDs
        String inputFileName = "";
        String outputFileName = "";
        String terminateMsg = "";
        if (args.length == 4 || args.length == 3) {
            inputFileName = args[0];
            outputFileName = args[1];
            n = Integer.parseInt(args[2]);
            if (args.length == 4)
                terminateMsg = args[3];
        } else
            System.out.println("Wrong arguments");
        LOGGER_LocalApplication.info("Create connection with amazon");
        CommonConstants.createElements();

        String file_path = "bin/" + inputFileName + ".txt";
        String folderName = "User-" + NAME;

        // 1. Upload the file with the commands & pdf links to s3
        CommonConstants.s3.putObject(PutObjectRequest.builder()
                        .bucket(CommonConstants.BUCKET_NAME)
                        .key(folderName + CommonConstants.SUFFIX + "inputFile_(inside).txt")
                        .build(),
                RequestBody.fromFile(new File(file_path)));

        String inputFileS3 = URLEncoder.encode(CommonConstants.BUCKET_NAME + CommonConstants.SUFFIX + folderName + CommonConstants.SUFFIX + "inputFile_(inside).txt", StandardCharsets.UTF_8.toString());

        if(!isManagerExists()) {
            LOGGER_LocalApplication.info("Starting manager...");
            startManager();
        }
        else
            LOGGER_LocalApplication.info("Manager is already exist");

        // 2. Send to the queue the location of input file in S3
        newTask_Send_LAMQueue(inputFileS3, NAME, terminateMsg);

        /*---------------------------------------------------------------------------
         * --------------------------------------------------------------------------
         * -----------      IN THIS MOMENT, THE MANAGER SENDS RESULTS      ----------
         * --------------------------------------------------------------------------
         * --------------------------------------------------------------------------
         * */
        while (doneTask_Receive_MLAQueue().isEmpty());
        LOGGER_LocalApplication.info("Message received from manager");
        Message finished = doneTask_Receive_MLAQueue().get(0);
        String[] details = decodeDoneTask(finished);

        /* Must delete MLA message in the end */
        CommonConstants.sqsMLA.deleteMessage(DeleteMessageRequest.builder()
                .queueUrl(CommonConstants.sqsUrl(CommonConstants.QUEUE_NAME_MLA))
                .receiptHandle(finished.receiptHandle())
                .build());

        /* 15+16. Local application downloads summary file */
        downloadFileS3(details[1], outputFileName);
        LOGGER_LocalApplication.info("finished!");
    }

    private static void startManager() {
        IamInstanceProfileSpecification role = IamInstanceProfileSpecification.builder()
                .name("amirRole")
                .build();
        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId("ami-076515f20540e6e0b")
                .instanceType(InstanceType.T2_MICRO)
                .maxCount(1)
                .minCount(1)
                .userData(getManagerDataScript(n))
                .iamInstanceProfile(role)
                .build();
        RunInstancesResponse buildManagerResponse = CommonConstants.ec2.runInstances(runRequest);
        String instanceId = buildManagerResponse.instances().get(0).instanceId();
        // Now we will add the manager a tag
        Tag tag = Tag.builder()
                .key("Name")
                .value("Manager Instance")
                .build();
        CreateTagsRequest tagsRequest = CreateTagsRequest.builder()
                .resources(instanceId)
                .tags(tag)
                .build();
        CommonConstants.ec2.createTags(tagsRequest);
    }

    private static String getManagerDataScript(int n) {
        ArrayList<String> lines = new ArrayList<String>();
        lines.add("#! /bin/bash");
        lines.add("wget https://dsps12020-sss3.s3.amazonaws.com/manager.jar");
        lines.add("java -jar manager.jar " + n);
        return Base64.getEncoder().encodeToString(join(lines, "\n").getBytes());
    }

    private static String join(Collection<String> s, String delimiter) {
        StringBuilder builder = new StringBuilder();
        Iterator<String> iter = s.iterator();
        while (iter.hasNext()) {
            builder.append(iter.next());
            if (!iter.hasNext())
                break;
            builder.append(delimiter);
        }
        return builder.toString();
    }

    private static String[] decodeDoneTask(Message managerMsg) throws UnsupportedEncodingException {
        String s1 = managerMsg.body();
        String s2 = s1.split("done task ", 2)[1];
        String s3 = URLDecoder.decode(s2, StandardCharsets.UTF_8.toString());
        String[] s4 = s3.split(CommonConstants.SUFFIX, 2);
        return s4;
    }

    private static void downloadFileS3(String key_name, String output_of_program) {
        // key_name = User-.../finalSummary.html
        CommonConstants.s3.getObject(GetObjectRequest.builder()
                        .bucket(CommonConstants.BUCKET_NAME)
                        .key(key_name)
                        .build(),
                ResponseTransformer.toFile(new File("bin/" + output_of_program + ".html")));
    }

    private static void newTask_Send_LAMQueue(String inputFileS3, String name, String terminate) {
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<String, MessageAttributeValue>();
        MessageAttributeValue nameAttribute = MessageAttributeValue.builder()
                .dataType("String")
                .stringValue(name)
                .build();
        messageAttributes.put("name", nameAttribute);
        if (terminate == "") {
            MessageAttributeValue terminateAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue("null")
                    .build();
            messageAttributes.put("terminate", terminateAttribute);
        } else {
            MessageAttributeValue terminateAttribute = MessageAttributeValue.builder()
                    .dataType("String")
                    .stringValue(terminate)
                    .build();
            messageAttributes.put("terminate", terminateAttribute);
        }

        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(CommonConstants.sqsUrl(CommonConstants.QUEUE_NAME_LAM))
                .messageBody("new task " + inputFileS3)
                .messageAttributes(messageAttributes)
                .build();
        CommonConstants.sqsLAM.sendMessage(send_msg_request);
    }

    private static List<Message> doneTask_Receive_MLAQueue() {
        ReceiveMessageRequest receive_msg_request = ReceiveMessageRequest.builder()
                .queueUrl(CommonConstants.sqsUrl(CommonConstants.QUEUE_NAME_MLA))
                .messageAttributeNames("name")
                .maxNumberOfMessages(1)
                .build();

        return CommonConstants.sqsMLA.receiveMessage(receive_msg_request).messages();
    }

    private static boolean isManagerExists() {
        DescribeInstancesRequest managerRequest = DescribeInstancesRequest.builder().build();
        DescribeInstancesResponse managerResponse = CommonConstants.ec2.describeInstances(managerRequest);
        for (Reservation reservation : managerResponse.reservations())
            for (Instance instance : reservation.instances())
                for (Tag tag : instance.tags())
                    if (tag.value().substring(0,7).equals("Manager") && (instance.state().code() == CommonConstants.running || instance.state().code() == CommonConstants.pending))
                        return true;
        return false;
    }
}
