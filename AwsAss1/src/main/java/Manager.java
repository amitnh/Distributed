import java.util.Base64;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.InstanceType;
import software.amazon.awssdk.services.ec2.model.RunInstancesRequest;
import software.amazon.awssdk.services.ec2.model.RunInstancesResponse;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.ec2.model.CreateTagsRequest;
import software.amazon.awssdk.services.ec2.model.Ec2Exception;

public class Manager implements Runnable{
    protected Job[] jobs;
    static int numOfCurrWorkers=0;
    static String responesAdd;
    static int terminated = 0;
    static int counter = 0;

    public static void main(String[] args) {
        while(true) { // maybe sleep ?
            String address = checkLocalAppSqs(); // check if SQS Queue has new msgs for me
            if (address != null && terminated !=1) { // if terminated dont add new Files, but still finish what he got so far
                String inputfile = downloadFileFromS3(address); // maybe not String
                Msg[] msgs = parse(inputfile);
                counter+=msgs.size();
                sendToWorkersSqs(msgs);
                createNewWorkers(sgs.size(),numOfCurrWorkers); // if needed adds new worker instances
            }

            /// maybe another Thread
            String response = checkRespondSqs();
            if (response != null) {
                bool isDuplicate = saveRespondToS3(response); // if not duplicated
                if (!isDuplicate){
                    counter--;
                }
            }

            if (counter == 0){ // finished, all msgs returned(how? maybe a counter  )
                sendToLocalAppSQS(responesAdd);
                if (terminated==1){
                    terminateAllWorkers();
                    createResponseMsg();// not sure
                    terminate();
                }
            }


        }

    }

    @Override
    public void run() {

    }
}
