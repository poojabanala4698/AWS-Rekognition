package com.amazonaws.samples;

import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.rekognition.model.DetectLabelsRequest;
import com.amazonaws.services.rekognition.model.DetectLabelsResult;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.Label;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.SendMessageRequest;

import java.io.IOException;

public class GetLabels {

    public static void main(String[] args) throws IOException {
        
    	Regions clientRegion = Regions.US_EAST_1;
        String bucketName = "njit-cs-643";
        String images_list[] = new String[10];
        int i=1;

        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withCredentials(new ProfileCredentialsProvider())
                    .withRegion(clientRegion)
                    .build();


            ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(bucketName).withMaxKeys(2);
            ListObjectsV2Result result;

            do {
                result = s3Client.listObjectsV2(req);

                for (S3ObjectSummary objectSummary : result.getObjectSummaries()) {
                	if(i<=10) {
                		images_list[i-1] = objectSummary.getKey();
                		i = i+1;
                		}
                }
                
                String token = result.getNextContinuationToken();
                req.setContinuationToken(token);
            } while (result.isTruncated());
        } catch (AmazonServiceException e) {
            
            e.printStackTrace();
        } catch (SdkClientException e) {
            
            e.printStackTrace();
        }
        
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard().withRegion("us-east-1").build();
        final AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion("us-east-1").build();
        
// AWS Rekognition
        for (String photo : images_list) {
        	
        	
        	DetectLabelsRequest request = new DetectLabelsRequest()
                .withImage(new Image().withS3Object(new S3Object().withName(photo).withBucket(bucketName)))
                .withMinConfidence(90F);

        try {
            DetectLabelsResult result = rekognitionClient.detectLabels(request);
            List<Label> labels = result.getLabels();
          
            for (Label label : labels) {
            
            	if(label.getName().equals("Car")) {
            		System.out.println(" \n "+photo);
            		System.out.println("Car : " + label.getConfidence() + "\n");
            		//SQS
            		SendMessageRequest send_msg_request = new SendMessageRequest()
                            .withQueueUrl("https://sqs.us-east-1.amazonaws.com/932808190514/MYqueue")
                            .withMessageBody(photo);
                // use setMessageGroupId and setMessageDeduplicationId for .fifo queue
             		//send_msg_request.setMessageGroupId("messageGroup1");
             		//send_msg_request.setMessageDeduplicationId("photo");
            		sqs.sendMessage(send_msg_request);

                             } 
                }
                
            }
        catch (AmazonRekognitionException e) {
            e.printStackTrace();
        }
        
        }  
    }
}
