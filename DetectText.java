package com.amazonaws.samples;

import com.amazonaws.services.rekognition.AmazonRekognition;
import com.amazonaws.services.rekognition.AmazonRekognitionClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.rekognition.model.AmazonRekognitionException;
import com.amazonaws.services.rekognition.model.Image;
import com.amazonaws.services.rekognition.model.S3Object;
import com.amazonaws.services.rekognition.model.DetectTextRequest;
import com.amazonaws.services.rekognition.model.DetectTextResult;
import com.amazonaws.services.rekognition.model.TextDetection;

import java.util.List;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.File;


public class DetectText {
	
	public static void main(String[] args) throws Exception {
        
        String bucket = "njit-cs-643";
        AmazonRekognition rekognitionClient = AmazonRekognitionClientBuilder.standard().withRegion("us-east-1").build();
        final AmazonSQS sqs = AmazonSQSClientBuilder.standard().withRegion("us-east-1").build();
        final AmazonS3 s3 = AmazonS3ClientBuilder.standard().withRegion("us-east-1").build();
        ListObjectsV2Result result = s3.listObjectsV2(bucket);
        List<S3ObjectSummary> objects = result.getObjectSummaries();
        String sqsurl = "https://sqs.us-east-1.amazonaws.com/932808190514/MYqueue";// assigning queue url to Variable
        FileWriter fw = new FileWriter("DetectedText.txt",true); // Creating File for .txt
        File f = new File("DetectedText.txt");
        BufferedWriter bw = new BufferedWriter(fw);
        System.out.println(f.getAbsolutePath());
        
        System.out.println("Receiving messages \n");
        final ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(sqsurl);
        receiveMessageRequest.setMaxNumberOfMessages(10);
        while(true) {
        final List<Message> messages = sqs.receiveMessage(receiveMessageRequest).getMessages();
       
        for (final Message message : messages) {
        	if(message.getBody().equals("-1")) {
        		System.out.println("end of Reciving Message");
        		final String messageReceiptHandle = message.getReceiptHandle();
                sqs.deleteMessage(new DeleteMessageRequest(sqsurl,messageReceiptHandle));
        		 bw.close();
        		System.exit(0);
        	}
        	for (S3ObjectSummary os : objects) {
        		
              if(os.getKey().contains(message.getBody())) {
            	  bw.write(message.getBody());
            	  bw.write("    ");
            	  System.out.println("* " + os.getKey());
            	  DetectTextRequest request = new DetectTextRequest().withImage(new Image().withS3Object(new S3Object().withName(os.getKey()).withBucket(bucket)));
            	  try {
                      DetectTextResult textresult = rekognitionClient.detectText(request);
                      List<TextDetection> textDetections = textresult.getTextDetections();
                      System.out.println("Detected lines and words for " + os.getKey());
                      for (TextDetection text: textDetections) {
                    	  
                          if(text.getConfidence() > 90 ) {                  
                              System.out.println("Detected: " + text.getDetectedText());
                              System.out.println("Confidence: " + text.getConfidence().toString());
                              System.out.println();
                      		  bw.write(text.getDetectedText());
                      		  bw.write("\t\t\t");
                      }
                      }
                   } catch(AmazonRekognitionException e) {
                      e.printStackTrace();
                   }catch(IOException e) {e.printStackTrace();}
              
            	// Deleting the message.
                  System.out.println("Deleting a message.\n");
                  final String messageReceiptHandle = message.getReceiptHandle();
                  sqs.deleteMessage(new DeleteMessageRequest(sqsurl,messageReceiptHandle));              
                  bw.write("\n");
              }            
        }
        System.out.println();

         }
        }
        }
    }
