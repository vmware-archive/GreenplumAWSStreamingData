# GreenplumAWSStreamingData

This Git project goal is to demonstrate Closed loop analytics with Greenplum leveraging AWS cloud native tools.

The Demo application choosed it a credit card fraud transaction Machine learning model. The Model will be built and tested on Greenplum using sample application in samples section of [RTSMADlib](https://www.google.com/url?q=https://github.com/pivotal/Realtime-scoring-for-MADlib&sa=D&source=hangouts&ust=1584626576096000&usg=AFQjCNEppB_6B4bN6Tg-eUv1O1qOuvKAkg). The model is then deployed on Amazon EKS to integrate in to streaming solution via Amazon Java Lambda. 

The streaming part is built with all AWS cloud native tooling;
* AWS Kinesis Stream
* AWS Kinesis S3 Firehose
* Pivotal Greenplum PXF, 
* AWS SQS 
* AWS Lambda
* RTSMadlib to deploy ML models on AWS EKS.

The overall folow can be shown as ;
![](/VmwareGreenplumStreamingMLUsecaseOnAmazonAWS.svg)



