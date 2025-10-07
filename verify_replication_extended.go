package main

import (
	"bytes"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	// Flags
	srcBucket := flag.String("source-bucket", "", "Source bucket name (required)")
	srcRegion := flag.String("source-region", "us-east-1", "Source bucket region")
	profile := flag.String("profile", "", "AWS profile to use (optional)")
	key := flag.String("key", "replication-test-ss.txt", "Object key to use for verification")
	flag.Parse()

	if *srcBucket == "" {
		log.Fatalf("--source-bucket must be provided.")
	}

	// Create session for source region
	srcSess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String(*srcRegion)},
		Profile:           *profile,
		SharedConfigState: session.SharedConfigEnable,
	}))
	s3Src := s3.New(srcSess)

	// Step 1: Upload to source bucket
	content := []byte("Hello extended replication test from Go SDK v1. Hello to CRR! Bye.")
	_, err := s3Src.PutObject(&s3.PutObjectInput{
		Bucket: aws.String(*srcBucket),
		Key:    key,
		Body:   bytes.NewReader(content),
	})
	if err != nil {
		log.Fatalf("Failed to upload object to source bucket: %v", err)
	}
	fmt.Printf("Uploaded object %s to source bucket %s\n", *key, *srcBucket)

	// Step 2: Get all destination buckets from replication rules
	getOut, err := s3Src.GetBucketReplication(&s3.GetBucketReplicationInput{
		Bucket: aws.String(*srcBucket),
	})
	if err != nil || getOut.ReplicationConfiguration == nil {
		log.Fatalf("Failed to get replication configuration: %v", err)
	}
	var destBuckets []string
	for _, rule := range getOut.ReplicationConfiguration.Rules {
		if rule.Destination != nil && rule.Destination.Bucket != nil {
			// Destination bucket ARN: arn:aws:s3:::bucketname
			arn := *rule.Destination.Bucket
			// Extract bucket name from ARN
			var bucketName string
			_, err := fmt.Sscanf(arn, "arn:aws:s3:::%s", &bucketName)
			if err == nil {
				destBuckets = append(destBuckets, bucketName)
			}
		}
	}
	if len(destBuckets) == 0 {
		log.Fatalf("No destination buckets found in replication rules.")
	}

	// Step 3: For each destination bucket, check for replicated object
	for _, dstBucket := range destBuckets {
		fmt.Printf("\nChecking replication to destination bucket: %s\n", dstBucket)
		// Detect region for destination bucket
		detectedRegion := *srcRegion
		// Use a generic session to get bucket location
		genericSess := session.Must(session.NewSessionWithOptions(session.Options{
			Config:            aws.Config{Region: aws.String(*srcRegion)},
			Profile:           *profile,
			SharedConfigState: session.SharedConfigEnable,
		}))
		genericS3 := s3.New(genericSess)
		locOut, err := genericS3.GetBucketLocation(&s3.GetBucketLocationInput{
			Bucket: aws.String(dstBucket),
		})
		if err == nil && locOut.LocationConstraint != nil {
			detectedRegion = aws.StringValue(locOut.LocationConstraint)
			if detectedRegion == "" {
				detectedRegion = "us-east-1"
			}
			// AWS returns some regions as enums, e.g. EU, so handle that
			if detectedRegion == "EU" {
				detectedRegion = "eu-west-1"
			}
		}
		dstSess := session.Must(session.NewSessionWithOptions(session.Options{
			Config:            aws.Config{Region: aws.String(detectedRegion)},
			Profile:           *profile,
			SharedConfigState: session.SharedConfigEnable,
		}))
		s3Dst := s3.New(dstSess)

		fmt.Printf("Using region %s for bucket %s\n", detectedRegion, dstBucket)
		fmt.Println("Waiting for replication (may take 30–60 seconds)...")
		found := false
		for i := 0; i < 12; i++ { // check up to 2 minutes
			time.Sleep(10 * time.Second)
			_, err := s3Dst.HeadObject(&s3.HeadObjectInput{
				Bucket: aws.String(dstBucket),
				Key:    key,
			})
			if err == nil {
				found = true
				break
			}
			fmt.Printf("Check %d: object not replicated yet\n", i+1)
		}

		if found {
			fmt.Printf("✅ Object %s replicated successfully to bucket %s\n", *key, dstBucket)
		} else {
			fmt.Printf("❌ Object %s did not replicate to bucket %s within timeout\n", *key, dstBucket)
		}
	}

	// Step 4: List all objects in source bucket
	fmt.Println("\nListing objects in source bucket:")
	srcObjects, err := listObjects(s3Src, *srcBucket)
	if err != nil {
		log.Fatalf("Failed to list source bucket: %v", err)
	}
	for _, obj := range srcObjects {
		fmt.Printf("  %s\n", obj)
	}
	// List objects in each destination bucket
	for _, dstBucket := range destBuckets {
		// Detect region for destination bucket
		detectedRegion := *srcRegion
		genericSess := session.Must(session.NewSessionWithOptions(session.Options{
			Config:            aws.Config{Region: aws.String(*srcRegion)},
			Profile:           *profile,
			SharedConfigState: session.SharedConfigEnable,
		}))
		genericS3 := s3.New(genericSess)
		locOut, err := genericS3.GetBucketLocation(&s3.GetBucketLocationInput{
			Bucket: aws.String(dstBucket),
		})
		if err == nil && locOut.LocationConstraint != nil {
			detectedRegion = aws.StringValue(locOut.LocationConstraint)
			if detectedRegion == "" {
				detectedRegion = "us-east-1"
			}
			if detectedRegion == "EU" {
				detectedRegion = "eu-west-1"
			}
		}
		dstSess := session.Must(session.NewSessionWithOptions(session.Options{
			Config:            aws.Config{Region: aws.String(detectedRegion)},
			Profile:           *profile,
			SharedConfigState: session.SharedConfigEnable,
		}))
		s3Dst := s3.New(dstSess)
		fmt.Printf("\nListing objects in destination bucket: %s (region: %s)\n", dstBucket, detectedRegion)
		dstObjects, err := listObjects(s3Dst, dstBucket)
		if err != nil {
			log.Fatalf("Failed to list destination bucket %s: %v", dstBucket, err)
		}
		for _, obj := range dstObjects {
			fmt.Printf("  %s\n", obj)
		}
		fmt.Printf("\nSource bucket has %d objects, destination bucket %s has %d objects\n",
			len(srcObjects), dstBucket, len(dstObjects))
		if len(dstObjects) >= len(srcObjects) {
			fmt.Println("✅ Destination bucket contains all (or more) objects.")
		} else {
			fmt.Println("⚠️ Some objects may not yet have replicated.")
		}
	}

}

// listObjects fetches all object keys in a bucket
func listObjects(s3client *s3.S3, bucket string) ([]string, error) {
	var keys []string
	input := &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	}
	err := s3client.ListObjectsV2Pages(input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.Contents {
			keys = append(keys, *obj.Key)
		}
		return !lastPage
	})
	if err != nil {
		return nil, err
	}
	return keys, nil
}
