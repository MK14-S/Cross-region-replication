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
	dstBucket := flag.String("dest-bucket", "", "Destination bucket name (required)")
	dstRegion := flag.String("dest-region", "us-west-2", "Destination bucket region")
	profile := flag.String("profile", "", "AWS profile to use (optional)")
	key := flag.String("key", "replication-test-1.txt", "Object key to use for verification")
	flag.Parse()

	if *srcBucket == "" || *dstBucket == "" {
		log.Fatalf("Both --source-bucket and --dest-bucket must be provided.")
	}

	// Create sessions
	srcSess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String(*srcRegion)},
		Profile:           *profile,
		SharedConfigState: session.SharedConfigEnable,
	}))
	dstSess := session.Must(session.NewSessionWithOptions(session.Options{
		Config:            aws.Config{Region: aws.String(*dstRegion)},
		Profile:           *profile,
		SharedConfigState: session.SharedConfigEnable,
	}))

	s3Src := s3.New(srcSess)
	s3Dst := s3.New(dstSess)

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

	// Step 2: Wait for replication
	fmt.Println("Waiting for replication (may take 30–60 seconds)...")
	found := false
	for i := 0; i < 12; i++ { // check up to 2 minutes
		time.Sleep(10 * time.Second)
		_, err := s3Dst.HeadObject(&s3.HeadObjectInput{
			Bucket: aws.String(*dstBucket),
			Key:    key,
		})
		if err == nil {
			found = true
			break
		}
		fmt.Printf("Check %d: object not replicated yet\n", i+1)
	}

	if found {
		fmt.Printf("✅ Object %s replicated successfully to bucket %s\n", *key, *dstBucket)
	} else {
		fmt.Printf("❌ Object %s did not replicate to bucket %s within timeout\n", *key, *dstBucket)
	}

	// Step 3: List all objects in both buckets
	fmt.Println("\nListing objects in source bucket:")
	srcObjects, err := listObjects(s3Src, *srcBucket)
	if err != nil {
		log.Fatalf("Failed to list source bucket: %v", err)
	}
	for _, obj := range srcObjects {
		fmt.Printf("  %s\n", obj)
	}

	fmt.Println("\nListing objects in destination bucket:")
	dstObjects, err := listObjects(s3Dst, *dstBucket)
	if err != nil {
		log.Fatalf("Failed to list destination bucket: %v", err)
	}
	for _, obj := range dstObjects {
		fmt.Printf("  %s\n", obj)
	}

	// Step 4: Compare counts
	fmt.Printf("\nSource bucket has %d objects, destination bucket has %d objects\n",
		len(srcObjects), len(dstObjects))

	if len(dstObjects) >= len(srcObjects) {
		fmt.Println("✅ Destination bucket contains all (or more) objects.")
	} else {
		fmt.Println("⚠️ Some objects may not yet have replicated.")
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
