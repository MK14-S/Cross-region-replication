package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/iam"
	"github.com/aws/aws-sdk-go/service/s3"
)

func main() {
	// Flags
	srcBucket := flag.String("source-bucket", "", "Source bucket name (required)")
	srcRegion := flag.String("source-region", "us-east-1", "Source bucket region")
	dstBucket := flag.String("dest-bucket", "", "Destination bucket name (required)")
	dstRegion := flag.String("dest-region", "us-west-2", "Destination bucket region")
	roleName := flag.String("role-name", "s3-replication-role-example", "IAM Role name for replication")
	profile := flag.String("profile", "", "AWS profile to use (optional)")
	flag.Parse()

	if *srcBucket == "" || *dstBucket == "" {
		log.Fatalf("Both --source-bucket and --dest-bucket must be provided.")
	}

	// Create sessions for source and destination regions. Use SharedConfigState to allow profile usage
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
	iamSvc := iam.New(srcSess) // IAM is global; region in session won't matter much

	fmt.Printf("Setting up replication from %s (%s) -> %s (%s)\n", *srcBucket, *srcRegion, *dstBucket, *dstRegion)

	// 1) Create destination bucket if not exists
	err := ensureBucketExists(s3Dst, *dstBucket, *dstRegion)
	if err != nil {
		log.Fatalf("Failed ensuring destination bucket: %v", err)
	}
	fmt.Println("Destination bucket exists/ready.")

	// 2) Enable versioning on both buckets
	if err := enableBucketVersioning(s3Src, *srcBucket); err != nil {
		log.Fatalf("Failed enabling versioning on source bucket: %v", err)
	}
	fmt.Println("Versioning enabled on source bucket.")

	if err := enableBucketVersioning(s3Dst, *dstBucket); err != nil {
		log.Fatalf("Failed enabling versioning on destination bucket: %v", err)
	}
	fmt.Println("Versioning enabled on destination bucket.")

	// 3) Create IAM role for replication
	roleArn, err := ensureReplicationRole(iamSvc, *roleName, *srcBucket, *dstBucket, *dstRegion)
	if err != nil {
		log.Fatalf("Failed to ensure IAM replication role: %v", err)
	}
	fmt.Printf("Replication role ready: %s\n", roleArn)

	// 4) Put replication configuration on source bucket
	if err := putReplicationConfiguration(s3Src, *srcBucket, *dstBucket, roleArn); err != nil {
		log.Fatalf("Failed to put replication configuration: %v", err)
	}
	fmt.Println("Replication configuration applied to source bucket.")

	fmt.Println("Cross-region replication setup complete.")
}

// ensureBucketExists creates a bucket if it doesn't exist.
// For non-us-east-1 regions, LocationConstraint must be set.
func ensureBucketExists(s3client *s3.S3, bucketName, region string) error {
	// Check head bucket
	_, err := s3client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err == nil {
		// exists and accessible
		return nil
	}

	// If HeadBucket error indicates not found or forbidden, try to create
	awsErr, ok := err.(awserr.Error)
	if ok {
		// If forbidden or not found, attempt create (it may fail if bucket owned by other account)
		_ = awsErr
	}

	createInput := &s3.CreateBucketInput{
		Bucket: aws.String(bucketName),
	}
	// For regions other than us-east-1 we must specify LocationConstraint
	if region != "us-east-1" {
		createInput.CreateBucketConfiguration = &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(region),
		}
	}
	_, err = s3client.CreateBucket(createInput)
	if err != nil {
		// If bucket already exists and is owned by you, treat as ok; otherwise fail
		if aerr, ok := err.(awserr.Error); ok {
			if aerr.Code() == s3.ErrCodeBucketAlreadyOwnedByYou {
				return nil
			}
			if aerr.Code() == s3.ErrCodeBucketAlreadyExists {
				return fmt.Errorf("bucket %s already exists and is owned by another account", bucketName)
			}
		}
		return err
	}

	// Wait until bucket exists
	err = s3client.WaitUntilBucketExists(&s3.HeadBucketInput{Bucket: aws.String(bucketName)})
	if err != nil {
		return fmt.Errorf("bucket creation started but wait failed: %w", err)
	}
	return nil
}

// enableBucketVersioning enables versioning on the given bucket.
func enableBucketVersioning(s3client *s3.S3, bucketName string) error {
	_, err := s3client.PutBucketVersioning(&s3.PutBucketVersioningInput{
		Bucket: aws.String(bucketName),
		VersioningConfiguration: &s3.VersioningConfiguration{
			Status: aws.String("Enabled"),
		},
	})
	return err
}

// ensureReplicationRole creates (or returns existing) an IAM role for S3 replication and attaches an inline policy.
// The role's trust policy allows the S3 service to assume it.
func ensureReplicationRole(iamSvc *iam.IAM, roleName, srcBucket, dstBucket, dstRegion string) (string, error) {
	assumeRolePolicy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Principal": map[string]interface{}{
					"Service": "s3.amazonaws.com",
				},
				"Action": "sts:AssumeRole",
			},
		},
	}
	assumePolicyBytes, _ := json.Marshal(assumeRolePolicy)

	createRoleOutput, err := iamSvc.CreateRole(&iam.CreateRoleInput{
		RoleName:                 aws.String(roleName),
		AssumeRolePolicyDocument: aws.String(string(assumePolicyBytes)),
		Description:              aws.String("Role for S3 cross-region replication"),
	})
	var roleArn string
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok {
			// If role already exists, retrieve it
			if aerr.Code() == iam.ErrCodeEntityAlreadyExistsException {
				// Get role
				out, gerr := iamSvc.GetRole(&iam.GetRoleInput{RoleName: aws.String(roleName)})
				if gerr != nil {
					return "", fmt.Errorf("role exists but failed to get role: %w", gerr)
				}
				roleArn = aws.StringValue(out.Role.Arn)
			} else {
				return "", fmt.Errorf("CreateRole error: %w", err)
			}
		} else {
			return "", fmt.Errorf("CreateRole error: %w", err)
		}
	} else {
		roleArn = aws.StringValue(createRoleOutput.Role.Arn)
	}

	// Attach inline policy that allows S3 to replicate from source to destination.
	// Policy gives S3 permissions to read the source object versions and write to destination bucket.
	// NOTE: Adjust policy if you use KMS or need additional permissions.
	policy := map[string]interface{}{
		"Version": "2012-10-17",
		"Statement": []map[string]interface{}{
			{
				"Effect": "Allow",
				"Action": []string{
					"s3:GetObjectVersion",
					"s3:GetObjectVersionAcl",
					"s3:GetObjectVersionTagging",
					"s3:GetObjectVersionForReplication",
					"s3:ListBucket",
					"s3:GetReplicationConfiguration",
				},
				"Resource": []string{
					fmt.Sprintf("arn:aws:s3:::%s", srcBucket),
					fmt.Sprintf("arn:aws:s3:::%s/*", srcBucket),
				},
			},
			{
				"Effect": "Allow",
				"Action": []string{
					"s3:ReplicateObject",
					"s3:ReplicateDelete",
					"s3:ReplicateTags",
					"s3:PutObjectAcl",
					"s3:PutObjectVersionAcl",
					"s3:PutObjectVersionTagging",
					"s3:PutObject",
				},
				"Resource": []string{
					fmt.Sprintf("arn:aws:s3:::%s", dstBucket),
					fmt.Sprintf("arn:aws:s3:::%s/*", dstBucket),
				},
			},
		},
	}

	policyBytes, _ := json.Marshal(policy)
	// Create a unique policy name for each src/dest bucket pair
	policyName := fmt.Sprintf("%s-replication-%s-to-%s", roleName, srcBucket, dstBucket)
	_, err = iamSvc.PutRolePolicy(&iam.PutRolePolicyInput{
		RoleName:       aws.String(roleName),
		PolicyName:     aws.String(policyName),
		PolicyDocument: aws.String(string(policyBytes)),
	})
	if err != nil {
		return "", fmt.Errorf("failed to put role policy: %w", err)
	}

	// Wait a bit for IAM propagation (IAM can be eventually consistent). Small sleep helps avoid immediate use errors.
	time.Sleep(5 * time.Second)

	return roleArn, nil
}

// putReplicationConfiguration configures a replication rule on the source bucket to the destination bucket.
func putReplicationConfiguration(s3client *s3.S3, srcBucket, dstBucket, roleArn string) error {
	// Build the replication config:
	// A single rule that replicates everything (empty prefix) and is enabled.
	dstARN := fmt.Sprintf("arn:aws:s3:::%s", dstBucket)

	// Prepare destination
	destination := &s3.Destination{
		Bucket: aws.String(dstARN),
		// StorageClass: aws.String("STANDARD"), // optional; can set to reduced_redundancy etc.
	}

	// Get existing replication configuration
	var existingRules []*s3.ReplicationRule
	getOut, err := s3client.GetBucketReplication(&s3.GetBucketReplicationInput{
		Bucket: aws.String(srcBucket),
	})
	if err == nil && getOut.ReplicationConfiguration != nil {
		existingRules = getOut.ReplicationConfiguration.Rules
	}

	// Check if a rule for this destination bucket already exists
	ruleID := fmt.Sprintf("replicate-to-%s", dstBucket)
	updated := false
	maxPriority := int64(0)
	for _, r := range existingRules {
		if r.Priority != nil && *r.Priority > maxPriority {
			maxPriority = *r.Priority
		}
	}
	for i, r := range existingRules {
		if r.Destination != nil && r.Destination.Bucket != nil && destination.Bucket != nil && *r.Destination.Bucket == *destination.Bucket {
			// Update existing rule, keep its priority
			priority := r.Priority
			if priority == nil {
				priority = aws.Int64(maxPriority + 1)
			}
			existingRules[i] = &s3.ReplicationRule{
				ID:       aws.String(ruleID),
				Status:   aws.String("Enabled"),
				Priority: priority,
				Filter: &s3.ReplicationRuleFilter{
					Prefix: aws.String(""),
				},
				Destination: destination,
				DeleteMarkerReplication: &s3.DeleteMarkerReplication{
					Status: aws.String("Disabled"),
				},
			}
			updated = true
			break
		}
	}
	if !updated {
		// Add new rule for this destination bucket with unique priority
		newRule := &s3.ReplicationRule{
			ID:       aws.String(ruleID),
			Status:   aws.String("Enabled"),
			Priority: aws.Int64(maxPriority + 1),
			Filter: &s3.ReplicationRuleFilter{
				Prefix: aws.String(""),
			},
			Destination: destination,
			DeleteMarkerReplication: &s3.DeleteMarkerReplication{
				Status: aws.String("Disabled"),
			},
		}
		existingRules = append(existingRules, newRule)
	}

	configuration := &s3.ReplicationConfiguration{
		Role:  aws.String(roleArn),
		Rules: existingRules,
	}

	_, err = s3client.PutBucketReplication(&s3.PutBucketReplicationInput{
		Bucket:                   aws.String(srcBucket),
		ReplicationConfiguration: configuration,
	})
	if err != nil {
		return fmt.Errorf("PutBucketReplication failed: %w", err)
	}
	return nil
}
