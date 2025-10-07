# S3 Cross-Region Replication Setup

This repository contains a Go script to automate the setup of AWS S3 Cross-Region Replication (CRR) between two buckets.

## Features
- Creates destination bucket(s) if they do not exist
- Enables versioning on all buckets involved
- Creates an IAM role with required trust and inline policies for replication
- Applies replication configuration to the source bucket
- Supports multiple replication rules (multiple destination buckets per source)

## Prerequisites
- Go 1.18+
- AWS CLI installed and configured (`aws configure`)
- AWS credentials with permissions to create buckets, IAM roles, and configure replication

## Usage

```bash
go run s3_crr_setup.go \
  --source-bucket <source-bucket-name> \
  --source-region <source-region> \
  --dest-bucket <destination-bucket-name> \
  --dest-region <destination-region> \
  --role-name <replication-role-name>
```

### Example
```bash
go run s3_crr_setup.go \
  --source-bucket my-src-bucket-123456 \
  --source-region us-east-1 \
  --dest-bucket my-dest-bucket-98765 \
  --dest-region us-west-2 \
  --role-name s3-replication-role
```

## Implementation Details

### s3_crr_setup.go

This Go file automates the following steps for S3 cross-region replication:

1. **Parse Flags**: Reads command-line arguments for source/destination bucket names, regions, IAM role name, and AWS profile.
2. **Create AWS Sessions**: Initializes AWS SDK sessions for both source and destination regions, supporting custom profiles.
3. **Bucket Creation**: Checks if the destination bucket(s) exist; creates them if not. Handles region-specific constraints.
4. **Enable Versioning**: Ensures versioning is enabled on all buckets involved, which is required for replication.
5. **IAM Role Creation**: Creates (or retrieves) an IAM role for replication. The role's trust policy allows S3 to assume it. An inline policy is attached to grant necessary S3 permissions for replication.
6. **Replication Configuration**: Applies replication rules to the source bucket. Each rule replicates all objects to a specific destination bucket, supports multiple destinations, and sets `DeleteMarkerReplication` as required by AWS.
7. **Error Handling**: Each step checks for errors and prints informative messages. The script exits on failure.

#### Key Functions
- `ensureBucketExists`: Checks for bucket existence and creates it if needed.
- `enableBucketVersioning`: Enables versioning on a bucket.
- `ensureReplicationRole`: Creates or retrieves an IAM role and attaches the required policy.
- `putReplicationConfiguration`: Configures replication rules on the source bucket, supporting multiple destinations and unique priorities.

#### AWS SDK v1
The script uses AWS SDK v1 for Go, which is in maintenance mode but still supported. All IAM and S3 operations are performed using this SDK.

#### Security
IAM role and policies are created programmatically. No manual JSON policy files are required.

## Verification Script

### verify_replication_extended.go

This Go file verifies that cross-region replication is working as expected:

1. **Parse Flags**: Reads command-line arguments for source bucket name, region, AWS profile, and the object key to use for testing.
2. **Create AWS Session**: Initializes AWS SDK session for the source region.
3. **Upload Test Object**: Uploads a test object to the source bucket using the provided key.
4. **Fetch Replication Rules**: Automatically detects all destination buckets from the source bucket's replication configuration.
5. **Detect Destination Regions**: Uses `GetBucketLocation` to determine the correct region for each destination bucket.
6. **Wait for Replication**: Periodically checks each destination bucket for the replicated object, waiting up to 2 minutes per bucket.
7. **List Objects**: Lists all objects in the source bucket and each destination bucket for comparison.
8. **Compare Object Counts**: Compares the number of objects in each bucket and reports replication status.

#### Key Functions
- `listObjects`: Lists all object keys in a given bucket using paginated requests.

#### Usage
```bash
go run verify_replication_extended.go \
  --source-bucket <source-bucket-name> \
  --source-region <source-region> \
  --key <test-object-key>
```

### Example
```bash
go run verify_replication_extended.go \
  --source-bucket my-src-bucket-123456 \
  --source-region us-east-1 \
  --key replication-test-2.txt
```

This script helps confirm that objects uploaded to the source bucket are successfully replicated to all destination buckets (across regions) and provides a summary of objects in each bucket.

## License
MIT
