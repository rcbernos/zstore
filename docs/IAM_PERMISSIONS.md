# IAM Permissions

Minimum permissions required for zstore to operate with AWS and GCP.

## AWS

Zstore uses three AWS services: S3 (shard storage), DynamoDB (metadata), and Resource Groups Tagging API (migration tracking).

### S3 Permissions

Required on each S3 bucket used for shard storage:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME",
        "arn:aws:s3:::YOUR-BUCKET-NAME/*"
      ]
    }
  ]
}
```

- `PutObject` — upload shards
- `GetObject` — download shards (also uses `HeadObject` for progress bars)
- `DeleteObject` — delete individual shards
- `ListBucket` — `DeletePrefix` lists objects before deleting them

### DynamoDB Permissions

Required on the metadata table (default: `object_metadata`):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:DeleteItem",
        "dynamodb:DescribeTable"
      ],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/object_metadata"
    }
  ]
}
```

For running migrations (`zstore init` / `zstore down`), additional permissions are needed:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:CreateTable",
        "dynamodb:DeleteTable",
        "dynamodb:DescribeTable"
      ],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/object_metadata"
    }
  ]
}
```

### Resource Groups Tagging API Permissions

Required for the migration system to track which migrations have been applied:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "tag:GetResources",
        "tag:TagResources",
        "tag:UntagResources"
      ],
      "Resource": "*"
    }
  ]
}
```

The tagging API does not support resource-level restrictions — the resource must be `*`.

### Combined Policy

A single policy covering all zstore operations:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ZstoreS3",
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::YOUR-BUCKET-NAME",
        "arn:aws:s3:::YOUR-BUCKET-NAME/*"
      ]
    },
    {
      "Sid": "ZstoreDynamoDB",
      "Effect": "Allow",
      "Action": [
        "dynamodb:PutItem",
        "dynamodb:GetItem",
        "dynamodb:Query",
        "dynamodb:DeleteItem",
        "dynamodb:CreateTable",
        "dynamodb:DeleteTable",
        "dynamodb:DescribeTable"
      ],
      "Resource": "arn:aws:dynamodb:REGION:ACCOUNT:table/object_metadata"
    },
    {
      "Sid": "ZstoreTagging",
      "Effect": "Allow",
      "Action": [
        "tag:GetResources",
        "tag:TagResources",
        "tag:UntagResources"
      ],
      "Resource": "*"
    }
  ]
}
```

Replace `YOUR-BUCKET-NAME`, `REGION`, and `ACCOUNT` with your values. Add additional S3 bucket ARNs if using multiple buckets.

## GCP

Zstore uses Google Cloud Storage for shard storage.

### Required Permissions

The service account or user needs the following permissions on each GCS bucket:

- `storage.objects.create` — upload shards
- `storage.objects.get` — download shards
- `storage.objects.delete` — delete shards
- `storage.objects.list` — `DeletePrefix` lists objects before deleting

The predefined role **`roles/storage.objectAdmin`** covers all of these. Apply it at the bucket level:

```bash
gcloud storage buckets add-iam-policy-binding gs://YOUR-BUCKET-NAME \
  --member="serviceAccount:YOUR-SA@YOUR-PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

### Service Account Setup

1. Create a service account:

```bash
gcloud iam service-accounts create zstore-sa \
  --display-name="Zstore Storage Service Account"
```

2. Grant bucket access:

```bash
gcloud storage buckets add-iam-policy-binding gs://YOUR-BUCKET-NAME \
  --member="serviceAccount:zstore-sa@YOUR-PROJECT.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"
```

3. Create and download a key:

```bash
gcloud iam service-accounts keys create ./gcs-credentials.json \
  --iam-account=zstore-sa@YOUR-PROJECT.iam.gserviceaccount.com
```

4. Set the environment variable:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=./gcs-credentials.json
```

## Credential Setup Summary

| Provider | Method | Environment Variable |
|----------|--------|---------------------|
| AWS | Access key pair | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` |
| AWS | AWS profile | `AWS_PROFILE` (uses `~/.aws/credentials`) |
| AWS | EC2 instance role | Automatic (no env var needed) |
| GCP | Service account JSON | `GOOGLE_APPLICATION_CREDENTIALS` |
| GCP | Application Default Credentials | `gcloud auth application-default login` |
| GCP | GCE metadata | Automatic (no env var needed) |

AWS region for DynamoDB is resolved in priority order:
1. `dynamodb_region` in `config.yaml`
2. `AWS_REGION` environment variable
3. `AWS_DEFAULT_REGION` environment variable
