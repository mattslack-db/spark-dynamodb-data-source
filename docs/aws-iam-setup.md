# AWS IAM and Unity Catalog Service Credential Setup

This document shows how to provision an IAM role with DynamoDB access in AWS and
register it as a Unity Catalog service credential in a Databricks workspace, so
the data source can be tested end-to-end with executor-side credential refresh.

The example below was used to create the test setup in the Databricks
`aws-sandbox` workspace (account `332745928618`).

## Prerequisites

- AWS CLI authenticated against the target AWS account with permissions to
  create IAM roles and policies.
- Databricks CLI authenticated against the target workspace
  (`databricks --profile <name> auth login`).
- The Unity Catalog metastore in your workspace — find the UC master role ARN
  and external ID by listing existing service credentials:

  ```bash
  databricks --profile <profile> credentials list-credentials --purpose SERVICE -o json
  ```

  The fields `aws_iam_role.unity_catalog_iam_arn` and
  `aws_iam_role.external_id` are what you need.

## 1. Create the IAM role

The role must trust the UC master role with the metastore's external ID, **and
trust itself** (UC validates that the role can self-assume).

```bash
export AWS_PROFILE=databricks-sandbox-admin-332745928618
export AWS_ACCOUNT_ID=332745928618
export ROLE_NAME=spark-dynamodb-test-role
export UC_MASTER_ROLE=arn:aws:iam::414351767826:role/unity-catalog-prod-UCMasterRole-14S5ZJVKOTYTL
export UC_EXTERNAL_ID=0d26daa6-5e44-4c97-a497-ef015f91254a
```

Create the trust policy file. The self-assume statement is required for UC's
storage-credential validation step.

```bash
cat > /tmp/uc-trust-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DatabricksUCTrust",
      "Effect": "Allow",
      "Principal": { "AWS": "${UC_MASTER_ROLE}" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "${UC_EXTERNAL_ID}" }
      }
    },
    {
      "Sid": "SelfAssume",
      "Effect": "Allow",
      "Principal": { "AWS": "arn:aws:iam::${AWS_ACCOUNT_ID}:role/${ROLE_NAME}" },
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF
```

Note: the self-assume principal references a role that doesn't yet exist, so
create the role first with only the UC trust statement, then update the trust
policy to add the self-assume statement.

```bash
# Step 1 — create the role (UC trust only)
cat > /tmp/uc-trust-policy.bootstrap.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DatabricksUCTrust",
      "Effect": "Allow",
      "Principal": { "AWS": "${UC_MASTER_ROLE}" },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": { "sts:ExternalId": "${UC_EXTERNAL_ID}" }
      }
    }
  ]
}
EOF

aws iam create-role \
  --role-name "${ROLE_NAME}" \
  --assume-role-policy-document file:///tmp/uc-trust-policy.bootstrap.json \
  --description "Service credential for spark-dynamodb-data-source DynamoDB integration tests"

# Step 2 — update the trust policy to include the self-assume statement
aws iam update-assume-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-document file:///tmp/uc-trust-policy.json
```

## 2. Attach the DynamoDB policy

For tests, full DynamoDB access scoped to the test region is sufficient. Tighten
this for production use cases.

```bash
cat > /tmp/dynamodb-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "DynamoDBFullAccessForTest",
      "Effect": "Allow",
      "Action": [ "dynamodb:*" ],
      "Resource": "*"
    }
  ]
}
EOF

aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name DynamoDBFullAccessForTest \
  --policy-document file:///tmp/dynamodb-policy.json
```

## 3. Register the role as a UC service credential

```bash
databricks --profile aws-sandbox credentials create-credential --json '{
  "name": "spark-dynamodb-test-cred",
  "purpose": "SERVICE",
  "comment": "Service credential for spark-dynamodb-data-source integration tests",
  "aws_iam_role": {
    "role_arn": "arn:aws:iam::332745928618:role/spark-dynamodb-test-role"
  }
}'
```

## 4. Validate

UC will assume the role end-to-end and report any trust-policy issues.

```bash
databricks --profile aws-sandbox credentials validate-credential --json '{
  "credential_name": "spark-dynamodb-test-cred",
  "purpose": "SERVICE"
}'
```

All three results should report `"PASS"`. A `non self-assuming` failure means
step 1's self-assume statement is missing or wrong.

## 5. Use it from Spark

In a notebook running on DBR 16.2+ in this workspace:

```python
from dynamodb_data_source import DynamoDbDataSource
spark.dataSource.register(DynamoDbDataSource)

(spark.read.format("dynamodb")
   .option("table_name", "my_table")
   .option("aws_region", "us-east-1")
   .option("credential_name", "spark-dynamodb-test-cred")
   .load()
   .show())
```

The `credential_name` option triggers the executor-side resolution path in
`reader.py` / `writer.py`, which calls
`databricks.service_credentials.getServiceCredentialsProvider()` from inside
the Python worker.
