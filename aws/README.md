# Cloud Data Pipeline for Car Dealership Service Centers

## **1. Overview**

This project represents a cloud-native expansion of the car factory data infrastructure. The goal is to capture and process vehicle service data from dealership service centers across Indonesia.

Unlike the standardized production line data, service records are highly variable—ranging from simple oil changes to complex engine repairs. To handle this variability and ensure scalability, we have built an event-driven, serverless data pipeline on **AWS**.

**Key Business Goals:**
*   **Centralized Service History:** Aggregate maintenance records from dispersed dealerships into a single source of truth.
*   **Data-Driven Insights:** Enable analysis of common failures, part usage trends, and service costs to improve vehicle quality and warranty planning.
*   **Scalability:** Support an increasing volume of service records as the number of vehicles on the road grows.

---

## **2. Data Architecture**

The pipeline utilizes a modern, serverless architecture designed for resilience and cost-efficiency.

![Service Records Architecture](../img/service_records_architecture.png)

### **Architecture Flow**
1.  **Ingestion (API Gateway + Lambda):** Dealerships submit service records via a secure REST API. A Lambda function validates the payload and stores the raw JSON data in an **S3 Raw Bucket**.
2.  **Buffering (SQS):** An S3 event triggers a message to an **SQS Queue**, decoupling ingestion from processing and ensuring no data is lost during spikes.
3.  **Processing (ECS Fargate):** Fargate tasks (containers) scale out based on queue depth. They read raw files, perform data cleaning/transformation using **Pandas** and **Pandera**, and separate valid data from invalid data.
    *   **Valid Data:** Stored in the **S3 Cleaned Bucket** (as Parquet for analytics and JSON for DynamoDB).
    *   **Invalid Data:** Moved to the **S3 Error Bucket** for debugging.
4.  **Loading (AWS Glue):** A scheduled Glue job batches the processed JSON files and efficiently loads them into **DynamoDB**.
5.  **Storage (DynamoDB):** The final destination for service records, offering low-latency access for operational reporting and applications.

### **2.1 Design Decisions (The "Why")**

**Q: Why is the ECS Processor "Event-Driven" while the Glue Loader is "Scheduled"?**
*   **ECS Fargate (Event-Driven):** We prioritize **low latency** for data validation. We want to know immediately if a file is valid or corrupt. SQS triggers processing as soon as data arrives.
*   **AWS Glue (Scheduled):** We prioritize **cost and throughput** for the database load. Glue charges a minimum of 1 minute per run. Triggering it for every single small file would be prohibitively expensive (the "Small File Problem"). Batching thousands of records into a daily run makes the process efficient and cheap.

**Q: Why use AWS Glue? Why not load directly to DynamoDB from Fargate?**
*   **Decoupling & Specialization:** Fargate is optimized for complex, custom Python transformations and validation logic. Glue is a managed ETL service specifically optimized for moving large volumes of data between stores (S3 to DynamoDB).
*   **Batch Efficiency:** By decoupling the load step, we allow Fargate to focus purely on processing. Glue then handles the "heavy lifting" of writing to the database in bulk, utilizing DynamoDB's batch write capabilities more effectively than individual Fargate tasks could.

---

## **3. Setup Guide**

Follow these steps to deploy the pipeline in your own AWS environment.

### **Prerequisites**
*   **AWS Account** with admin access.
*   **AWS CLI** installed and configured (`aws configure`).
*   **Docker Desktop** installed and running.

### **Phase 1: Infrastructure Setup**
1.  **S3 Buckets:** Create three unique buckets (block public access):
    *   `project-raw-data`
    *   `project-cleaned-data`
    *   `project-error-data`
2.  **DynamoDB Table:** Create a table named `service_records` with Partition Key `service_record_id` (String).
3.  **SQS Queues:**
    *   Create a **Dead-Letter Queue (DLQ)** named `service-record-process-dlq`.
    *   Create a **Standard Queue** named `service-record-process-queue`. Configure it to use the DLQ created above.

### **Phase 2: Ingestion API (Lambda & API Gateway)**
1.  **ECR Repository:** Create a private repo named `car-service-api`.
2.  **Build & Push Image:**
    ```bash
    # Login
    aws ecr get-login-password --region [REGION] | docker login --username AWS --password-stdin [ACCOUNT_ID].dkr.ecr.[REGION].amazonaws.com
    # Build
    docker build -t car-service-api -f aws/API/Dockerfile aws/API
    # Tag & Push
    docker tag car-service-api:latest [REPO_URI]:latest
    docker push [REPO_URI]:latest
    ```
3.  **Lambda Function:** Create a function from the container image. Add an environment variable `RAW_BUCKET_NAME`.
4.  **API Gateway:** Create a REST API with a `POST /service-records` endpoint. Integrate it with the Lambda function. Create an API Key and Usage Plan to secure the endpoint.

### **Phase 3: Data Processor (ECS Fargate)**
1.  **ECR Repository:** Create a private repo named `car-service-processor` and push the processor image (similar to step 2).
2.  **ECS Cluster:** Create a generic **Fargate** cluster.
3.  **Task Definition:** Create a task definition utilizing the processor image. Add environment variables: `RAW_BUCKET_NAME`, `CLEANED_BUCKET_NAME`, `ERROR_BUCKET_NAME`, and `SQS_QUEUE_URL`. Grant the task role permissions to access S3 and SQS.
4.  **ECS Service:** Create a service to run the task definition. Set desired tasks to 0 initially.
5.  **Auto-Scaling:** Configure Step Scaling policies on the service to scale out (add tasks) when SQS messages > 0 and scale in (remove tasks) when messages = 0.

### **Phase 4: Triggers & Loading**
1.  **S3 Trigger:** Configure the `raw-data` bucket to send an event notification to the `service-record-process-queue` on `All object create events`.
2.  **Glue Job:**
    *   Create a **Python Shell** job. Upload the `aws/glue/load_to_dynamo.py` script.
    *   Set arguments `--DYNAMODB_TABLE_NAME` and `--SOURCE_BUCKET_NAME`.
3.  **Glue Schedule:** Create a Trigger to run the Glue job daily (e.g., at 00:30 UTC).

### **Phase 5: CI/CD (GitHub Actions)**
1.  **IAM User:** Create a `github-actions-deployer` user with permissions for ECR and ECS updates.
2.  **Secrets:** Add `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your GitHub repository secrets.
3.  **Deploy:** Push changes to the `main` branch to trigger the `.github/workflows/deploy-processor.yml` workflow.
