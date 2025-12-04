# How to Test the API with Postman

This guide provides a full example of how to test the deployed API endpoint using Postman.

---

### Step 1: Create a New Request

1.  Open Postman and click the `+` button to open a new tab.

### Step 2: Set the Method and URL

1.  Change the HTTP method from `GET` to **`POST`**.
2.  In the URL field, paste your full **Invoke URL** from the API Gateway `v1` stage. It should look like this:
    ```
    https://{your-api-id}.execute-api.{your-region}.amazonaws.com/v1/service-records
    ```

### Step 3: Configure Headers

1.  Click on the **Headers** tab.
2.  Add two key-value pairs:

| KEY | VALUE | DESCRIPTION |
| :--- | :--- | :--- |
| `Content-Type` | `application/json` | Tells the API you are sending JSON data. |
| `x-api-key` | `YOUR_API_KEY_VALUE` | The API Key you created and copied in Step 7 of the setup guide. |

### Step 4: Add the Request Body

1.  Click on the **Body** tab.
2.  Select the **raw** radio button.
3.  On the right side of the radio buttons, a dropdown will appear. Change it from `Text` to **`JSON`**.
4.  Paste the entire JSON payload below into the text area.

#### Example JSON Payload:
```json
[
  {
    "service_record_id": "SRV-2025-001345",
    "dealer": { "dealer_id": "DEALER-JKT-01", "dealer_name": "Prestige Auto Jakarta", "dealer_location": "Jakarta" },
    "vehicle": { "vin": "JN1AZ000000C12345", "model": "Grand Livina", "year": 2023, "odometer_reading": 30152 },
    "service_details": {
      "service_date": "2025-11-20T11:05:30Z",
      "service_advisor_name": "Budi",
      "total_cost": 1250000,
      "services_performed": [ { "service_code": "SVC-OIL", "service_name": "Engine Oil and Filter Change Labor", "service_cost": 300000 } ],
      "parts_used": [ { "part_code": "OIL-5W30-SYN", "part_name": "5W-30 Synthetic Oil (Liter)", "quantity": 4, "unit_cost": 150000 } ]
    },
    "customer": { "customer_id": "CUST-8872", "customer_name": "Siti" }
  },
  {
    "service_record_id": "SRV-2025-001346",
    "dealer": { "dealer_id": "DEALER-JKT-01", "dealer_name": "Prestige Auto Jakarta", "dealer_location": "Jakarta" },
    "vehicle": { "vin": "JN1AZ000000D67890", "model": "Serena", "year": 2022, "odometer_reading": 45091 },
    "service_details": {
      "service_date": "2025-11-20T12:15:00Z",
      "service_advisor_name": "Budi",
      "total_cost": 900000,
      "services_performed": [ { "service_code": "SVC-BRAKE", "service_name": "Brake Pad Replacement", "service_cost": 400000 } ],
      "parts_used": [ { "part_code": "BRK-PAD-FRT", "part_name": "Front Brake Pad Set", "quantity": 1, "unit_cost": 500000 } ]
    },
    "customer": { "customer_id": "CUST-1024", "customer_name": "Ahmad" }
  }
]
```

### Step 5: Send the Request

Click the blue **Send** button.

### Step 6: Check the Response

If the request is successful, you should see the following in the response panel at the bottom:
*   **Status:** `202 Accepted`
*   **Body:** A JSON response from the Lambda function, like:
    ```json
    {
        "status": "accepted",
        "file_key": "raw/service-records/date=2025-11-20/11-05-30_...some_uuid....json",
        "record_count": 2
    }
    ```

After a successful request, you can go to your `raw` S3 bucket in the AWS console to verify that the new JSON file was created.