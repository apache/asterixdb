# Admin API Documentation

## GET /admin/version

**Description:**
Returns build and version information of the AsterixDB instance. Useful for debugging and system introspection.

**Method:**
GET

**Response:**
- A JSON object containing build properties such as version details.

**Content-Type:**
text/plain (JSON-formatted string)

**Example Response:**
```json
{
  "git.build.version": "x.x.x",
  "git.commit.id": "abc123",
  "build.timestamp": "2026-01-01T00:00:00Z"
}
```
---
## GET /admin/net

**Description:**
Returns network-related diagnostic information for the cluster. Useful for analyzing connectivity and communication between nodes.

**Method:**
GET

**Response:**
- JSON object containing network status and diagnostics.

**Content-Type:**
application/json

**Example Request:**
GET /admin/net

**Example Response:**
```json
{
  "nodes": [
    {
      "node_id": "nc1",
      "status": "connected"
    }
  ]
}
```

---




## GET /admin/active

**Description:**
Returns information about currently active operations and jobs in the system.

**Method:**
GET

**Response:**
- JSON object containing active job details and system activity.

**Content-Type:**
application/json

**Example Request:**
GET /admin/active

**Example Response:**
```json
{
  "active_jobs": [
    {
      "job_id": "12345",
      "status": "running"
    }
  ]
}
```

---



## GET /admin/libraryrecovery

**Description:**
Returns information about recovery and management of user-defined libraries.

**Method:**
GET

**Response:**
- JSON object containing library recovery status and details.

**Content-Type:**
application/json

**Example Request:**
GET /admin/libraryrecovery

**Example Response:**
```json
{
  "libraries": [
    {
      "name": "example-lib",
      "status": "recovered"
    }
  ]
}
```
---
