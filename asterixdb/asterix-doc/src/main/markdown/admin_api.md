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