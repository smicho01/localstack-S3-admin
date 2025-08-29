/**
 * LocalStack S3 UI Backend API
 *
 * This Express server provides a REST API for interacting with LocalStack S[EV]3 Admin
 * It acts as a proxy between your frontend and LocalStack's S3 service
 *
 * Prerequisites:
 * - LocalStack running on localhost:4566
 * - Node.js 16+ installed
 *
 * Installation:
 * npm install express cors dotenv multer @aws-sdk/client-s3 @aws-sdk/s3-request-presigner
 *
 * Usage:
 * 1. Create a .env file with your configuration (optional)
 * 2. Run: node server.js or npm start
 * 3. API will be available at http://localhost:4000
 */

import express from "express";
import cors from "cors";
import dotenv from "dotenv";
import multer from "multer";
import {
  S3Client,
  ListBucketsCommand,
  CreateBucketCommand,
  DeleteBucketCommand,
  ListObjectsV2Command,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
  DeleteObjectsCommand,
  HeadObjectCommand,
  CopyObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { Readable } from "stream";

// Load environment variables from .env file
dotenv.config();

// Initialize Express app
const app = express();

// Middleware configuration
app.use(cors()); // Enable CORS for all origins
app.use(express.json({ limit: "50mb" })); // Parse JSON bodies, increased limit for base64 uploads
app.use(express.urlencoded({ extended: true })); // Parse URL-encoded bodies

// Configure multer for file uploads (stores files in memory)
const upload = multer({
  storage: multer.memoryStorage(),
  limits: {
    fileSize: 100 * 1024 * 1024, // 100MB max file size
  },
});

/**
 * S3 Client Configuration
 *
 * Environment variables (optional):
 * - LOCALSTACK_ENDPOINT: LocalStack endpoint (default: http://localhost:4566)
 * - AWS_REGION: AWS region (default: us-east-1)
 * - AWS_ACCESS_KEY_ID: Access key (default: test)
 * - AWS_SECRET_ACCESS_KEY: Secret key (default: test)
 */
const s3 = new S3Client({
  region: process.env.AWS_REGION || "us-east-1",
  endpoint: process.env.LOCALSTACK_ENDPOINT || "http://localhost:4566",
  forcePathStyle: true, // Required for LocalStack
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID || "test",
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY || "test",
  },
});

// ============================================================================
// HEALTH CHECK ENDPOINTS
// ============================================================================

/**
 * GET /api/health
 *
 * Check API and LocalStack connection health
 *
 * Response:
 * - 200: { status: "healthy", localstack: "connected", timestamp: "..." }
 * - 503: { status: "unhealthy", error: "...", timestamp: "..." }
 *
 * Example:
 * curl http://localhost:4000/api/health
 */
app.get("/api/health", async (_req, res) => {
  try {
    // Try to list buckets to verify LocalStack connection
    await s3.send(new ListBucketsCommand({}));
    res.json({
      status: "healthy",
      localstack: "connected",
      timestamp: new Date().toISOString(),
    });
  } catch (err) {
    console.error("Health check failed:", err);
    res.status(503).json({
      status: "unhealthy",
      error: err.message,
      timestamp: new Date().toISOString(),
    });
  }
});

// ============================================================================
// BUCKET ENDPOINTS
// ============================================================================

/**
 * GET /api/buckets
 *
 * List all S3 buckets
 *
 * Response:
 * - 200: Array of bucket objects
 *   [{ Name: "bucket-name", CreationDate: "2024-01-01T00:00:00.000Z" }, ...]
 * - 500: { error: "Failed to list buckets" }
 *
 * Example:
 * curl http://localhost:4000/api/buckets
 */
app.get("/api/buckets", async (_req, res) => {
  try {
    const response = await s3.send(new ListBucketsCommand({}));
    res.json(response.Buckets ?? []);
  } catch (err) {
    console.error("List buckets error:", err);
    res.status(500).json({ error: "Failed to list buckets" });
  }
});

/**
 * POST /api/buckets
 *
 * Create a new S3 bucket
 *
 * Request Body:
 * - name: string (required) - Bucket name (must be globally unique)
 *
 * Response:
 * - 201: { message: "Bucket created", name: "bucket-name" }
 * - 400: { error: "Missing bucket name" }
 * - 409: { error: "Bucket already exists" }
 * - 500: { error: "Failed to create bucket" }
 *
 * Example:
 * curl -X POST http://localhost:4000/api/buckets \
 *   -H "Content-Type: application/json" \
 *   -d '{"name": "my-new-bucket"}'
 */
app.post("/api/buckets", async (req, res) => {
  const { name } = req.body;

  if (!name) {
    return res.status(400).json({ error: "Missing bucket name" });
  }

  // Validate bucket name (S3 naming rules)
  const bucketNameRegex = /^[a-z0-9][a-z0-9.-]*[a-z0-9]$/;
  if (!bucketNameRegex.test(name) || name.length < 3 || name.length > 63) {
    return res.status(400).json({
      error:
        "Invalid bucket name. Must be 3-63 characters, lowercase letters, numbers, hyphens, and periods only",
    });
  }

  try {
    await s3.send(new CreateBucketCommand({ Bucket: name }));
    res.status(201).json({ message: "Bucket created", name });
  } catch (err) {
    if (
      err.name === "BucketAlreadyExists" ||
      err.name === "BucketAlreadyOwnedByYou"
    ) {
      res.status(409).json({ error: "Bucket already exists" });
    } else {
      console.error("Create bucket error:", err);
      res.status(500).json({ error: "Failed to create bucket" });
    }
  }
});

/**
 * DELETE /api/buckets/:bucket
 *
 * Delete an S3 bucket (must be empty)
 *
 * Parameters:
 * - bucket: string - Bucket name
 *
 * Response:
 * - 204: No content (success)
 * - 409: { error: "Bucket not empty" }
 * - 500: { error: "Failed to delete bucket" }
 *
 * Example:
 * curl -X DELETE http://localhost:4000/api/buckets/my-bucket
 */
app.delete("/api/buckets/:bucket", async (req, res) => {
  const { bucket } = req.params;

  try {
    await s3.send(new DeleteBucketCommand({ Bucket: bucket }));
    res.sendStatus(204);
  } catch (err) {
    if (err.name === "BucketNotEmpty") {
      res.status(409).json({ error: "Bucket not empty" });
    } else {
      console.error("Delete bucket error:", err);
      res.status(500).json({ error: "Failed to delete bucket" });
    }
  }
});

// ============================================================================
// OBJECT LISTING ENDPOINTS
// ============================================================================

/**
 * GET /api/buckets/:bucket/objects
 *
 * List objects in a bucket with optional prefix filtering and pagination
 *
 * Parameters:
 * - bucket: string - Bucket name
 *
 * Query Parameters:
 * - prefix: string (optional) - Filter objects by prefix (for folder navigation)
 * - delimiter: string (optional) - Delimiter for grouping (default: "/")
 * - token: string (optional) - Continuation token for pagination
 * - maxKeys: number (optional) - Maximum number of objects to return (default: 100, max: 1000)
 *
 * Response:
 * - 200: {
 *     Contents: [{ Key: "file.txt", Size: 1234, LastModified: "...", ETag: "..." }, ...],
 *     CommonPrefixes: [{ Prefix: "folder/" }, ...],
 *     IsTruncated: boolean,
 *     NextContinuationToken: "...",
 *     KeyCount: number
 *   }
 * - 500: { error: "Failed to list objects" }
 *
 * Example:
 * curl "http://localhost:4000/api/buckets/my-bucket/objects?prefix=photos/&maxKeys=50"
 */
app.get("/api/buckets/:bucket/objects", async (req, res) => {
  const { bucket } = req.params;
  const { prefix = "", delimiter = "/", token, maxKeys = "100" } = req.query;

  try {
    const response = await s3.send(
      new ListObjectsV2Command({
        Bucket: bucket,
        Prefix: prefix || undefined,
        Delimiter: delimiter || undefined,
        ContinuationToken: token || undefined,
        MaxKeys: Math.min(parseInt(maxKeys), 1000), // Cap at 1000
      })
    );

    res.json({
      Contents: response.Contents ?? [],
      CommonPrefixes: response.CommonPrefixes ?? [],
      IsTruncated: response.IsTruncated ?? false,
      NextContinuationToken: response.NextContinuationToken,
      KeyCount: response.KeyCount ?? 0,
    });
  } catch (err) {
    console.error("List objects error:", err);
    res.status(500).json({ error: "Failed to list objects" });
  }
});

// ============================================================================
// OBJECT OPERATIONS ENDPOINTS
// ============================================================================

/**
 * HEAD /api/buckets/:bucket/object
 *
 * Get object metadata without downloading the object
 *
 * Query Parameters:
 * - key: string (required) - Object key (URL encoded)
 *
 * Response:
 * - 200: {
 *     ContentType: "image/png",
 *     ContentLength: 12345,
 *     LastModified: "2024-01-01T00:00:00.000Z",
 *     ETag: "\"abc123\"",
 *     Metadata: { ... }
 *   }
 * - 404: { error: "Object not found" }
 *
 * Example:
 * curl -I "http://localhost:4000/api/buckets/my-bucket/object?key=photos%2Fvacation.jpg"
 */
app.head("/api/buckets/:bucket/object", async (req, res) => {
  const { bucket } = req.params;
  const key = req.query.key;

  if (!key) {
    return res.status(400).json({ error: "Missing object key" });
  }

  try {
    const response = await s3.send(
      new HeadObjectCommand({
        Bucket: bucket,
        Key: decodeURIComponent(key),
      })
    );

    res.set({
      "Content-Type": response.ContentType,
      "Content-Length": response.ContentLength?.toString(),
      "Last-Modified": response.LastModified?.toISOString(),
      ETag: response.ETag,
    });
    res.sendStatus(200);
  } catch (err) {
    if (err.name === "NotFound") {
      res.status(404).json({ error: "Object not found" });
    } else {
      console.error("Head object error:", err);
      res.status(500).json({ error: "Failed to get object metadata" });
    }
  }
});

/**
 * GET /api/buckets/:bucket/presign
 *
 * Generate a presigned URL for secure, temporary access to an object
 *
 * Query Parameters:
 * - key: string (required) - Object key (URL encoded)
 * - expires: number (optional) - URL expiration in seconds (default: 300, max: 3600)
 *
 * Response:
 * - 200: { url: "https://..." }
 * - 400: { error: "Missing object key" }
 * - 404: { error: "Object not found" }
 *
 * Example:
 * curl "http://localhost:4000/api/buckets/my-bucket/presign?key=document.pdf&expires=600"
 */
app.get("/api/buckets/:bucket/presign", async (req, res) => {
  const { bucket } = req.params;
  const key = req.query.key;
  const expires = Math.min(parseInt(req.query.expires || "300"), 3600);

  if (!key) {
    return res.status(400).json({ error: "Missing object key" });
  }

  try {
    const url = await getSignedUrl(
      s3,
      new GetObjectCommand({ Bucket: bucket, Key: decodeURIComponent(key) }),
      { expiresIn: expires }
    );
    res.json({ url });
  } catch (err) {
    console.error("Presign error:", err);
    res.status(404).json({ error: "Object not found" });
  }
});

/**
 * GET /api/buckets/:bucket/download
 *
 * Direct download/stream an object from S3
 *
 * Query Parameters:
 * - key: string (required) - Object key (URL encoded)
 * - attachment: boolean (optional) - Force download instead of inline display
 *
 * Response:
 * - 200: Binary file content with appropriate headers
 * - 404: { error: "Object not found" }
 *
 * Example:
 * curl -O "http://localhost:4000/api/buckets/my-bucket/download?key=document.pdf&attachment=true"
 */
app.get("/api/buckets/:bucket/download", async (req, res) => {
  const { bucket } = req.params;
  const key = req.query.key;
  const attachment = req.query.attachment === "true";

  if (!key) {
    return res.status(400).json({ error: "Missing object key" });
  }

  try {
    const decodedKey = decodeURIComponent(key);
    const response = await s3.send(
      new GetObjectCommand({
        Bucket: bucket,
        Key: decodedKey,
      })
    );

    // Set response headers
    res.setHeader(
      "Content-Type",
      response.ContentType || "application/octet-stream"
    );
    res.setHeader("Content-Length", response.ContentLength);

    if (attachment) {
      const filename = decodedKey.split("/").pop();
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${filename}"`
      );
    }

    // Stream the object body to response
    if (response.Body instanceof Readable) {
      response.Body.pipe(res);
    } else {
      res.send(response.Body);
    }
  } catch (err) {
    if (err.name === "NoSuchKey") {
      res.status(404).json({ error: "Object not found" });
    } else {
      console.error("Download error:", err);
      res.status(500).json({ error: "Failed to download object" });
    }
  }
});

// ============================================================================
// UPLOAD ENDPOINTS
// ============================================================================

/**
 * POST /api/buckets/:bucket/upload
 *
 * Upload a file using multipart/form-data (recommended for large files)
 *
 * Parameters:
 * - bucket: string - Bucket name
 *
 * Form Data:
 * - file: File (required) - File to upload
 * - key: string (required) - Object key/path in S3
 * - metadata: JSON string (optional) - Custom metadata
 *
 * Response:
 * - 201: { message: "Upload successful", key: "...", size: 12345 }
 * - 400: { error: "Missing file or key" }
 * - 500: { error: "Upload failed" }
 *
 * Example:
 * curl -X POST http://localhost:4000/api/buckets/my-bucket/upload \
 *   -F "file=@/path/to/file.pdf" \
 *   -F "key=documents/file.pdf"
 */
app.post(
  "/api/buckets/:bucket/upload",
  upload.single("file"),
  async (req, res) => {
    const { bucket } = req.params;
    const { key, metadata } = req.body;
    const file = req.file;

    if (!key || !file) {
      return res.status(400).json({ error: "Missing file or key" });
    }

    try {
      // Parse metadata if provided
      let parsedMetadata = {};
      if (metadata) {
        try {
          parsedMetadata = JSON.parse(metadata);
        } catch (e) {
          console.warn("Invalid metadata JSON:", e);
        }
      }

      await s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: file.buffer,
          ContentType: file.mimetype,
          Metadata: parsedMetadata,
        })
      );

      res.status(201).json({
        message: "Upload successful",
        key,
        size: file.size,
      });
    } catch (err) {
      console.error("Upload error:", err);
      res.status(500).json({ error: "Upload failed" });
    }
  }
);

/**
 * POST /api/buckets/:bucket/upload-base64
 *
 * Upload a file using base64 encoded content (for small files)
 *
 * Parameters:
 * - bucket: string - Bucket name
 *
 * Request Body:
 * - key: string (required) - Object key/path in S3
 * - content: string (required) - Base64 encoded file content
 * - contentType: string (optional) - MIME type (default: application/octet-stream)
 * - metadata: object (optional) - Custom metadata
 *
 * Response:
 * - 201: { message: "Upload successful", key: "..." }
 * - 400: { error: "Missing key or content" }
 * - 500: { error: "Upload failed" }
 *
 * Example:
 * curl -X POST http://localhost:4000/api/buckets/my-bucket/upload-base64 \
 *   -H "Content-Type: application/json" \
 *   -d '{
 *     "key": "text-file.txt",
 *     "content": "SGVsbG8gV29ybGQh",
 *     "contentType": "text/plain"
 *   }'
 */
app.post("/api/buckets/:bucket/upload-base64", async (req, res) => {
  const { bucket } = req.params;
  const { key, content, contentType, metadata } = req.body || {};

  if (!key || !content) {
    return res.status(400).json({ error: "Missing key or content" });
  }

  try {
    const buffer = Buffer.from(content, "base64");

    await s3.send(
      new PutObjectCommand({
        Bucket: bucket,
        Key: key,
        Body: buffer,
        ContentType: contentType || "application/octet-stream",
        Metadata: metadata || {},
      })
    );

    res.status(201).json({
      message: "Upload successful",
      key,
    });
  } catch (err) {
    console.error("Upload error:", err);
    res.status(500).json({ error: "Upload failed" });
  }
});

/**
 * POST /api/buckets/:bucket/copy
 *
 * Copy an object within or between buckets
 *
 * Request Body:
 * - sourceKey: string (required) - Source object key
 * - targetKey: string (required) - Target object key
 * - targetBucket: string (optional) - Target bucket (default: same bucket)
 *
 * Response:
 * - 201: { message: "Copy successful", targetKey: "..." }
 * - 400: { error: "Missing required parameters" }
 * - 404: { error: "Source object not found" }
 * - 500: { error: "Copy failed" }
 *
 * Example:
 * curl -X POST http://localhost:4000/api/buckets/my-bucket/copy \
 *   -H "Content-Type: application/json" \
 *   -d '{
 *     "sourceKey": "original.jpg",
 *     "targetKey": "backup/copy.jpg"
 *   }'
 */
app.post("/api/buckets/:bucket/copy", async (req, res) => {
  const { bucket } = req.params;
  const { sourceKey, targetKey, targetBucket } = req.body;

  if (!sourceKey || !targetKey) {
    return res.status(400).json({ error: "Missing required parameters" });
  }

  try {
    await s3.send(
      new CopyObjectCommand({
        CopySource: `${bucket}/${sourceKey}`,
        Bucket: targetBucket || bucket,
        Key: targetKey,
      })
    );

    res.status(201).json({
      message: "Copy successful",
      targetKey,
    });
  } catch (err) {
    if (err.name === "NoSuchKey") {
      res.status(404).json({ error: "Source object not found" });
    } else {
      console.error("Copy error:", err);
      res.status(500).json({ error: "Copy failed" });
    }
  }
});

// ============================================================================
// DELETE ENDPOINTS
// ============================================================================

/**
 * DELETE /api/buckets/:bucket/object
 *
 * Delete a single object from S3
 *
 * Query Parameters:
 * - key: string (required) - Object key (URL encoded)
 *
 * Response:
 * - 204: No content (success)
 * - 400: { error: "Missing object key" }
 * - 500: { error: "Delete failed" }
 *
 * Example:
 * curl -X DELETE "http://localhost:4000/api/buckets/my-bucket/object?key=old-file.txt"
 */
app.delete("/api/buckets/:bucket/object", async (req, res) => {
  const { bucket } = req.params;
  const key = req.query.key;

  if (!key) {
    return res.status(400).json({ error: "Missing object key" });
  }

  try {
    await s3.send(
      new DeleteObjectCommand({
        Bucket: bucket,
        Key: decodeURIComponent(key),
      })
    );
    res.sendStatus(204);
  } catch (err) {
    console.error("Delete error:", err);
    res.status(500).json({ error: "Delete failed" });
  }
});

/**
 * POST /api/buckets/:bucket/delete-multiple
 *
 * Delete multiple objects in a single request
 *
 * Request Body:
 * - keys: string[] (required) - Array of object keys to delete
 *
 * Response:
 * - 200: {
 *     deleted: ["key1", "key2", ...],
 *     errors: [{ key: "key3", error: "..." }, ...]
 *   }
 * - 400: { error: "Missing or invalid keys array" }
 * - 500: { error: "Batch delete failed" }
 *
 * Example:
 * curl -X POST http://localhost:4000/api/buckets/my-bucket/delete-multiple \
 *   -H "Content-Type: application/json" \
 *   -d '{"keys": ["file1.txt", "folder/file2.txt", "image.jpg"]}'
 */
app.post("/api/buckets/:bucket/delete-multiple", async (req, res) => {
  const { bucket } = req.params;
  const { keys } = req.body;

  if (!keys || !Array.isArray(keys) || keys.length === 0) {
    return res.status(400).json({ error: "Missing or invalid keys array" });
  }

  try {
    const response = await s3.send(
      new DeleteObjectsCommand({
        Bucket: bucket,
        Delete: {
          Objects: keys.map((key) => ({ Key: key })),
          Quiet: false,
        },
      })
    );

    res.json({
      deleted: response.Deleted?.map((d) => d.Key) ?? [],
      errors:
        response.Errors?.map((e) => ({
          key: e.Key,
          error: e.Message,
        })) ?? [],
    });
  } catch (err) {
    console.error("Batch delete error:", err);
    res.status(500).json({ error: "Batch delete failed" });
  }
});

// ============================================================================
// ERROR HANDLING MIDDLEWARE
// ============================================================================

/**
 * Global error handler
 * Catches any unhandled errors and returns a consistent error response
 */
app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);
  res.status(500).json({
    error: "Internal server error",
    message: process.env.NODE_ENV === "development" ? err.message : undefined,
  });
});

/**
 * 404 handler for undefined routes
 */
app.use((req, res) => {
  res.status(404).json({
    error: "Endpoint not found",
    path: req.path,
    method: req.method,
  });
});

// ============================================================================
// SERVER STARTUP
// ============================================================================

const PORT = process.env.PORT || 4000;
const HOST = process.env.HOST || "localhost";

app.listen(PORT, HOST, () => {
  console.log(`
╔════════════════════════════════════════════════════════╗
║         LocalStack S3 UI Backend API                  ║
╠════════════════════════════════════════════════════════╣
║  Server:     http://${HOST}:${PORT}                       ║
║  LocalStack: ${
    process.env.LOCALSTACK_ENDPOINT || "http://localhost:4566"
  }          ║
║  Region:     ${
    process.env.AWS_REGION || "us-east-1"
  }                             ║
╠════════════════════════════════════════════════════════╣
║  API Endpoints:                                        ║
║  - GET    /api/health                                  ║
║  - GET    /api/buckets                                 ║
║  - POST   /api/buckets                                 ║
║  - DELETE /api/buckets/:bucket                         ║
║  - GET    /api/buckets/:bucket/objects                 ║
║  - HEAD   /api/buckets/:bucket/object                  ║
║  - GET    /api/buckets/:bucket/presign                 ║
║  - GET    /api/buckets/:bucket/download                ║
║  - POST   /api/buckets/:bucket/upload                  ║
║  - POST   /api/buckets/:bucket/upload-base64           ║
║  - POST   /api/buckets/:bucket/copy                    ║
║  - DELETE /api/buckets/:bucket/object                  ║
║  - POST   /api/buckets/:bucket/delete-multiple         ║
╚════════════════════════════════════════════════════════╝
  `);
});

// Handle graceful shutdown
process.on("SIGTERM", () => {
  console.log("SIGTERM signal received: closing HTTP server");
  app.close(() => {
    console.log("HTTP server closed");
  });
});

export default app;
