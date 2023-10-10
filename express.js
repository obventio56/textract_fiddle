import express from "express";
import bodyParser from "body-parser";
import cors from "cors";
import { getExtraction, getOCRDocument } from "./index.js";
import {
  S3Client,
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  GetObjectCommand,
} from "@aws-sdk/client-s3";
import multer from "multer";
import { v4 } from "uuid";
import mime from "mime-types";
import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { processJob } from "./processJob.js";
import { preprocessJob } from "./preprocessJob.js";
import { OpenAIApi } from "./openAI.js";

const app = express();

// Set up middleware
app.use(cors()); // Allow all CORS requests
app.use(bodyParser.json({ limit: "50mb" })); // Set body size limit to 50mb

const storage = multer.memoryStorage(); // Store the file data in memory
const upload = multer({
  storage: storage,
  limits: { fileSize: 50 * 1024 * 1024 },
}); // 50mb

const fivemb = 5 * 1024 * 1024;

const s3Client = new S3Client({});
const bucketName = "unstructured-api-images"; // Replace with your bucket name
export const openAIApi = new OpenAIApi();

// Endpoints

app.get("/downloadDocument", async (req, res) => {
  const { key } = req.query;

  if (!key) {
    return res.status(400).send({ error: "Key is required." });
  }

  try {
    const { Body, ContentType } = await s3Client.send(
      new GetObjectCommand({
        Bucket: bucketName,
        Key: key,
      })
    );

    // Preserve the content type so browsers can recognize and handle the content correctly
    res.set("Content-Type", ContentType);

    // Stream the S3 object directly to the client
    Body.pipe(res);
  } catch (error) {
    console.error(error);
    res.status(500).send({ error: "Failed to download the document." });
  }
});

app.post("/documents", upload.array("documents"), async (req, res) => {
  const files = req.files;
  const uploadedKeys = [];

  for (let file of files) {
    const fileExtension = file.originalname.split(".").pop();
    const key = `${v4()}.${fileExtension}`;
    const contentType =
      mime.lookup(fileExtension) || "application/octet-stream";

    try {
      const { UploadId } = await s3Client.send(
        new CreateMultipartUploadCommand({
          Bucket: bucketName,
          Key: key,
          ContentType: contentType,
        })
      );

      const buffer = file.buffer;

      // Split the file into 5mb chunks
      const numParts = Math.ceil(buffer.length / fivemb);
      const uploadPromises = [];

      for (let i = 0; i < numParts; i++) {
        const start = i * fivemb;
        const end =
          start + fivemb >= buffer.length ? buffer.length : start + fivemb;

        uploadPromises.push(
          s3Client.send(
            new UploadPartCommand({
              Bucket: bucketName,
              Key: key,
              UploadId: UploadId,
              Body: buffer.subarray(start, end),
              PartNumber: i + 1,
            })
          )
        );
      }

      const uploadResults = await Promise.all(uploadPromises);

      await s3Client.send(
        new CompleteMultipartUploadCommand({
          Bucket: bucketName,
          Key: key,
          UploadId: UploadId,
          MultipartUpload: {
            Parts: uploadResults.map(({ ETag }, i) => ({
              ETag,
              PartNumber: i + 1,
            })),
          },
        })
      );

      uploadedKeys.push(key);
    } catch (err) {
      console.error(err);

      if (UploadId) {
        await s3Client.send(
          new AbortMultipartUploadCommand({
            Bucket: bucketName,
            Key: key,
            UploadId: UploadId,
          })
        );
      }

      return res.status(500).send({ error: "Failed to upload documents." });
    }
  }

  res.json({ uploadedKeys });
});

app.post("/getLayout", async (req, res) => {
  try {
    const { key: s3FileId } = req.body;
    const text = await getOCRDocument(s3FileId);

    // Your logic for getLayout
    res.json({ textLayout: text });
  } catch (e) {
    console.error(e);
    res.status(500).send({ error: "Failed to get layout." });
  }
});

app.post("/extract", async (req, res) => {
  try {
    const { shape, textLayout } = req.body;
    const args = await getExtraction(textLayout, shape);

    // Your logic for extract
    res.json({ results: args });
  } catch (e) {
    console.error(e);
    res.status(500).send({ error: "Failed to extract." });
  }
});

app.post("/processJob", async (req, res) => {
  try {
    const { shape, jobId } = req.body;

    const supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_PUBLIC_ANON_KEY
    );

    const record = await supabase.from("jobs").select().eq("id", jobId);
    const currentState = record.data[0].state;
    const newState = {
      ...currentState,
      shape,
      results: { extractionResults: {} },
    };

    const updatedRecord = await supabase
      .from("jobs")
      .insert({
        state: newState,
      })
      .select();

    res.json({ jobId: updatedRecord.data[0].id });

    await processJob(updatedRecord.data[0].id);
  } catch (e) {
    console.error(e);
    res.status(500).send({ error: "Failed to create job." });
  }
});

app.post("/preprocessJob", async (req, res) => {
  try {
    const { files } = req.body;

    const supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_PUBLIC_ANON_KEY
    );

    const record = await supabase
      .from("jobs")
      .insert({
        status: "PREPROCESSING",
        state: {
          rawFiles: files,
          rawTextLayouts: {},
        },
      })
      .select();

    res.json({ jobId: record.data[0].id });

    await preprocessJob(record.data[0].id);
  } catch (e) {
    console.error(e);
    res.status(500).send({ error: "Failed to create job." });
  }
});

// Global Error Handling
app.use((err, req, res, next) => {
  console.error(err.stack);
  res.status(500).send({ error: "Something went wrong!" });
});

// Start server
const PORT = 3006;
const server = app.listen(PORT, () => {
  console.log(`Server started on http://localhost:${PORT}`);
});

server.timeout = 120000; // Set the server timeout to 2 minutes
