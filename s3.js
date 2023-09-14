import axios from "axios";
import { v4 } from "uuid";
import mime from "mime";
import {
  CreateMultipartUploadCommand,
  UploadPartCommand,
  CompleteMultipartUploadCommand,
  AbortMultipartUploadCommand,
  S3Client,
} from "@aws-sdk/client-s3";

const BUCKET_NAME = "unstructured-api-images";
const MIN_CHUNK_SIZE = 5 * 1024 * 1024; // 5MB

const s3Client = new S3Client({});

const uploadFileFromUrl = async (remoteURL) => {
  // Get mime ahead of time
  const responseHead = await axios.head(remoteURL);
  const contentType = responseHead.headers["content-type"];
  const extension = mime.getExtension(contentType);

  let uploadId;
  let partNumber = 1;
  const partUploads = [];
  const key = `${v4()}.${extension}`;

  try {
    // Start multipart upload
    const multipartUpload = await s3Client.send(
      new CreateMultipartUploadCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        ContentType: contentType,
      })
    );

    uploadId = multipartUpload.UploadId;

    // Fetch remote file and upload chunks
    const response = await axios.get(remoteURL, { responseType: "stream" });

    await new Promise((resolve, reject) => {
      let buffer = Buffer.alloc(0);

      response.data.on("data", async (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);

        if (buffer.length >= MIN_CHUNK_SIZE) {
          // Pause the stream to handle this chunk
          response.data.pause();

          try {
            const partUpload = await s3Client.send(
              new UploadPartCommand({
                Bucket: BUCKET_NAME,
                Key: key,
                UploadId: uploadId,
                Body: buffer,
                PartNumber: partNumber,
              })
            );

            partUploads.push({
              ETag: partUpload.ETag,
              PartNumber: partNumber,
            });

            console.log("Part", partNumber, "uploaded");
            partNumber++;

            // Reset buffer
            buffer = Buffer.alloc(0);

            // Resume the stream for the next chunk
            response.data.resume();
          } catch (error) {
            reject(error);
          }
        }
      });

      response.data.on("end", async () => {
        // If there's remaining data in the buffer, upload it as the final part
        if (buffer.length > 0) {
          try {
            const partUpload = await s3Client.send(
              new UploadPartCommand({
                Bucket: BUCKET_NAME,
                Key: key,
                UploadId: uploadId,
                Body: buffer,
                PartNumber: partNumber,
              })
            );

            partUploads.push({
              ETag: partUpload.ETag,
              PartNumber: partNumber,
            });

            console.log("Final Part", partNumber, "uploaded");
          } catch (error) {
            reject(error);
          }
        }

        resolve();
      });

      response.data.on("error", reject);
    });

    // Complete the multipart upload
    await s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: partUploads,
        },
      })
    );
  } catch (err) {
    console.error(err);

    // If there was an error, abort the multipart upload
    if (uploadId) {
      await s3Client.send(
        new AbortMultipartUploadCommand({
          Bucket: BUCKET_NAME,
          Key: key,
          UploadId: uploadId,
        })
      );
    }
  }
  return key;
};

const uploadFileFromByteString = async (base64String, mimeType) => {
  const s3Client = new S3Client({});
  let uploadId;
  let partNumber = 1;
  const partUploads = [];
  // Convert the base64 string to a buffer
  const fileBuffer = Buffer.from(base64String, "base64");

  // Determine file extension
  const extension = mime.getExtension(mimeType);
  const key = `${v4()}.${extension}`;

  try {
    // 1. Start the multipart upload with the content type
    const multipartUpload = await s3Client.send(
      new CreateMultipartUploadCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        ContentType: mimeType,
      })
    );

    uploadId = multipartUpload.UploadId;

    // 2. Split the buffer into chunks and upload each part
    for (let offset = 0; offset < fileBuffer.length; offset += MIN_CHUNK_SIZE) {
      const end = Math.min(offset + MIN_CHUNK_SIZE, fileBuffer.length);
      const partBuffer = fileBuffer.slice(offset, end);

      const partUpload = await s3Client.send(
        new UploadPartCommand({
          Bucket: BUCKET_NAME,
          Key: key,
          UploadId: uploadId,
          Body: partBuffer,
          PartNumber: partNumber,
        })
      );

      partUploads.push({
        ETag: partUpload.ETag,
        PartNumber: partNumber,
      });

      console.log("Part", partNumber, "uploaded");
      partNumber++;
    }

    // 3. Complete the multipart upload
    await s3Client.send(
      new CompleteMultipartUploadCommand({
        Bucket: BUCKET_NAME,
        Key: key,
        UploadId: uploadId,
        MultipartUpload: {
          Parts: partUploads,
        },
      })
    );

    console.log("File uploaded successfully!");
  } catch (err) {
    console.error("Error uploading file:", err);

    // If there was an error, abort the multipart upload
    if (uploadId) {
      await s3Client.send(
        new AbortMultipartUploadCommand({
          Bucket: BUCKET_NAME,
          Key: key,
          UploadId: uploadId,
        })
      );
    }
  }
  return key;
};

export { uploadFileFromByteString, uploadFileFromUrl };
