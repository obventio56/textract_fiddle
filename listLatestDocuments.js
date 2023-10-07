import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";
import fs from "fs";

const s3Client = new S3Client({});
const bucketName = "unstructured-api-images"; // Replace with your bucket name

const input = {
  // ListObjectsRequest
  Bucket: bucketName, // required
};

const command = new ListObjectsV2Command(input);
const response = await s3Client.send(command);

let res = [];
let ContinuationToken = null;
while (true) {
  let command = {
    // ListObjectsRequest
    Bucket: bucketName, // required
  };
  if (ContinuationToken) {
    command.ContinuationToken = ContinuationToken;
  }

  const page = await s3Client.send(new ListObjectsV2Command(command));

  if (page.NextContinuationToken) {
    ContinuationToken = page.NextContinuationToken;
  }

  res = [...res, ...page.Contents];
  if (!page.IsTruncated) {
    break;
  }
}

res.sort((a, b) => {
  return b.LastModified - a.LastModified;
});

fs.writeFileSync("latestDocuments.json", JSON.stringify(res, null, 2));
