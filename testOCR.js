import { bb2Layout } from "./bb_2_layout.js";
import {
  getAWSOCRResponse,
  getAzureOCRResponse,
  getOCRDocument,
} from "./index.js";
import "dotenv/config";
import fs from "fs";
// const s3DownloadUrlPrefix =
//   "https://f155-18-117-160-75.ngrok-free.app/downloadDocument?key=";
const chineseInvoiceS3Id = "9bcf6719-b3d0-4ae4-a0cf-50f93b0dfb0c.pdf";
const receiptPhotoS3Id = "fd07426a-1961-4078-a6a5-790751f462ca.jpg";
const johnsPorfolio = "2758c016-51f2-40d5-9241-e7f289ccfb8b.pdf";
const res = await getOCRDocument(johnsPorfolio);

// const testDataRaw = fs.readFileSync("./testNewOCR.json");
// const { awsResponse, azureResponse } = JSON.parse(testDataRaw);

// const res = await bb2Layout(awsResponse, azureResponse);

console.log(res);
