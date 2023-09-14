import {
  TextractClient,
  AnalyzeDocumentCommand,
  StartDocumentAnalysisCommand,
  GetDocumentAnalysisCommand,
} from "@aws-sdk/client-textract"; // ES Modules import
import { re } from "mathjs";
// import { readFileSync, writeFileSync } from "fs";
import { bb2Layout } from "./bb_2_layout.js";
import { chatAPI } from "./openAI.js";
import { uploadFileFromByteString, uploadFileFromUrl } from "./s3.js";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import { readFileSync } from "fs";
const client = new TextractClient();

const BUCKET_NAME = "unstructured-api-images";

// const res = await uploadFileFromUrl(
//   "https://drive.google.com/uc?export=download&id=1eMiMjPlqIcAnwPT6gc71ImOhO3Snm59o"
// );

const exampleShape = {
  total: {
    type: "number",
    description:
      "Total amount of the invoice. Don't include any currency symbols.",
  },
  date: {
    type: "string",
    description:
      "Date of the invoice. Format is YYYY-MM-DD. For example, 2021-01-31.",
  },
  vendor: {
    type: "string",
    description:
      "Name of the vendor. This is the name of the company that issued the invoice or receipt",
  },
  items: {
    type: "array",
    description: "The line items included in the invoice or receipt",
    items: {
      type: "object",
      properties: {
        name: { type: "string", description: "The name of this item" },
        price: { type: "number", description: "The price of this item" },
      },
    },
  },
};

// const res = await uploadFileFromByteString(base64ByteString, "image/jpeg");

export const getQueryResponses = async (text, shape) => {
  const res = await chatAPI(
    [
      {
        role: "system",
        content: `You are an expert at data entry and document analysis helping me correctly extract information from documents to call an external API.
You will be given a plain-text representation of a document that was created using OCR. We have done our best to maintain layout and formatting in the plain-text representation, but it may not be perfect.
Your job is to use the provided document to call an external API via the callAPI function. You must extract all the information as necessary to call the function. 
If you cannot find a piece of information, it is ok to return null.`,
      },
      {
        role: "user",
        content: `Here is a plain-text representation of a document. Use this to call the callAPI function.
Document:
${text}
    `,
      },
    ],
    [
      {
        name: "callAPI",
        description: "Call external api with data extract from document",
        parameters: {
          type: "object",
          // Convert shape to parameter format
          properties: shape,
        },
      },
    ]
  );

  const args = res.choices?.[0]?.message?.["function_call"]?.arguments;
  if (!args) {
    console.log("No arguments found");
    return;
  }
  return args;
};

// This must be terrible but let's try it.
const awaitOCRResult = (jobId) => {
  return new Promise((resolve, reject) => {
    const interval = setInterval(async () => {
      const command = new GetDocumentAnalysisCommand({ JobId: jobId });
      const res = await client.send(command);
      console.log("Polling result", res.JobStatus);

      if (res.JobStatus === "IN_PROGRESS") {
        return;
      }

      // Stop polling
      clearInterval(interval);

      if (res.JobStatus === "FAILED") {
        reject(res);
        return;
      }

      if (res.JobStatus === "SUCCEEDED") {
        // Handle pagination
        let nextToken = res.NextToken;
        let blocks = res.Blocks;

        // Get all blocks
        while (!!nextToken) {
          const command = new GetDocumentAnalysisCommand({
            JobId: jobId,
            NextToken: nextToken,
          });
          const pageRes = await client.send(command);
          blocks = [...blocks, ...pageRes.Blocks];
          nextToken = pageRes.NextToken;
        }

        resolve(blocks);
      }
    }, 2000);
  });
};

// Get text in approximate plain text layout from document in S3
export const getOCRDocument = async (fileKey) => {
  const input = {
    // AnalyzeDocumentRequest
    DocumentLocation: {
      // Document
      // Bytes: testImage,
      S3Object: {
        // S3Object
        Bucket: BUCKET_NAME, // required
        Name: fileKey,
      },
    },
    FeatureTypes: ["TABLES"],
    //   HumanLoopConfig: {
    //     // HumanLoopConfig
    //     HumanLoopName: "STRING_VALUE", // required
    //     FlowDefinitionArn: "STRING_VALUE", // required
    //     DataAttributes: {
    //       // HumanLoopDataAttributes
    //       ContentClassifiers: [
    //         // ContentClassifiers
    //         "FreeOfPersonallyIdentifiableInformation" || "FreeOfAdultContent",
    //       ],
    //     },
    //   },
    //   QueriesConfig: {
    //     // QueriesConfig
    //     Queries: [
    //       // Queries // required
    //       {
    //         // Query
    //         Text: "STRING_VALUE", // required
    //         Alias: "STRING_VALUE",
    //         Pages: [
    //           // QueryPages
    //           "STRING_VALUE",
    //         ],
    //       },
    //     ],
    //   },
  };

  console.log("Starting document analysis");
  const command = new StartDocumentAnalysisCommand(input);
  const { JobId: jobId } = await client.send(command);
  console.log(jobId);
  const blocks = await awaitOCRResult(jobId);
  const text = await bb2Layout({ Blocks: blocks });
  return text;
};

export const handler = async (event) => {
  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_SERVICE_ROLE,
    { auth: { persistSession: false } }
  );
  console.log(event);

  const {
    contentUrl,
    contentByteString,
    content,
    shape,
    contentMimeType,
    jobId,
    callbackUrl,
  } = event;
  try {
    // const authorization = event?.headers?.["authorization"];
    // const apiKey = authorization?.split(" ")?.[1];

    // // No API key passed
    // if (!apiKey) {
    //   throw new Error("No API key provided");
    // }

    // const { data, error } = await supabase
    //   .from("api_keys")
    //   .select("id")
    //   .eq("key", apiKey);

    // // Error accessing API key
    // if (error) {
    //   throw new Error(error.message);
    // }

    // // No record matches passed API key
    // if (!data || data.length === 0) {
    //   throw new Error("Invalid API key");
    // }

    // if (!event.headers?.["content-type"]?.includes("application/json")) {
    //   throw new Error("Invalid content type");
    // }

    // const { contentUrl, contentByteString, content, shape, contentMimeType } =
    //   JSON.parse(event.body);

    // Update job to status "PROCESSING"
    await supabase
      .from("jobs")
      .update({ status: "PROCESSING" })
      .eq("id", jobId);

    // If they passed plain text, we don't need to do OCR
    if (content) {
      return await getQueryResponses(content, shape);
    }

    // upload document to s3 and set key
    let key;
    if (contentUrl) {
      key = await uploadFileFromUrl(contentUrl);
    } else if (contentByteString && contentMimeType) {
      key = await uploadFileFromByteString(contentByteString, contentMimeType);
    }

    // Get text in approximate plain text layout from document in S3
    const text = await getOCRDocument(key);
    // Extract queries from document using OpenAI
    const args = await getQueryResponses(text, shape);

    // Update job to status "COMPLETED" and set result to args
    await supabase
      .from("jobs")
      .update({ status: "COMPLETED", result: JSON.parse(args) })
      .eq("id", jobId);

    if (callbackUrl) {
      // Post results to callback url using axios
      await axios.post(callbackUrl, {
        result: JSON.parse(args),
        status: "COMPLETED",
        id: jobId,
      });
    }
  } catch (e) {
    await supabase
      .from("jobs")
      .update({ status: "ERROR", result: { ...e } })
      .eq("id", jobId);
  }

  return;
};

// const testImage = readFileSync("./small.pdf");
// const base64ByteString = testImage.toString("base64");
// const key = await uploadFileFromByteString(base64ByteString, "application/pdf");
// const text = await getOCRDocument(key);
// const args = await getQueryResponses(text, exampleShape);
// console.log(args);
// const testKey = "dde23afc-21ab-4f29-935c-3b43046ac3e2";

// const args = await handler({
//   authorization: `Bearer ${testKey}`,
//   contentUrl:
//     "https://drive.google.com/uc?export=download&id=1eMiMjPlqIcAnwPT6gc71ImOhO3Snm59o",
//   shape: exampleShape,
// });

// console.log("Result", args);
