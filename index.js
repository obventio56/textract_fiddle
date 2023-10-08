import {
  TextractClient,
  AnalyzeDocumentCommand,
  StartDocumentAnalysisCommand,
  GetDocumentAnalysisCommand,
} from "@aws-sdk/client-textract"; // ES Modules import
import { filter, re } from "mathjs";
// import { readFileSync, writeFileSync } from "fs";
import { bb2Layout } from "./bb_2_layout.js";
import { chatAPI } from "./openAI.js";
import { uploadFileFromByteString, uploadFileFromUrl } from "./s3.js";
import { createClient } from "@supabase/supabase-js";
import axios from "axios";
import { readFileSync } from "fs";
import { ComputerVisionClient } from "@azure/cognitiveservices-computervision";
import { ApiKeyCredentials } from "@azure/ms-rest-js";
const client = new TextractClient({ maxAttempts: 3 });
const BUCKET_NAME = "unstructured-api-images";

// A public URL for downloading files by S3 key
const fileDownloadUrlPrefix = `${process.env.PROD_URL}/downloadDocument?key=`;

const computerVisionClient = new ComputerVisionClient(
  new ApiKeyCredentials({
    inHeader: { "Ocp-Apim-Subscription-Key": process.env.AZURE_VISION_KEY },
  }),
  process.env.AZURE_VISION_ENDPOINT
);

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

const sleep = (ms) => {
  return new Promise((resolve) => setTimeout(resolve, ms));
};

const formatJSON = (json, shape, defaultValue = "NOT FOUND") => {
  // This should ensure the order of the keys is the same as the shape
  return Object.entries(shape).reduce((acc, [key, value]) => {
    // If it's expected to be an array, we need to map over it
    // If there are no values, we need to return an empty array
    if (value.type === "array") {
      return {
        ...acc,
        [key]: (json[key] || []).map((item) =>
          formatJSON(item, value.items.properties)
        ),
      };
    }

    // Otherwise just return the value
    return {
      ...acc,
      [key]: json[key] || defaultValue,
    };
  }, {});
};

// const res = await uploadFileFromByteString(base64ByteString, "image/jpeg");

/**
 * Do a first pass of each property in the shape and ask ChatGPT if it's present in the text.
 * Return a filtered shape with only the properties that are present.
 */
export const getFilteredShape = async (text, shape) => {
  // Convert each property in shape into a boolean type and adjust description to ask whether or not this property exists
  // For array, flatten the properties and construct a prompt asking if sub properties exist
  const translatedShape = Object.entries(shape).reduce((cum, [key, value]) => {
    if (value.type === "array") {
      // Reduce over all properties in array and flatten into cum
      const flattenedProperties = Object.entries(value.items.properties).reduce(
        (subCum, [subKey, subValue]) => {
          return {
            ...subCum,
            [`${key}_${subKey}`]: {
              type: "boolean",
              description: `The document might contain an array of information called "${key}" that is described as "${value.description}." If the document does contain this array, does each element of this array contain "${subValue.description}"?`,
            },
          };
        },
        {}
      );
      // Add to the cum
      return {
        ...cum,
        [key]: {
          type: "boolean",
          description: `Does the document contain a piece of information called "${key}" described as "${value.description}"?`,
        },
        ...flattenedProperties,
      };
    }

    return {
      ...cum,
      [key]: {
        type: "boolean",
        description: `Does the document contain a piece of information called "${key}" described as "${value.description}"?`,
      },
    };
  }, {});

  let res;
  try {
    res = await chatAPI(
      [
        {
          role: "system",
          content: `You are an expert accountant specializing in accounts payable. You are going to help me correctly determine what information is present in a provided invoice so that I can call an API in our ERP system.
          You will be given a plain-text representation of an invoice, receipt, or other bill document that was created using OCR. We have done our best to maintain layout and formatting in the plain-text representation, but it may not be perfect.
          Your job is to use the provided document to determine what information is present in order to call an external API via the callAPI function. You must determine the presence or absence of each piece of information in order to call the API. 
          For each piece of information you will respond with a boolean.`,
        },
        {
          role: "user",
          content: `Here is a plain-text representation of an invoice, receipt or other bill document. Use this to call the callAPI function.
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
            properties: translatedShape,
          },
        },
      ],
      "gpt-4", // Use gpt-4
      2 // Allow 2 attempts
    );
  } catch (e) {
    console.log("Error calling API", e);
    return { error: "Error calling API", rawResponse: "" };
  }

  const args = res.choices?.[0]?.message?.["function_call"]?.arguments;

  if (!args) {
    console.log("No arguments found");
    return { error: "No arguments found", rawResponse: "" };
  }

  let formattedResults;

  try {
    const rawJSON = JSON.parse(args);
    formattedResults = formatJSON(rawJSON, translatedShape, null);
  } catch (e) {
    return { error: "Error parsing argument JSON", rawResponse: args };
  }

  // Reduce original shape to add back in whether or not each property is present.
  // Handle arrays by finding key in formattedResults that starts with the array name and ends with the sub property name
  const shapeWithPresence = Object.entries(shape).reduce(
    (cum, [key, value]) => {
      if (value.type === "array") {
        // Reduce over all properties in array and flatten into cum
        const flattenedProperties = Object.entries(
          value.items.properties
        ).reduce((subCum, [subKey, subValue]) => {
          return {
            ...subCum,
            [subKey]: {
              ...subValue,
              presence: formattedResults[`${key}_${subKey}`],
            },
          };
        }, {});
        // Add to the cum
        return {
          ...cum,
          [key]: {
            ...value,
            presence: formattedResults[key],
            items: {
              ...value.items,
              properties: flattenedProperties,
            },
          },
        };
      }

      return {
        ...cum,
        [key]: {
          ...value,
          presence: formattedResults[key],
        },
      };
    },
    {}
  );

  return shapeWithPresence;
};

export const getQueryResponses = async (text, shape) => {
  // Filter shape so that it only contains properties that are present
  // Also remove presence property from shape
  const filteredShape = Object.entries(shape).reduce((cum, [key, value]) => {
    if (value.type === "array") {
      // Check all properties in items
      const filteredProperties = Object.entries(value.items.properties).reduce(
        (subCum, [subKey, subValue]) => {
          if (subValue.presence) {
            return {
              ...subCum,
              [subKey]: subValue,
            };
          }
          return subCum;
        }
      );
    }

    if (value.presence) {
      return {
        ...cum,
        [key]: value,
      };
    }

    return cum;
  }, {});

  // If there are no properties present, return empty object
  if (Object.keys(filteredShape).length === 0) {
    return formatJSON({}, shape);
  }

  let res;
  try {
    res = await chatAPI(
      [
        {
          role: "system",
          content: `You are an expert accountant specializing in accounts payable. You are going to help me correctly extract, summarize, and translate information from receipts, invoices, and other bills necessary to call an API to enter these transactions into our ERP system.
          You will be given a plain-text representation of an invoice, receipt, or other bill document that was created using OCR. We have done our best to maintain layout and formatting in the plain-text representation, but it may not be perfect.
          Your job is to use the provided document to call an external API via the callAPI function. You must extract all the information necessary to call the function. You may also be required to produce summaries and classifications in order to call the function.
          If you cannot find an answer or piece of information, it is ok to return null.`,
        },
        {
          role: "user",
          content: `Here is a plain-text representation of an invoice, receipt or other bill document. Use this to call the callAPI function.
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
            properties: filteredShape,
          },
        },
      ],
      "gpt-4", // Use gpt-4
      2 // Allow 2 attempts
    );
  } catch (e) {
    console.log("Error calling API", e);
    return { error: "Error calling API", rawResponse: "" };
  }

  const args = res.choices?.[0]?.message?.["function_call"]?.arguments;

  if (!args) {
    console.log("No arguments found");
    return { error: "No arguments found", rawResponse: "" };
  }

  try {
    const rawJSON = JSON.parse(args);

    // This will add back all original properties. Properties not in filtered shape will be null.
    return formatJSON(rawJSON, shape);
  } catch (e) {
    return { error: "Error parsing argument JSON", rawResponse: args };
  }
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

/**
 * Normalize all line coordinates from Azure response to be between 0 and 1
 * Also switch to AWS Polygon convention for lines
 */
const normalizeAzureCoordinates = (azureResponse) => {
  return azureResponse.readResults.reduce((cum, page) => {
    const pageWidth = page.width;
    const pageHeight = page.height;

    const lines = page.lines.map((line) => {
      const boundingBox = line.boundingBox;

      const normalizedBoundingBox = [
        { X: boundingBox[0] / pageWidth, Y: boundingBox[1] / pageHeight },
        { X: boundingBox[2] / pageWidth, Y: boundingBox[3] / pageHeight },
        { X: boundingBox[4] / pageWidth, Y: boundingBox[5] / pageHeight },
        { X: boundingBox[6] / pageWidth, Y: boundingBox[7] / pageHeight },
      ];

      return {
        text: line.text,
        Polygon: normalizedBoundingBox,
      };
    });

    return [...cum, { page: page.page, lines }];
  }, []);
};

export const getAzureOCRResponse = async (fileUrl) => {
  let result = await computerVisionClient.read(fileUrl);
  // Operation ID is last path segment of operationLocation (a URL)
  let operation = result.operationLocation.split("/").slice(-1)[0];

  // Wait for read recognition to complete
  // result.status is initially undefined, since it's the result of read
  while (result.status !== "succeeded") {
    await sleep(1000);
    result = await computerVisionClient.getReadResult(operation);
  }
  return normalizeAzureCoordinates(result.analyzeResult);
};

export const getAWSOCRResponse = async (fileKey) => {
  const input = {
    DocumentLocation: {
      S3Object: {
        Bucket: BUCKET_NAME,
        Name: fileKey,
      },
    },
    FeatureTypes: ["TABLES"],
  };

  console.log("Starting document analysis");
  const command = new StartDocumentAnalysisCommand(input);
  const { JobId: jobId } = await client.send(command, {});
  console.log(jobId);
  const blocks = await awaitOCRResult(jobId);
  return blocks;
};

// Get text in approximate plain text layout from document in S3
export const getOCRDocument = async (fileKey) => {
  const [awsResponse, azureResponse] = await Promise.all([
    getAWSOCRResponse(fileKey),
    getAzureOCRResponse(fileDownloadUrlPrefix + fileKey),
  ]);

  const text = await bb2Layout(awsResponse, azureResponse);
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
