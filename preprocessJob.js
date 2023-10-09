import { createClient } from "@supabase/supabase-js";
import { chunkArray, getOCRForJob } from "./processJob.js";
import { chatAPI } from "./openAI.js";
import { formatJSON } from "./index.js";

const firstPassClassificationShape = {
  documentType: {
    type: "string",
    description: `Which option best describes the type of bill or invoice document/documents represented in the text. 
      Here are the definitions of each type of document:
        bill - A bill document that does not contain all the details of an invoice. This type of document likely does not contain an invoice number but may contain some other type of unique id.
        invoice - A formal invoice document that contains an invoice id, an order number and other details.
        other - Some other type of document that is not a bill or invoice.`,
    enum: ["bill", "invoice", "other"],
  },
  moreThanOne: {
    type: "boolean",
    description: `If there is more than one bill/invoice document represented the text, select true. Otherwise, select false.`,
  },
};

const singelOrMultiPOInvoiceShape = {
  poNumbers: {
    type: "array",
    description: `All purchase order numbers (also could be referred to as PO numbers, order numbers, or the equivalent in a foreign language) present in this invoice. There is an edge case that some invoices might include PO numbers used internally by the supplier. These might be marked "ours." You should ignore these PO numbers, but include all others.`,
    items: {
      type: "string",
    },
  },
};

const classifyInvoiceDocuments = async (text, shape) => {
  let res;
  try {
    res = await chatAPI(
      [
        {
          role: "system",
          content: `You are polyglot and expert international accountant accountant specializing in accounts payable. You are going to help me do classification and preprocessing of receipt and invoice documents in order to determine how to call an API for further processing.
          You will be given a plain-text representation of an invoice, receipt, or other bill document that was created using OCR. We have done our best to maintain layout and formatting in the plain-text representation, but it may not be perfect.
          Additionally, not all documents will be in English. Your job is to use your expertise and language ability to extract and classify information from the document, no matter its language. 
          Your job is to use the provided document to call an external API via the callAPI function. You must classify the document and extract all the information necessary to call the function. `,
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
            properties: shape,
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
    return formatJSON(rawJSON, shape, null);
  } catch (e) {
    return { error: "Error parsing argument JSON", rawResponse: args };
  }
};

const getClassificationForJob = async (fileId, textLayout, shape) => {
  if (!textLayout || !!textLayout.error) {
    return { [fileId]: { error: "No text layout for file" } };
  }

  try {
    const args = await classifyInvoiceDocuments(textLayout, shape);
    return { [fileId]: args };
  } catch (e) {
    console.log("error", e);
    return { [fileId]: { error: e.message } };
  }
};

export const preprocessJob = async (jobId) => {
  try {
    const supabase = createClient(
      process.env.SUPABASE_URL,
      process.env.SUPABASE_PUBLIC_ANON_KEY
    );

    // Get job from supabase
    const res = await supabase.from("jobs").select().eq("id", jobId);
    const initialJob = res.data[0];
    if (!initialJob) {
      throw new Error("Job not found");
    }

    // Get documents that still need OCR
    const fileIdsForOCR = initialJob.state.rawFiles
      .map((rf) => rf.serverId)
      .filter((sid) => !initialJob.state.rawTextLayouts[sid]);

    const chunkedFileIdsForOCR = chunkArray(fileIdsForOCR, 15);

    for (const ocrChunk of chunkedFileIdsForOCR) {
      // Wait for all docs in this chunk to return
      const ocrResults = await Promise.all(
        ocrChunk.map((fileId) => getOCRForJob(fileId, true))
      );

      // Get current job state
      const currentJobRes = await supabase
        .from("jobs")
        .select()
        .eq("id", jobId);
      const currentJobState = currentJobRes.data[0].state;
      const currentRawTextLayouts = currentJobState.rawTextLayouts;

      // Compute new state by merging all OCR results with old state
      const newRawTextLayouts = ocrResults.reduce((cum, res) => {
        return {
          ...cum,
          ...res,
        };
      }, currentRawTextLayouts);

      const newJobState = currentJobState;
      newJobState.rawTextLayouts = newRawTextLayouts;

      // Update state
      await supabase
        .from("jobs")
        .update({ state: newJobState })
        .eq("id", jobId);
    }

    // Get state after all OCR
    const postOCRRes = await supabase.from("jobs").select().eq("id", jobId);
    const postOCRJob = postOCRRes.data[0];

    /**
     * Classify each raw file and extract some characteristics
     *
     * What type of document is it? Receipt, single PO invoice, multi PO invoice, etc.
     * Is there one of this item in the document or more than one
     * Page ranges:
     *
     * Please choose the classification that best describes the document.
     * Is there one of these or several in the document
     * What are the page ranges of each of these
     *
     */

    /**
     *
     * Here's the decision tree
     *
     * Is it a bill or an invoice?
     * If it's an invoice, is there one invoice or more than one?
     * For each invoice, is there one PO or more than one?
     *
     */

    // Get documents that still need OCR
    const fileIdsForClassification = postOCRJob.state.rawFiles
      .map((rf) => rf.serverId)
      .filter((sid) => !postOCRJob.state.rawTextClassifications[sid]);

    const chunkedFileIdsForClassification = chunkArray(
      fileIdsForClassification,
      10
    );

    for (const classifyChunk of chunkedFileIdsForClassification) {
      // Wait for all docs in this chunk to return
      const classificationResults = await Promise.all(
        classifyChunk.map((fileId) =>
          getClassificationForJob(
            fileId,
            postOCRJob.state.rawTextLayouts[fileId],
            firstPassClassificationShape
          )
        )
      );

      // For results that are invoices, check if there's more than one PO
      const poInvoiceResults = classificationResults.filter((res) => {
        const val = Object.values(res)[0];
        return !val.error && val.documentType === "invoice";
      });

      const poInvoiceClassificationResult = await Promise.all(
        poInvoiceResults.map((res) => {
          const fileId = Object.keys(res)[0];
          return getClassificationForJob(
            fileId,
            postOCRJob.state.rawTextLayouts[fileId],
            singelOrMultiPOInvoiceShape
          );
        })
      );

      const classificationResultObject = classificationResults.reduce(
        (cum, res) => {
          return { ...cum, ...res };
        },
        {}
      );

      const poInvoiceClassificationResultObject =
        poInvoiceClassificationResult.reduce((cum, res) => {
          return { ...cum, ...res };
        }, {});

      // Merge the two objects
      const combinedClassificationResults = Object.keys(
        classificationResultObject
      ).map((key) => {
        return {
          [key]: {
            ...classificationResultObject[key],
            ...(poInvoiceClassificationResultObject[key] || {}),
          },
        };
      });

      // Get current job state
      const currentJobRes = await supabase
        .from("jobs")
        .select()
        .eq("id", jobId);
      const currentJobState = currentJobRes.data[0].state;
      const currentClassificationResults =
        currentJobState.rawTextClassifications;

      // Compute new state by merging all OCR results with old state
      const newClassificationResults = combinedClassificationResults.reduce(
        (cum, res) => {
          return {
            ...cum,
            ...res,
          };
        },
        currentClassificationResults
      );

      const newJobState = currentJobState;
      newJobState.rawTextClassifications = newClassificationResults;

      // Update state
      await supabase
        .from("jobs")
        .update({ state: newJobState })
        .eq("id", jobId);
    }

    // Set status to processing
    // await supabase
    //   .from("jobs")
    //   .update({
    //     status: "EDITING",
    //     state: { ...postOCRJob.state, editingFiles: postOCRJob.state.rawFiles },
    //   })
    //   .eq("id", jobId);
  } catch (e) {
    console.log("error", e);
  }
};
