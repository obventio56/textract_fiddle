import { createClient } from "@supabase/supabase-js";
import { chunkArray, getOCRForJob } from "./processJob.js";
import {
  fileDownloadUrlPrefix,
  fileUploadUrl,
  getQueryResponses,
} from "./index.js";
import { joinPages } from "./bb_2_layout.js";
import { v4 } from "uuid";
import axios from "axios";
import { PDFDocument } from "pdf-lib";

const multiInvoicesPerDocumentShape = {
  hasMultipleInvoices: {
    type: "boolean",
    description:
      "Return true if the provided text layout contains more than one invoice or bill document. Otherwise, return false. A key indicator of multiple invoices is the presence of multiple invoice numbers.",
  },
  invoiceDetails: {
    type: "array",
    description:
      "Details about each invoice or bill in the document. If there is only one invoice/bill in the document, return an array with one item.",
    items: {
      type: "object",
      properties: {
        invoiceNumber: {
          type: "string",
          description:
            "The unique identifier for this invoice or bill. This is often called the invoice number or bill number. If no invoice number is present, return an index or random uuid.",
        },
        invoiceStartPage: {
          type: "number",
          description:
            "The page number of the first page that contains information about this invoice or bill.",
        },
        invoiceEndPage: {
          type: "number",
          description:
            "The page number of the last page that contains information about this invoice or bill.",
        },
      },
    },
  },
};

// Takes a subset of the pages from a PDF and returns a new PDF
// End page is inclusive
const splitPdf = async (pdfBuffer, startPage, endPage) => {
  const pdfDoc = await PDFDocument.load(pdfBuffer);
  const pageCount = pdfDoc.getPageCount();

  if (startPage < 0 || endPage >= pageCount || startPage > endPage) {
    throw new Error("Invalid page range provided.");
  }

  const newPdfDoc = await PDFDocument.create();

  for (let i = startPage; i <= endPage; i++) {
    const [copiedPage] = await newPdfDoc.copyPages(pdfDoc, [i]);
    newPdfDoc.addPage(copiedPage);
  }

  return await newPdfDoc.save();
};

/**
 * Takes in a text layout.
 * Determines how many invoices are in the layout and their page ranges
 * Splits the file into multiple files
 * Splits layout into multiple layouts
 *
 * Returns an object:
 * {
 *      [serverId]: {
 *          textLayout
 *      },
 *     ...
 * }
 */
const splitDocumentsIntoInvoices = async (file, textPages) => {
  const fileId = file.serverId;

  // If it's not a PDF, it's not clear how we would split it
  if (!fileId.toLowerCase().includes("pdf")) {
    return { [fileId]: { ...file, textLayout: joinPages(textPages) } };
  }

  // If theres only one page we will assume there is only one invoice
  if (textPages.length < 2) {
    return { [fileId]: { ...file, textLayout: joinPages(textPages) } };
  }

  const splitResult = await getQueryResponses(
    joinPages(textPages),
    multiInvoicesPerDocumentShape
  );

  // If there was some error, return the original text layout
  if (!splitResult) {
    return { [fileId]: { ...file, textLayout: joinPages(textPages) } };
  }

  // If there's only one invoice, return the original text layout
  if (
    !splitResult.hasMultipleInvoices ||
    splitResult.invoiceDetails.length < 2
  ) {
    return { [fileId]: { ...file, textLayout: joinPages(textPages) } };
  }

  // Download the original document
  const originalDocumentRes = await axios.get(
    `${fileDownloadUrlPrefix}${fileId}`,
    {
      responseType: "arraybuffer",
    }
  );
  const originalDocument = new Uint8Array(originalDocumentRes.data);

  const splitFiles = await Promise.all(
    splitResult.invoiceDetails.map(async (invoice, idx) => {
      if (
        !Number.isInteger(invoice.invoiceStartPage) ||
        !Number.isInteger(invoice.invoiceEndPage)
      ) {
        throw new Error("Invalid page range provided.");
      }

      const startPage = invoice.invoiceStartPage - 1;
      const endPage = invoice.invoiceEndPage - 1;

      const invoiceLayout = joinPages(textPages.slice(startPage, endPage + 1));
      const invoicePdf = await splitPdf(originalDocument, startPage, endPage);

      const formData = new FormData();
      formData.append(
        `documents`,
        new Blob([invoicePdf], { type: "application/pdf" }),
        `${v4()}.pdf`
      );

      const response = await axios.post(fileUploadUrl, formData, {
        headers: {
          "Content-Type": "multipart/form-data",
        },
      });
      const { uploadedKeys } = response.data;

      return {
        [uploadedKeys[0]]: {
          serverId: uploadedKeys[0],
          fileType: "application/pdf",
          filename: `part_${idx}_of_${splitResult.invoiceDetails.length}_${file.filename}`,
          textLayout: invoiceLayout,
        },
      };
    })
  );

  return splitFiles.reduce((cum, file) => {
    return {
      ...cum,
      ...file,
    };
  }, {});
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
     * BEGIN SPLIT DOCUMENTS INTO INVOICES
     */

    // Get documents that successfully returned OCR
    const filesToSplit = postOCRJob.state.rawFiles.filter(
      (f) => postOCRJob.state.rawTextLayouts[f.serverId].length > 0
    );

    const chunkedFilesToSplit = chunkArray(filesToSplit, 10);

    for (const splitChunk of chunkedFilesToSplit) {
      // Wait for all docs in this chunk to return
      const splitFiles = await Promise.all(
        splitChunk.map((file) =>
          splitDocumentsIntoInvoices(
            file,
            postOCRJob.state.rawTextLayouts[file.serverId]
          )
        )
      );

      const processedFiles = splitFiles.reduce((cum, file) => {
        return {
          ...cum,
          ...file,
        };
      }, {});

      // Get current job state
      const currentJobRes = await supabase
        .from("jobs")
        .select()
        .eq("id", jobId);
      const currentJobState = currentJobRes.data[0].state;
      const currentProcessedFiles = currentJobState.processedFiles;

      // Compute new state by merging all OCR results with old state
      const newProcessedFiles = {
        ...currentProcessedFiles,
        ...processedFiles,
      };

      const newJobState = currentJobState;
      newJobState.processedFiles = newProcessedFiles;

      // Update state
      await supabase
        .from("jobs")
        .update({ state: newJobState })
        .eq("id", jobId);
    }

    // Set status to EDITING
    await supabase
      .from("jobs")
      .update({
        status: "EDITING",
      })
      .eq("id", jobId);
  } catch (e) {
    console.log("error", e);
  }
};
