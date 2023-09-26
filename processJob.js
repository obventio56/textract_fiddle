import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { getOCRDocument, getQueryResponses } from "./index.js";

function chunkArray(array, chunkSize) {
  // Initialize an empty array to hold the chunks
  let chunks = [];

  // Loop over the array and create the chunks
  for (let i = 0; i < array.length; i += chunkSize) {
    let chunk = array.slice(i, i + chunkSize);
    chunks.push(chunk);
  }

  return chunks;
}

/**
 * Take a jobId,
 * get state from supabase,
 * determine which files remain to process,
 * do the work in chunks,
 *
 * Not premature optimization but,
 *
 * Could send like 100+ jobs to aws at once, should save text layout
 *
 */

const getOCRForJob = async (fileId, jobId) => {
  const textLayout = await getOCRDocument(fileId);
  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_PUBLIC_ANON_KEY
  );

  // Get current state
  const res = await supabase.from("jobs").select().eq("id", jobId);
  const job = res.data[0];

  // Set text layout
  job.state.results.textLayouts[fileId] = textLayout;

  // Update state
  await supabase.from("jobs").update({ state: job.state }).eq("id", jobId);
};

const getExtractionForJob = async (fileId, jobId, shape, textLayout) => {
  if (!textLayout) {
    throw new Error("No text layout for file");
  }

  const args = await getQueryResponses(textLayout, shape);
  const supabase = createClient(
    process.env.SUPABASE_URL,
    process.env.SUPABASE_PUBLIC_ANON_KEY
  );

  // Get current state
  const res = await supabase.from("jobs").select().eq("id", jobId);
  const job = res.data[0];

  // Set extraction results
  job.state.results.extractionResults[fileId] = args;

  // Update state
  await supabase.from("jobs").update({ state: job.state }).eq("id", jobId);
};

export const processJob = async (jobId) => {
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

    // Set status to processing
    await supabase
      .from("jobs")
      .update({ status: "PROCESSING" })
      .eq("id", jobId);

    // Get documents that still need OCR
    const fileIdsForOCR = initialJob.state.fileIds.filter(
      (fid) => !initialJob.state.results.textLayouts[fid]
    );
    const chunkedFileIdsForOCR = chunkArray(fileIdsForOCR, 100);
    for (const ocrChunk of chunkedFileIdsForOCR) {
      await Promise.all(ocrChunk.map((fileId) => getOCRForJob(fileId, jobId)));
    }

    // Get state after all OCR
    const postOCRRes = await supabase.from("jobs").select().eq("id", jobId);
    const postOCRJob = postOCRRes.data[0];

    const fileIdsForExtraction = postOCRJob.state.fileIds.filter(
      (fid) => !postOCRJob.state.results.extractionResults[fid]
    );
    const chunkedFileIdsForExtraction = chunkArray(fileIdsForExtraction, 10);
    for (const extractChunk of chunkedFileIdsForExtraction) {
      await Promise.all(
        extractChunk.map((fileId) =>
          getExtractionForJob(
            fileId,
            jobId,
            postOCRJob.state.shape,
            postOCRJob.state.results.textLayouts[fileId]
          )
        )
      );
    }

    // Final state update to READY
    await supabase.from("jobs").update({ status: "READY" }).eq("id", jobId);
  } catch (e) {
    console.log("error", e);
  }
};
