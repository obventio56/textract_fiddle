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

const getOCRForJob = async (fileId) => {
  const textLayout = await getOCRDocument(fileId);
  return { [fileId]: textLayout };
};

const getExtractionForJob = async (fileId, shape, textLayout) => {
  if (!textLayout) {
    throw new Error("No text layout for file");
  }

  const args = await getQueryResponses(textLayout, shape);
  return { [fileId]: args };
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
      // Wait for all docs in this chunk to return
      const ocrResults = await Promise.all(
        ocrChunk.map((fileId) => getOCRForJob(fileId))
      );

      // Get current job state
      const currentJobRes = await supabase
        .from("jobs")
        .select()
        .eq("id", jobId);
      const currentJobState = currentJobRes.data[0].state;
      const currentTextLayouts = currentJobState.results.textLayouts;

      // Compute new state by merging all OCR results with old state
      const newTextLayouts = ocrResults.reduce((cum, res) => {
        return {
          ...cum,
          ...res,
        };
      }, currentTextLayouts);

      const newJobState = currentJobState;
      newJobState.results.textLayouts = newTextLayouts;

      // Update state
      await supabase
        .from("jobs")
        .update({ state: newJobState })
        .eq("id", jobId);
    }

    // Get state after all OCR
    const postOCRRes = await supabase.from("jobs").select().eq("id", jobId);
    const postOCRJob = postOCRRes.data[0];

    const fileIdsForExtraction = postOCRJob.state.fileIds.filter(
      (fid) => !postOCRJob.state.results.extractionResults[fid]
    );
    const chunkedFileIdsForExtraction = chunkArray(fileIdsForExtraction, 10);

    for (const extractChunk of chunkedFileIdsForExtraction) {
      // Wait for all docs in this chunk to return
      const extractionResults = await Promise.all(
        extractChunk.map((fileId) =>
          getExtractionForJob(
            fileId,
            postOCRJob.state.shape,
            postOCRJob.state.results.textLayouts[fileId]
          )
        )
      );

      // Get current job state
      const currentJobRes = await supabase
        .from("jobs")
        .select()
        .eq("id", jobId);
      const currentJobState = currentJobRes.data[0].state;
      const currentExtractionResults =
        currentJobState.results.extractionResults;

      // Compute new state by merging all OCR results with old state
      const newExtractionResults = extractionResults.reduce((cum, res) => {
        return {
          ...cum,
          ...res,
        };
      }, currentExtractionResults);

      const newJobState = currentJobState;
      newJobState.results.extractionResults = newExtractionResults;

      // Update state
      await supabase
        .from("jobs")
        .update({ state: newJobState })
        .eq("id", jobId);
    }

    // Final state update to READY
    await supabase.from("jobs").update({ status: "READY" }).eq("id", jobId);
  } catch (e) {
    console.log("error", e);
  }
};
