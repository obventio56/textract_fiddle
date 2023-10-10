import "dotenv/config";
import { createClient } from "@supabase/supabase-js";
import { getExtraction, getOCRDocument, sleep } from "./index.js";

export function chunkArray(array, chunkSize) {
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

export const getOCRForJob = async (fileId, returnPages = false) => {
  try {
    const textLayout = await getOCRDocument(fileId, returnPages);
    return { [fileId]: textLayout };
  } catch (e) {
    console.log("error", e);
    return { [fileId]: { error: e.message } };
  }
};

/**
 * Intelligently coalesce extracted values
 */
export const coalesceExtractedValues = (shapeProperty, extractionResults) => {
  const allSame = extractionResults.every(
    (value) => value === extractionResults[0]
  );

  if (!allSame) {
    console.log(shapeProperty, "not all same", extractionResults);
  }

  // Return the first non-falsy value or return the first value if all are falsy
  for (const extractionResult of extractionResults) {
    const isFalsy = extractionResult === "null" || !extractionResult;
    if (!isFalsy) {
      return extractionResult;
    }
  }

  return extractionResults[0];
};

const attempts = 1;
const multiExtractEntry = async (shapeProperty, shapeValue, textLayout) => {
  const trialResults = [];
  const singlePropertyShape = { [shapeProperty]: shapeValue };

  for (let i = 0; i < attempts; i++) {
    const trialResult = await getExtraction(textLayout, singlePropertyShape);
    trialResults.push(trialResult[shapeProperty]);
  }

  const coalescedValue = coalesceExtractedValues(shapeValue, trialResults);

  return {
    [shapeProperty]: coalescedValue,
  };
};

const getExtractionForJob = async (fileId, shape, textLayout) => {
  if (!textLayout || !!textLayout.error) {
    return { [fileId]: { error: "No text layout for file" } };
  }

  try {
    const shapeEntries = Object.entries(shape);

    // Every property that is an array of objects should be its own chunk
    const listShapeChunks = shapeEntries
      .filter(
        ([_, value]) => value.type === "array" && value.items.type === "object"
      )
      .map((entry) => [entry]);

    const nonListShapeEntries = shapeEntries.filter(
      ([_, value]) => !(value.type === "array" && value.items.type === "object")
    );

    const nonListShapeEntryChunks = chunkArray(nonListShapeEntries, 10);
    const shapeEntryChunks = [...listShapeChunks, ...nonListShapeEntryChunks];

    let propertyObjects = [];
    for (const shapeEntryChunk of shapeEntryChunks) {
      const propertyObjectChunk = await Promise.all(
        shapeEntryChunk.map(([shapeProperty, shapeValue]) =>
          multiExtractEntry(shapeProperty, shapeValue, textLayout)
        )
      );

      propertyObjects = [...propertyObjects, ...propertyObjectChunk];
    }

    // Get extraction for each property, several times separately
    const args = propertyObjects.reduce((cum, pv) => {
      return {
        ...cum,
        ...pv,
      };
    }, {});

    sleep(5000);

    return { [fileId]: args };
  } catch (e) {
    console.log("error", e);
    return { [fileId]: { error: e.message } };
  }
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

    const fileIdsForExtraction = Object.keys(initialJob.state.processedFiles);
    const chunkedFileIdsForExtraction = chunkArray(fileIdsForExtraction, 3);

    for (const extractChunk of chunkedFileIdsForExtraction) {
      // Wait for all docs in this chunk to return
      const extractionResults = await Promise.all(
        extractChunk.map((fileId) =>
          getExtractionForJob(
            fileId,
            initialJob.state.shape,
            initialJob.state.processedFiles[fileId].textLayout
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
