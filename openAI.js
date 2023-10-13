import axios from "axios";
import { encodingForModel } from "js-tiktoken";
import dotenv from "dotenv";
dotenv.config();

const OPEN_AI_API_URL = "https://api.openai.com";
const OPEN_AI_API_KEY = process.env.OPEN_AI_API_KEY;

// Check renderer
const modelTokenLimits = {
  "gpt-4": 4096,
  "gpt-4": 8192,
  "text-embedding-ada-002": 8191,
};

const guardTokenLimit = (model, prompt) => {
  const enc = encodingForModel(model);
  const promptTokenLength = enc.encode(prompt).length;

  console.log("tokens used", promptTokenLength, "of", modelTokenLimits[model]);

  if (promptTokenLength > modelTokenLimits[model]) {
    throw new Error(
      `Prompt length (${promptTokenLength}) exceeds maximum for model ${model} (${modelTokenLimits[model]}).`
    );
  }
};

const chatAPI = async (
  messages,
  functions,
  model = "gpt-4",
  allowedAttempts = 1
) => {
  // Ensure prompt length is within model limits
  guardTokenLimit(model, JSON.stringify({ messages, functions }));

  const headers = {
    "Content-Type": "application/json",
    Authorization: `Bearer ${OPEN_AI_API_KEY}`,
  };

  const payload = {
    model,
    functions,
    messages,
  };

  let attempts = 0;
  while (true) {
    try {
      const response = await axios.post(
        `${OPEN_AI_API_URL}/v1/chat/completions`,
        payload,
        { headers }
      );
      const assistantMessage = response.data;

      // If we succeed, just return
      return assistantMessage || "No response from API.";
    } catch (error) {
      attempts++;

      // If we've exceeded our attempts, throw the error
      if (attempts === allowedAttempts) {
        throw error;
      }

      // Otherwise, keep trying
      console.log(error, "Retrying...");
    }
  }
};

export { chatAPI };
