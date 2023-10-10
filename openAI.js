import axios from "axios";
import { encodingForModel } from "js-tiktoken";
import dotenv from "dotenv";
dotenv.config();

const OPEN_AI_API_URL = "https://api.openai.com";
const OPEN_AI_API_KEY = process.env.OPEN_AI_API_KEY;

// Check renderer
const modelTokenLimits = {
  "gpt-3.5-turbo": 4096,
  "gpt-4": 8192,
  "text-embedding-ada-002": 8191,
  "gpt-3.5-turbo-16k": 16384,
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

  return promptTokenLength;
};

const tokensPerMinute = 40000;
const requestsPerMinute = 200;

export class OpenAIApi {
  constructor() {
    // Keep info on requests from the last minute
    // {tokensUsed: 0, timestamp: 0}
    this.history = [];
  }

  // Filter out requests that are older than 1 minute
  cleanHistory() {
    const now = Date.now();
    this.history = this.history.filter((h) => h.timestamp > now - 60000);
  }

  historyTokensUsed() {
    return this.history.reduce((cum, h) => cum + h.tokensUsed, 0);
  }

  historyRequests() {
    return this.history.length;
  }

  async chatAPI(messages, functions, model = "gpt-4", allowedAttempts = 1) {
    // Ensure prompt length is within model limits
    const requestTokens = guardTokenLimit(
      model,
      JSON.stringify({ messages, functions })
    );

    while (true) {
      // Clean history
      this.cleanHistory();

      // Check if we've exceeded our token limit
      if (
        this.historyTokensUsed() + requestTokens > tokensPerMinute ||
        this.historyRequests() >= requestsPerMinute
      ) {
        // Wait a second and try again
        // console.log(
        //   "Waiting... current token usage is",
        //   this.historyTokensUsed(),
        //   "request usages is",
        //   this.historyRequests()
        // );
        await new Promise((r) => setTimeout(r, 1000));
      } else {
        break;
      }
    }

    // Add request to history
    this.history.push({
      tokensUsed: requestTokens,
      timestamp: Date.now(),
    });

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

        console.log(attempts, allowedAttempts);
        // If we've exceeded our attempts, throw the error
        if (attempts >= allowedAttempts) {
          throw error;
        }

        // Otherwise, keep trying
        console.log(error, "Retrying...");
      }
    }
  }
}
