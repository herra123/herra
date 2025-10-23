import OpenAI from 'openai';
import { config } from '../config/env';

let client: OpenAI | null = null;

export function getOpenAI(): OpenAI | null {
  if (!config.openaiApiKey) return null;
  if (!client) {
    client = new OpenAI({ apiKey: config.openaiApiKey });
  }
  return client;
}

export async function runCompletion(prompt: string, model?: string): Promise<string> {
  const api = getOpenAI();
  if (!api) {
    throw new Error('OpenAI API key not configured');
  }
  const modelToUse = model || config.openaiModel;

  // Use Chat Completions for broad compatibility
  const response = await api.chat.completions.create({
    model: modelToUse,
    messages: [
      { role: 'system', content: 'You are a helpful, expert assistant.' },
      { role: 'user', content: prompt }
    ],
    temperature: 0.7
  });

  const content = response.choices?.[0]?.message?.content || '';
  return content.trim();
}
