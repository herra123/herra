import { runCompletion } from './openai';

export type EnhanceInput = {
  input: string;
  domain?: string; // writing | coding | marketing | business | design | etc.
  tone?: string;   // professional, friendly, persuasive, etc.
  format?: string; // list, sections, numbered, bullets, etc.
};

export async function enhancePrompt(params: EnhanceInput): Promise<string> {
  const { input, domain, tone, format } = params;

  const systemPrimer = `You are an AI Prompt Generator inspired by PromptPerfect. Rewrite the user's request into a highly effective, structured prompt including: role, context, objectives, constraints, output format, tone, and quality guardrails. Make it concise but comprehensive.`;

  const userInstruction = [
    `User request: ${input}`,
    domain ? `Domain hint: ${domain}` : null,
    tone ? `Preferred tone: ${tone}` : null,
    format ? `Preferred output format: ${format}` : null,
    `Output only the enhanced prompt, nothing else.`
  ].filter(Boolean).join('\n');

  try {
    const result = await runCompletion(`${systemPrimer}\n\n${userInstruction}`);
    return sanitizeEnhancedPrompt(result);
  } catch {
    return heuristicEnhance(params);
  }
}

function sanitizeEnhancedPrompt(text: string): string {
  return text.trim().replace(/^"|"$/g, '');
}

function heuristicEnhance({ input, domain, tone, format }: EnhanceInput): string {
  const role = domain ? `You are an expert ${domain} assistant.` : 'You are an expert assistant.';
  const toneLine = tone ? `Use a ${tone} tone.` : 'Use a professional, engaging tone.';
  const formatLine = format ? `Format the output as ${format}.` : 'Format the output with clear sections and bullet points where helpful.';

  return [
    role,
    `Task: ${input}`,
    'Include: key assumptions, constraints, and rationale as needed.',
    toneLine,
    formatLine
  ].join('\n');
}
