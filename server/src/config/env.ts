import 'dotenv/config';

export const config = {
  port: Number(process.env.PORT || 4000),
  openaiApiKey: process.env.OPENAI_API_KEY || '',
  openaiModel: process.env.OPENAI_MODEL || 'gpt-4o-mini',
  databasePath: process.env.DATABASE_PATH || './data/prompts.db',
  clientOrigin: process.env.CLIENT_ORIGIN || 'http://localhost:5173'
};

export function assertConfig() {
  if (!config.openaiApiKey) {
    // We allow startup without the key to support offline enhance heuristics,
    // but warn loudly so users know to set it for full functionality.
    console.warn('[warn] OPENAI_API_KEY is not set. API calls will be disabled.');
  }
}
