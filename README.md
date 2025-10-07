Prompt Generator Clone

Quick start:

Server:
  cd server
  cp .env.example .env
  # Fill in OPENAI_API_KEY and optionally OPENAI_MODEL
  npm install
  npm run dev

Client:
  cd client
  npm install
  VITE_API_BASE=http://localhost:4000/api npm run dev

Features:
- Enhance prompts
- Run via OpenAI
- Save & History (SQLite)
