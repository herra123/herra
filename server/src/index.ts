import express from 'express';
import cors from 'cors';
import { config, assertConfig } from './config/env';
import promptsRouter from './routes/prompts';

assertConfig();

const app = express();
app.use(express.json({ limit: '1mb' }));
app.use(cors({ origin: config.clientOrigin, credentials: false }));

app.get('/api/health', (_req, res) => {
  res.json({ ok: true });
});

app.use('/api', promptsRouter);

app.listen(config.port, () => {
  console.log(`[server] listening on http://localhost:${config.port}`);
});
