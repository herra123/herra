export type HistoryItem = {
  id: number;
  title: string;
  prompt: string;
  enhancedPrompt?: string | null;
  tags?: string[];
  createdAt: string;
  updatedAt: string;
};
