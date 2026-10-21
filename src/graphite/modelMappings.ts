export interface GraphiteModelMappingInput {
  id: string;
  request_model_id?: string | null;
  context_window?: number | null;
  reasoning_effort?: string | null;
}

export function modelMappingKey(id: string, requestModelId?: string | null): string {
  const actualId = id.trim();
  const requestId = requestModelId?.trim() || actualId;
  return JSON.stringify([actualId, requestId]);
}

export function normalizeGraphiteModels<T extends GraphiteModelMappingInput>(models: readonly T[]): T[] {
  const byMapping = new Map<string, T>();
  for (const source of models) {
    const id = source.id.trim();
    if (!id) continue;
    const requestModelId = source.request_model_id?.trim() || null;
    const normalized = {
      ...source,
      id,
      request_model_id: requestModelId === id ? null : requestModelId,
      context_window: source.context_window ?? null,
      reasoning_effort: source.reasoning_effort?.trim() || null
    } as T;
    byMapping.set(modelMappingKey(id, normalized.request_model_id), normalized);
  }
  return [...byMapping.values()];
}
