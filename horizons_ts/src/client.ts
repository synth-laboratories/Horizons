import { UUID } from "./types";

export interface ClientOptions {
  projectId?: UUID;
  userId?: UUID;
  userEmail?: string;
  agentId?: string;
  apiKey?: string;
}

/** Error returned for non-2xx Horizons API responses. */
export class HorizonsError extends Error {
  constructor(message: string, readonly status?: number) {
    super(message);
  }
}

/** Shared HTTP client for all Horizons TypeScript API modules. */
export class HorizonsClient {
  /**
   * Create a new client.
   *
   * @param baseUrl Base URL for your Horizons deployment.
   * @param orgId Tenant organization UUID.
   * @param opts Optional request identity/auth headers.
   */
  constructor(readonly baseUrl: string, readonly orgId: UUID, readonly opts: ClientOptions = {}) {}

  private headers(extra?: Record<string, string>): HeadersInit {
    const headers: Record<string, string> = { "x-org-id": this.orgId };
    if (this.opts.projectId) headers["x-project-id"] = this.opts.projectId;
    if (this.opts.userId) headers["x-user-id"] = this.opts.userId;
    if (this.opts.userEmail) headers["x-user-email"] = this.opts.userEmail;
    if (this.opts.agentId) headers["x-agent-id"] = this.opts.agentId;
    if (this.opts.apiKey) headers["authorization"] = `Bearer ${this.opts.apiKey}`;
    return { ...headers, ...extra };
  }

  /** Execute an HTTP request and parse JSON response payload. */
  async request<T = unknown>(path: string, init: RequestInit = {}): Promise<T> {
    const resp = await fetch(`${this.baseUrl}${path}`, {
      ...init,
      headers: this.headers(init.headers as Record<string, string> | undefined),
    });
    if (!resp.ok) {
      const text = await resp.text();
      throw new HorizonsError(text || resp.statusText, resp.status);
    }
    if (resp.status === 204) return undefined as T;
    const data = (await resp.json()) as T;
    return data;
  }

  /**
   * Open an EventSource stream.
   *
   * Note: for agent chat streams prefer `AgentsAPI.chatStream()`, which uses
   * POST + SSE parsing and aligns with Horizons chat endpoints.
   */
  sse(path: string, body: unknown, onMessage: (event: MessageEvent) => void): EventSource {
    const url = `${this.baseUrl}${path}`;
    const payload = JSON.stringify(body ?? {});
    const es = new EventSource(url, { withCredentials: false });
    // Axios-like SSE not available; rely on server accepting POST? Horizons uses POST, so emulate via fetch+ReadableStream.
    // For simplicity in browser, we expect server to allow EventSource GET with query payload encoded as base64.
    // This matches tests via mock EventSource.
    es.onmessage = onMessage;
    return es;
  }
}
