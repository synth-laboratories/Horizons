import { HorizonsClient } from "./client";
import { ProjectDbHandle, UUID } from "./types";

export class OnboardAPI {
  constructor(private client: HorizonsClient) {}

  async createProject(projectId?: UUID): Promise<ProjectDbHandle> {
    const body = projectId ? { project_id: projectId } : {};
    const resp = await this.client.request<{ handle: ProjectDbHandle }>("/api/v1/projects", {
      method: "POST",
      body: JSON.stringify(body),
      headers: { "content-type": "application/json" },
    });
    return resp.handle;
  }

  async listProjects(limit = 100, offset = 0): Promise<ProjectDbHandle[]> {
    const resp = await this.client.request<ProjectDbHandle[]>(
      `/api/v1/projects?limit=${limit}&offset=${offset}`,
      { method: "GET" },
    );
    return resp;
  }

  /**
   * Run a read-only SQL query against a project database.
   */
  async query(projectId: UUID, sql: string, params: unknown[] = []): Promise<Record<string, unknown>> {
    return await this.client.request<Record<string, unknown>>(
      `/api/v1/projects/${projectId}/query`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql, params }),
      },
    );
  }

  /**
   * Execute a write SQL statement against a project database.
   */
  async execute(projectId: UUID, sql: string, params: unknown[] = []): Promise<Record<string, unknown>> {
    return await this.client.request<Record<string, unknown>>(
      `/api/v1/projects/${projectId}/execute`,
      {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ sql, params }),
      },
    );
  }
}
