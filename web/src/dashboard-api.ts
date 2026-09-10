export type Facade = { display_name: string; tagline: string; issue_command: string; branch_prefix: string };
export type EffectiveConfiguration = {
  deployment_mode: "generated" | "minimal";
  policy_source: string;
  facade: Facade;
  github: { auth_mode: string; ingress_mode: string; repositories: string[] };
  plugin: { id: string; flow: string } | null;
  capabilities: string[];
  warnings: string[];
};

const record = (value: unknown): value is Record<string, unknown> => typeof value === "object" && value !== null && !Array.isArray(value);
const text = (value: unknown): value is string => typeof value === "string" && value.trim().length > 0;
const strings = (value: unknown): value is string[] => Array.isArray(value) && value.every((item) => typeof item === "string");

function facade(value: unknown): Facade {
  if (!record(value) || ![value.display_name, value.tagline, value.issue_command, value.branch_prefix].every(text)) {
    throw new Error("The API returned an invalid dashboard facade.");
  }
  return value as Facade;
}

function configuration(value: unknown): EffectiveConfiguration {
  if (!record(value) || (value.deployment_mode !== "generated" && value.deployment_mode !== "minimal") || !text(value.policy_source)
    || !record(value.github) || !text(value.github.auth_mode) || !text(value.github.ingress_mode) || !strings(value.github.repositories)
    || !strings(value.capabilities) || !strings(value.warnings)
    || !(value.plugin === null || (record(value.plugin) && text(value.plugin.id) && text(value.plugin.flow)))) {
    throw new Error("The API returned invalid dashboard configuration.");
  }
  facade(value.facade);
  return value as EffectiveConfiguration;
}

// Bound both headers and body reads. Consuming the query signal prevents an
// abandoned request from overwriting a later retry with its stale response.
async function load<T>(url: string, validate: (value: unknown) => T, signal?: AbortSignal): Promise<T> {
  const controller = new AbortController();
  let timedOut = false;
  const abort = () => controller.abort();
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  const timer = window.setTimeout(() => { timedOut = true; controller.abort(); }, 10_000);
  try {
    const response = await fetch(url, { signal: controller.signal, cache: "no-store" });
    if (!response.ok) throw new Error(`API request failed (HTTP ${response.status}).`);
    const contentType = response.headers.get("content-type")?.split(";")[0].trim().toLowerCase();
    if (contentType !== "application/json" && !contentType?.endsWith("+json")) {
      throw new Error("The API returned a non-JSON response. Check the web proxy routing.");
    }
    let value: unknown;
    try { value = await response.json(); }
    catch (error) {
      if (controller.signal.aborted) throw error;
      throw new Error("The API returned invalid JSON.");
    }
    return validate(value);
  } catch (error) {
    if (timedOut) throw new Error("The API did not respond within 10 seconds.");
    if (signal?.aborted) throw error;
    if (error instanceof TypeError) throw new Error("The API could not be reached.");
    throw error;
  } finally {
    window.clearTimeout(timer);
    signal?.removeEventListener("abort", abort);
  }
}

export const loadFacade = ({ signal }: { signal?: AbortSignal }) => load("/api/facade", facade, signal);
export const loadConfiguration = ({ signal }: { signal?: AbortSignal }) => load("/api/configuration", configuration, signal);
