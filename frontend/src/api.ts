import type { ActorFeedStatus, ApplyResult, FillActorPlan, PlanEnvelope, PlanJob } from './types'

const API_TOKEN_KEY = 'embyx-web-api-token'
const ACTIVE_PLAN_KEY = 'embyx-web-active-plan-id'
const PLAN_ID = /^[A-Za-z0-9_-]{1,256}$/

export function hasApiToken(): boolean {
  return Boolean(window.sessionStorage.getItem(API_TOKEN_KEY))
}

export function setApiToken(value: string): void {
  const token = value.trim()
  if (token) window.sessionStorage.setItem(API_TOKEN_KEY, token)
  else window.sessionStorage.removeItem(API_TOKEN_KEY)
}

export function getActivePlanId(): string | null {
  try {
    const value = window.sessionStorage.getItem(ACTIVE_PLAN_KEY)
    if (!value) return null
    if (PLAN_ID.test(value)) return value
    window.sessionStorage.removeItem(ACTIVE_PLAN_KEY)
    return null
  } catch {
    return null
  }
}

export function setActivePlanId(value: string | null): void {
  try {
    if (value && PLAN_ID.test(value)) window.sessionStorage.setItem(ACTIVE_PLAN_KEY, value)
    else window.sessionStorage.removeItem(ACTIVE_PLAN_KEY)
  } catch {
    // Session recovery is best-effort; storage restrictions must not block a scan.
  }
}

export class ApiError extends Error {
  readonly status: number
  readonly code: string

  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

function requestHeaders(): HeadersInit {
  const apiToken = window.sessionStorage.getItem(API_TOKEN_KEY)
  return {
    Accept: 'application/json',
    'Content-Type': 'application/json',
    ...(apiToken ? { Authorization: `Bearer ${apiToken}` } : {}),
  }
}

async function request(path: string, init?: RequestInit): Promise<unknown> {
  let response: Response
  try {
    response = await fetch(path, { ...init, headers: { ...requestHeaders(), ...init?.headers } })
  } catch (error) {
    if (error instanceof DOMException && error.name === 'AbortError') throw error
    throw new ApiError(0, 'network_error', '无法连接到服务，请检查网络后重试。')
  }

  const body = (await response.json().catch(() => null)) as unknown
  if (!response.ok) {
    const record = isRecord(body) ? body : {}
    const detail = isRecord(record.error) ? record.error : isRecord(record.detail) ? record.detail : record
    const code = typeof detail.code === 'string' ? detail.code : `http_${response.status}`
    const message =
      typeof detail.message === 'string'
        ? detail.message
        : typeof detail.detail === 'string'
          ? detail.detail
          : '请求未能完成，请稍后重试。'
    throw new ApiError(response.status, code, message)
  }
  return body
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function looksLikePlan(value: unknown): value is FillActorPlan {
  return isRecord(value) && typeof value.plan_id === 'string' && Array.isArray(value.videos)
}

function looksLikeJob(value: unknown): value is PlanJob {
  if (!isRecord(value)) return false
  return (
    typeof value.state === 'string' ||
    typeof value.status === 'string' ||
    typeof value.job_id === 'string' ||
    typeof value.id === 'string'
  )
}

function normalizeActorFeedStatus(value: unknown): ActorFeedStatus | null {
  if (!isRecord(value)) return null
  if (!(
    typeof value.actor_id === 'string' &&
    ['queued', 'warming', 'ready', 'failed'].includes(String(value.state)) &&
    typeof value.attempts === 'number' &&
    Number.isFinite(value.attempts) &&
    typeof value.updated_at === 'string' &&
    (value.error_code === null || typeof value.error_code === 'string') &&
    (value.freshrss_add_url === null || typeof value.freshrss_add_url === 'string') &&
    (value.freshrss_url === undefined || value.freshrss_url === null || typeof value.freshrss_url === 'string')
  )) return null
  return {
    actor_id: value.actor_id,
    state: value.state as ActorFeedStatus['state'],
    attempts: value.attempts,
    updated_at: value.updated_at,
    error_code: value.error_code,
    freshrss_add_url: value.freshrss_add_url,
    freshrss_url: value.freshrss_url ?? null,
  }
}

export function normalizePlanEnvelope(value: unknown): PlanEnvelope {
  if (looksLikePlan(value)) return { plan: value, job: null, planId: value.plan_id, feeds: [] }
  if (!isRecord(value)) return { plan: null, job: null, planId: null, feeds: [] }

  const plan = looksLikePlan(value.plan) ? value.plan : null
  const job = looksLikeJob(value.job) ? value.job : looksLikeJob(value) ? value : null
  const feeds = Array.isArray(value.feeds)
    ? value.feeds.flatMap((feed) => {
        const normalized = normalizeActorFeedStatus(feed)
        return normalized ? [normalized] : []
      })
    : []
  const planId =
    plan?.plan_id ??
    (typeof value.plan_id === 'string' ? value.plan_id : null) ??
    (job && typeof job.plan_id === 'string' ? job.plan_id : null) ??
    (job && typeof job.id === 'string' ? job.id : null)
  return { plan, job, planId, feeds }
}

function normalizeApplyResult(value: unknown): ApplyResult {
  if (isRecord(value) && isRecord(value.result)) return value.result as unknown as ApplyResult
  return value as ApplyResult
}

export async function createPlan(actorIds: string[]): Promise<PlanEnvelope> {
  return normalizePlanEnvelope(
    await request('/api/fill-actor/plans', {
      method: 'POST',
      body: JSON.stringify({ actor_ids: actorIds }),
    }),
  )
}

export async function getPlan(planId: string, signal?: AbortSignal): Promise<PlanEnvelope> {
  return normalizePlanEnvelope(
    await request(`/api/fill-actor/plans/${encodeURIComponent(planId)}`, { cache: 'no-store', signal }),
  )
}

export async function applyCandidates(
  planId: string,
  revision: string,
  candidateIds: string[],
): Promise<ApplyResult> {
  return normalizeApplyResult(
    await request(`/api/fill-actor/plans/${encodeURIComponent(planId)}/apply`, {
      method: 'POST',
      body: JSON.stringify({ revision, candidate_ids: candidateIds }),
    }),
  )
}

export interface HealthStatus {
  status: string
  database?: boolean | string
  roots?: boolean | string | Record<string, string | boolean>
}

export async function getHealth(): Promise<HealthStatus> {
  return (await request('/api/health')) as HealthStatus
}
