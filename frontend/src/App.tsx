import { useEffect, useMemo, useRef, useState } from 'react'
import {
  ApiError,
  applyCandidates,
  cancelPlan,
  createPlan,
  getActivePlanId,
  getHealth,
  getPlan,
  hasApiToken,
  setActivePlanId,
  setApiToken as storeApiToken,
  type HealthStatus,
} from './api'
import type {
  ActorFeedStatus,
  ApplyResult,
  FillActorPlan,
  JobProgress,
  JobState,
  MoveCandidate,
  PlanEnvelope,
  PlanJob,
  VideoPlan,
  VideoState,
} from './types'

const ACTOR_ID = /^[A-Za-z0-9_-]{1,32}$/
const MAX_ACTORS = 20
const STALE_CODES = new Set(['expired_plan', 'revision_mismatch', 'unknown_plan', 'legacy_plan_requires_rescan'])
const BUSINESS_PROGRESS_WARNING_SECONDS = 60
const HEARTBEAT_WARNING_SECONDS = 35

const FEED_STATE_LABELS = {
  queued: '等待缓存',
  warming: '缓存预热中',
  ready: '缓存已就绪',
  failed: '缓存失败',
} as const

const STAGE_LABELS: Record<string, string> = {
  queued: '任务已排队',
  actor_catalog: '正在获取演员作品',
  actor_fetch: '正在获取演员作品',
  fetching_actors: '正在获取演员作品',
  actors: '正在获取演员作品',
  library_scan: '正在扫描本地片库',
  video_scan: '正在扫描本地片库',
  scanning_videos: '正在扫描本地片库',
  videos: '正在扫描本地片库',
  magnet_lookup: '正在查询磁力资源',
  magnet_search: '正在查询磁力资源',
  magnets: '正在查询磁力资源',
  persisting: '正在保存扫描结果',
  finalizing: '正在整理扫描结果',
  saving_plan: '正在保存扫描结果',
  done: '扫描已完成',
  unknown: '正在处理扫描任务',
}

const UNIT_LABELS: Record<string, string> = {
  actor: '位演员',
  actors: '位演员',
  page: '页',
  pages: '页',
  video: '个作品',
  videos: '个作品',
  magnet: '个磁力查询',
  magnets: '个磁力查询',
  item: '项',
  items: '项',
  step: '步',
  steps: '步',
}

const VIDEO_GROUPS: Array<{
  state: VideoState
  label: string
  description: string
  tone: string
}> = [
  { state: 'additional_found', label: '可移入', description: '在附加片库中找到文件', tone: 'amber' },
  { state: 'magnet_found', label: '可下载', description: '已找到磁力链接', tone: 'violet' },
  { state: 'missing', label: '未找到', description: '本地与磁力源均无结果', tone: 'muted' },
  { state: 'exists', label: '已入库', description: '演员片库已有文件', tone: 'green' },
  { state: 'invalid_video_id', label: '无法识别', description: '番号或厂牌无法解析', tone: 'red' },
  { state: 'scan_failed', label: '扫描失败', description: '扫描时发生局部错误', tone: 'red' },
]

const MOVE_LABELS = {
  moved: '已移入',
  stale: '源文件已变化',
  conflict: '目标冲突',
  invalid_path: '路径无效',
  failed: '移动失败',
} as const

const MOVE_ERROR_LABELS: Record<string, string> = {
  cloud_move_status_unknown: '远端状态待确认，请勿重复操作',
  cloud_move_in_progress: '已有移动正在核验',
  cloud_destination_missing: '无法准备目标目录',
  cloud_destination_exists: '目标位置已有文件',
  cloud_source_changed: '远端源文件已变化',
  strm_target_changed: '映射目标已变化，请重新扫描',
}

function parseActorIds(value: string) {
  const values = value
    .split(/[\s,，;；]+/)
    .map((item) => item.trim())
    .filter(Boolean)
  const actorIds = [...new Set(values)]
  const invalid = actorIds.filter((item) => !ACTOR_ID.test(item))
  return { actorIds, invalid, duplicateCount: values.length - actorIds.length }
}

function jobState(job: PlanJob | null): JobState | null {
  return job?.state ?? job?.status ?? null
}

function isJobPending(job: PlanJob | null) {
  const state = jobState(job)
  return state === 'queued' || state === 'running'
}

function isJobCancelled(job: PlanJob | null) {
  return jobState(job) === 'failed' && job?.error_code === 'job_cancelled'
}

function candidateMap(plan: FillActorPlan | null) {
  const map = new Map<string, MoveCandidate>()
  plan?.videos.forEach((video) => video.move_candidates.forEach((candidate) => map.set(candidate.candidate_id, candidate)))
  return map
}

function safeMagnet(magnet: string | null): string | null {
  if (!magnet || !magnet.toLowerCase().startsWith('magnet:?')) return null
  return [...magnet].every((character) => {
    const code = character.charCodeAt(0)
    return code > 31 && code !== 127
  }) ? magnet : null
}

function safeFreshRssUrl(value: string | null): string | null {
  if (!value) return null
  try {
    const url = new URL(value)
    return url.protocol === 'https:' || url.protocol === 'http:' ? value : null
  } catch {
    return null
  }
}

function planMagnets(plan: FillActorPlan | null): string[] {
  const seen = new Set<string>()
  const magnets: string[] = []
  plan?.videos.forEach((video) => {
    const magnet = safeMagnet(video.magnet)
    if (magnet && !seen.has(magnet)) {
      seen.add(magnet)
      magnets.push(magnet)
    }
  })
  return magnets
}

function errorMessage(error: unknown): string {
  if (error instanceof ApiError) {
    const messages: Record<string, string> = {
      move_disabled: '文件移动当前由管理员暂停。',
      legacy_plan_requires_rescan: '该计划来自旧版本，请重新扫描后再操作。',
      not_ready: '移动依赖尚未就绪，请稍后重试。',
    }
    return messages[error.code] ?? error.message
  }
  return '操作未能完成，请稍后重试。'
}

function progressValue(progress?: JobProgress | null): number | null {
  if (!progress) return null
  if (typeof progress.percent === 'number') return Math.max(0, Math.min(100, progress.percent))
  if (typeof progress.completed === 'number' && typeof progress.total === 'number' && progress.total > 0) {
    return Math.round((progress.completed / progress.total) * 100)
  }
  return null
}

function safeSeconds(value: number | null | undefined): number | null {
  return typeof value === 'number' && Number.isFinite(value) && value >= 0 ? value : null
}

function secondsSince(value: string | null | undefined, now: number): number | null {
  if (!value) return null
  const timestamp = Date.parse(value)
  return Number.isFinite(timestamp) ? Math.max(0, Math.floor((now - timestamp) / 1000)) : null
}

function durationText(rawSeconds: number): string {
  const seconds = Math.max(0, Math.floor(rawSeconds))
  if (seconds < 60) return `${seconds} 秒`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes} 分 ${seconds % 60} 秒`
  const hours = Math.floor(minutes / 60)
  return `${hours} 小时 ${minutes % 60} 分`
}

function stageElapsed(progress: JobProgress | null | undefined, now: number): number | null {
  if (!progress) return null
  const fromStart = secondsSince(progress.stage_started_at, now)
  if (fromStart !== null) return fromStart
  const elapsed = safeSeconds(progress.elapsed_seconds)
  if (elapsed === null) return null
  return elapsed + (secondsSince(progress.updated_at, now) ?? 0)
}

function remainingEta(progress: JobProgress | null | undefined): number | null {
  return safeSeconds(progress?.eta_seconds)
}

function lastProgressAge(progress: JobProgress | null | undefined, now: number): number | null {
  if (!progress) return null
  return secondsSince(progress.updated_at, now) ?? safeSeconds(progress.last_progress_seconds)
}

function progressCount(progress: JobProgress | null | undefined): string | null {
  const completed = safeSeconds(progress?.completed)
  if (completed === null) return null
  const count = Math.floor(completed)
  const total = safeSeconds(progress?.total)
  const unit = progress?.unit ? (UNIT_LABELS[progress.unit] ?? progress.unit) : '项'
  return total === null ? `已完成 ${count} ${unit}` : `${count} / ${Math.floor(total)} ${unit}`
}

export default function App() {
  const [recoveredPlanId] = useState(getActivePlanId)
  const [input, setInput] = useState('')
  const [apiTokenInput, setApiTokenInput] = useState('')
  const [authRequired, setAuthRequired] = useState(false)
  const [authConfigured, setAuthConfigured] = useState(hasApiToken)
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [healthFailed, setHealthFailed] = useState(false)
  const [plan, setPlan] = useState<FillActorPlan | null>(null)
  const [feeds, setFeeds] = useState<ActorFeedStatus[]>([])
  const [planId, setPlanId] = useState<string | null>(recoveredPlanId)
  const [job, setJob] = useState<PlanJob | null>(() => recoveredPlanId
    ? { plan_id: recoveredPlanId, state: 'running' }
    : null)
  const [submitting, setSubmitting] = useState(false)
  const [cancelling, setCancelling] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [pollWarning, setPollWarning] = useState<string | null>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [applying, setApplying] = useState(false)
  const [applyResult, setApplyResult] = useState<ApplyResult | null>(null)
  const [needsFreshPlan, setNeedsFreshPlan] = useState(false)
  const [copyingMagnets, setCopyingMagnets] = useState(false)
  const [copiedRevision, setCopiedRevision] = useState<string | null>(null)
  const [magnetCopyError, setMagnetCopyError] = useState<string | null>(null)
  const [now, setNow] = useState(Date.now())
  const lastAutoSelectedRevision = useRef<string | null>(null)
  const pollFailures = useRef(0)
  const requestGeneration = useRef(0)
  const activePollController = useRef<AbortController | null>(null)
  const activeCancelController = useRef<AbortController | null>(null)
  const parsed = useMemo(() => parseActorIds(input), [input])
  const candidates = useMemo(() => candidateMap(plan), [plan])
  const magnets = useMemo(() => planMagnets(plan), [plan])
  const selectedCandidates = [...selected].map((id) => candidates.get(id)).filter(Boolean) as MoveCandidate[]
  const planExpired = Boolean(plan && new Date(plan.expires_at).getTime() <= now)
  const applyVerificationPending = Boolean(
    applyResult?.results.some((item) => item.error_code === 'cloud_move_status_unknown'),
  )
  const jobPending = isJobPending(job)
  const jobCancelled = isJobCancelled(job)
  const feedsPending = feeds.some((feed) => feed.state === 'queued' || feed.state === 'warming')
  const envelopePending = jobPending || feedsPending
  const healthReady = Boolean(health && ['ok', 'healthy', 'ready'].includes(health.status.toLowerCase()))
  const applyEnabled = health?.apply_ready === true
  const applyNotice = !health || applyEnabled
    ? null
    : health.apply_enabled === false
      ? {
          title: '文件移动已暂停',
          body: '当前仅支持扫描、磁力查询和订阅操作；确认移入功能已由管理员关闭。',
        }
      : health.legacy_journal === false
        ? {
            title: '文件移动等待管理员处理',
            body: '检测到旧版本未完成的移动记录。为避免误动派生映射文件，新的移入已被阻止。',
          }
        : {
            title: '文件移动尚未就绪',
            body: health.cloud === false
              ? 'CloudDrive 连接或授权尚未就绪，当前不会提交移动。'
              : '文件移动依赖尚未就绪，请稍后重试。',
          }

  useEffect(() => {
    let mounted = true
    const refresh = () => {
      void getHealth()
        .then((value) => {
          if (!mounted) return
          setHealth(value)
          setHealthFailed(false)
        })
        .catch(() => {
          if (!mounted) return
          setHealth(null)
          setHealthFailed(true)
        })
    }
    refresh()
    const timer = window.setInterval(refresh, 30_000)
    return () => {
      mounted = false
      window.clearInterval(timer)
    }
  }, [])

  useEffect(() => {
    setNow(Date.now())
    const timer = window.setInterval(() => setNow(Date.now()), jobPending ? 1_000 : 30_000)
    return () => window.clearInterval(timer)
  }, [jobPending])

  useEffect(() => {
    if (planId && envelopePending) setActivePlanId(planId)
    else if (job || plan) setActivePlanId(null)
  }, [envelopePending, job, plan, planId])

  useEffect(() => {
    if (!plan || lastAutoSelectedRevision.current === plan.revision) return
    lastAutoSelectedRevision.current = plan.revision
    const safeIds = plan.videos.flatMap((video) =>
      video.move_candidates.filter((candidate) => !candidate.destination_conflict).map((candidate) => candidate.candidate_id),
    )
    setSelected(new Set(safeIds))
  }, [plan])

  useEffect(() => {
    if (!planId || (!isJobPending(job) && !feedsPending) || cancelling) return
    const generation = requestGeneration.current
    const controller = new AbortController()
    activePollController.current = controller
    const delay = Math.min(800 * 2 ** pollFailures.current, 10_000)
    const timer = window.setTimeout(() => {
      void getPlan(planId, controller.signal)
        .then((envelope) => {
          if (generation !== requestGeneration.current) return
          pollFailures.current = 0
          setPollWarning(null)
          consumeEnvelope(envelope, setPlan, setPlanId, setJob, setFeeds, setError)
        })
        .catch((pollError: unknown) => {
          if (generation !== requestGeneration.current) return
          if (pollError instanceof DOMException && pollError.name === 'AbortError') return
          if (pollError instanceof ApiError && pollError.code === 'unauthorized') {
            setAuthRequired(true)
            setError(errorMessage(pollError))
          } else if (pollError instanceof ApiError && STALE_CODES.has(pollError.code)) {
            setNeedsFreshPlan(true)
            setJob((current) => current ? { ...current, state: 'failed', error_code: pollError.code } : current)
          } else {
            pollFailures.current += 1
            setPollWarning('暂时无法刷新任务状态，将自动重试。')
            setJob((current) => current ? { ...current } : current)
          }
        })
    }, delay)
    return () => {
      window.clearTimeout(timer)
      controller.abort()
      if (activePollController.current === controller) activePollController.current = null
    }
  }, [cancelling, feedsPending, job, planId])

  useEffect(() => () => {
    requestGeneration.current += 1
    activePollController.current?.abort()
    activeCancelController.current?.abort()
  }, [])

  async function startScan() {
    if (!parsed.actorIds.length || parsed.invalid.length || parsed.actorIds.length > MAX_ACTORS) return
    setSubmitting(true)
    setCancelling(false)
    setError(null)
    setPollWarning(null)
    setActivePlanId(null)
    setPlan(null)
    setFeeds([])
    setPlanId(null)
    setJob(null)
    setApplyResult(null)
    setSelected(new Set())
    setNeedsFreshPlan(false)
    setCopiedRevision(null)
    setMagnetCopyError(null)
    lastAutoSelectedRevision.current = null
    pollFailures.current = 0
    requestGeneration.current += 1
    activePollController.current?.abort()
    activeCancelController.current?.abort()
    try {
      consumeEnvelope(await createPlan(parsed.actorIds), setPlan, setPlanId, setJob, setFeeds, setError)
    } catch (scanError) {
      if (scanError instanceof ApiError && scanError.code === 'unauthorized') setAuthRequired(true)
      setError(errorMessage(scanError))
    } finally {
      setSubmitting(false)
    }
  }

  async function cancelScan() {
    const targetPlanId = planId
    if (!targetPlanId || !isJobPending(job) || cancelling) return
    const generation = requestGeneration.current + 1
    requestGeneration.current = generation
    activePollController.current?.abort()
    activeCancelController.current?.abort()
    const controller = new AbortController()
    activeCancelController.current = controller
    setCancelling(true)
    setError(null)
    setPollWarning(null)

    try {
      let envelope: PlanEnvelope
      try {
        envelope = await cancelPlan(targetPlanId, controller.signal)
      } catch (cancelError) {
        if (
          cancelError instanceof ApiError &&
          cancelError.status === 409 &&
          cancelError.code === 'plan_not_cancellable'
        ) {
          envelope = await getPlan(targetPlanId, controller.signal)
        } else {
          throw cancelError
        }
      }
      if (generation !== requestGeneration.current) return
      pollFailures.current = 0
      consumeEnvelope(envelope, setPlan, setPlanId, setJob, setFeeds, setError)
    } catch (cancelError) {
      if (generation !== requestGeneration.current) return
      if (cancelError instanceof DOMException && cancelError.name === 'AbortError') return
      if (cancelError instanceof ApiError && cancelError.code === 'unauthorized') setAuthRequired(true)
      if (cancelError instanceof ApiError && STALE_CODES.has(cancelError.code)) {
        setNeedsFreshPlan(true)
        setJob((current) => current ? { ...current, state: 'failed', error_code: cancelError.code } : current)
      } else {
        setJob((current) => current ? { ...current } : current)
      }
      setError(errorMessage(cancelError))
    } finally {
      if (activeCancelController.current === controller) activeCancelController.current = null
      if (generation === requestGeneration.current) setCancelling(false)
    }
  }

  function toggleCandidate(candidate: MoveCandidate) {
    if (candidate.destination_conflict) return
    setSelected((previous) => {
      const next = new Set(previous)
      if (next.has(candidate.candidate_id)) next.delete(candidate.candidate_id)
      else next.add(candidate.candidate_id)
      return next
    })
  }

  async function confirmApply() {
    if (!applyEnabled || !plan || !selected.size) return
    setConfirmOpen(false)
    setApplying(true)
    setError(null)
    try {
      const result = await applyCandidates(plan.plan_id, plan.revision, [...selected])
      setApplyResult(result)
      if (result.results.some((item) => item.state === 'stale')) setNeedsFreshPlan(true)
    } catch (applyError) {
      if (applyError instanceof ApiError && applyError.code === 'unauthorized') setAuthRequired(true)
      if (applyError instanceof ApiError && STALE_CODES.has(applyError.code)) setNeedsFreshPlan(true)
      if (applyError instanceof ApiError && applyError.code === 'move_disabled') {
        setHealth((current) => current ? { ...current, apply_enabled: false, apply_ready: false } : current)
      }
      setError(errorMessage(applyError))
    } finally {
      setApplying(false)
    }
  }

  async function copyAllMagnets() {
    if (!magnets.length || copyingMagnets) return
    setCopyingMagnets(true)
    setMagnetCopyError(null)
    try {
      await navigator.clipboard.writeText(magnets.join('\n'))
      const revision = plan?.revision ?? null
      setCopiedRevision(revision)
      window.setTimeout(() => setCopiedRevision((current) => (current === revision ? null : current)), 1600)
    } catch {
      setMagnetCopyError('浏览器不允许访问剪贴板，请手动复制磁力链接。')
    } finally {
      setCopyingMagnets(false)
    }
  }

  function saveApiToken() {
    storeApiToken(apiTokenInput)
    setAuthConfigured(Boolean(apiTokenInput.trim()))
    setAuthRequired(false)
    setApiTokenInput('')
    setError(null)
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <a className="brand" href="#top" aria-label="Embyx 首页">
          <span className="brand-mark" aria-hidden="true">E</span>
          <span>embyx</span>
        </a>
        <div className="topbar-meta">
          <span className={`health-dot ${healthReady ? 'online' : healthFailed || health ? 'offline' : ''}`} />
          {healthFailed ? '服务不可达' : health ? healthReady ? '服务正常' : '服务未就绪' : '正在连接'}
        </div>
      </header>

      <main id="top">
        <section className="hero">
          <div className="eyebrow"><span /> MEDIA OPERATIONS</div>
          <h1>补全演员片库</h1>
          <p>批量检查演员作品，汇总本地文件和下载线索，在确认后安全移入待整理目录。</p>
        </section>

        <section className="scan-panel" aria-labelledby="scan-title">
          <div className="section-heading compact">
            <div>
              <span className="step-number">01</span>
              <h2 id="scan-title">输入演员 ID</h2>
            </div>
            <span className="field-count">{parsed.actorIds.length} / {MAX_ACTORS}</span>
          </div>
          <div className={`input-frame ${parsed.invalid.length ? 'invalid' : ''}`}>
            <textarea
              aria-label="演员 ID"
              value={input}
              onChange={(event) => setInput(event.target.value)}
              placeholder={'例如：A12345, B67890\n支持空格、逗号或换行分隔'}
              rows={4}
              disabled={submitting || jobPending}
            />
            <div className="input-footer">
              <span>仅支持字母、数字、下划线和连字符</span>
              {parsed.duplicateCount > 0 && <span>{parsed.duplicateCount} 个重复项已自动合并</span>}
            </div>
          </div>
          {parsed.invalid.length > 0 && (
            <p className="validation" role="alert">无法识别：{parsed.invalid.join('、')}</p>
          )}
          {parsed.actorIds.length > MAX_ACTORS && (
            <p className="validation" role="alert">每次最多扫描 {MAX_ACTORS} 位演员。</p>
          )}
          {authRequired && (
            <div className="auth-prompt" role="group" aria-label="API 认证">
              <div>
                <strong>需要 API Token</strong>
                <span>Token 仅保存在当前浏览器会话中，不会写入构建产物。</span>
              </div>
              <input
                aria-label="API Token"
                type="password"
                autoComplete="off"
                value={apiTokenInput}
                onChange={(event) => setApiTokenInput(event.target.value)}
              />
              <button className="button secondary" type="button" disabled={!apiTokenInput.trim()} onClick={saveApiToken}>
                保存 Token
              </button>
            </div>
          )}
          {authConfigured && !authRequired && <p className="auth-configured">当前会话已配置 API Token。</p>}
          <button
            className="button primary scan-button"
            type="button"
            disabled={!parsed.actorIds.length || Boolean(parsed.invalid.length) || parsed.actorIds.length > MAX_ACTORS || submitting || jobPending}
            onClick={() => void startScan()}
          >
            {submitting || jobPending ? <Spinner /> : <ScanIcon />}
            {submitting ? '正在提交' : jobPending ? '正在扫描' : '开始扫描'}
          </button>
        </section>

        {(submitting || jobPending) && (
          <ProgressPanel
            job={job}
            planId={planId}
            now={now}
            pollWarning={pollWarning}
            submitting={submitting}
            cancelling={cancelling}
            onCancel={() => void cancelScan()}
          />
        )}

        {error && <Notice tone="error" title="操作未完成" body={error} />}
        {jobCancelled && (
          <Notice tone="neutral" title="扫描已取消" body="任务已停止，未生成可应用的扫描结果。" />
        )}
        {(needsFreshPlan || planExpired) && (
          <Notice
            tone="warning"
            title="扫描结果已失效"
            body="文件状态或计划版本已经变化。请重新扫描后再选择文件，避免使用过期结果。"
            action={<button className="text-button" type="button" onClick={() => void startScan()}>重新扫描 <ArrowIcon /></button>}
          />
        )}

        {feeds.length > 0 && !jobCancelled && <ActorFeeds feeds={feeds} />}

        {plan && (
          <>
            <PlanSummary plan={plan} />
            <section className="results-section" aria-labelledby="results-title">
              <div className="section-heading">
                <div>
                  <span className="step-number">02</span>
                  <h2 id="results-title">扫描结果</h2>
                </div>
                <div className="result-heading-actions">
                  <span className="result-total">共 {plan.videos.length} 部作品</span>
                  {magnets.length > 0 && (
                    <button
                      className="button secondary magnet-copy-button"
                      type="button"
                      disabled={copyingMagnets}
                      onClick={() => void copyAllMagnets()}
                    >
                      {copyingMagnets ? <Spinner /> : copiedRevision === plan.revision ? <CheckIcon /> : <CopyIcon />}
                      {copyingMagnets
                        ? '正在复制'
                        : copiedRevision === plan.revision
                          ? `已复制 ${magnets.length} 个磁力`
                          : `复制全部磁力（${magnets.length}）`}
                    </button>
                  )}
                </div>
              </div>
              {magnetCopyError && <p className="magnet-copy-error" role="alert">{magnetCopyError}</p>}
              <div className="group-stack">
                {VIDEO_GROUPS.map((group) => {
                  const videos = plan.videos.filter((video) => video.state === group.state)
                  if (!videos.length) return null
                  return (
                    <VideoGroup
                      key={group.state}
                      group={group}
                      videos={videos}
                      selected={selected}
                      toggleCandidate={toggleCandidate}
                      applyResult={applyResult}
                    />
                  )
                })}
              </div>
            </section>

            {applyResult && <ApplySummary result={applyResult} />}

            {applyNotice && (
              <Notice
                tone="warning"
                title={applyNotice.title}
                body={applyNotice.body}
              />
            )}

            <div className="action-dock">
              <div>
                <span>已选择</span>
                <strong>{selected.size}</strong>
                <span>个文件</span>
              </div>
              <button
                className="button primary"
                type="button"
                disabled={
                  !applyEnabled
                  || !selected.size
                  || applying
                  || needsFreshPlan
                  || planExpired
                  || applyVerificationPending
                }
                onClick={() => applyEnabled && setConfirmOpen(true)}
              >
                {applying ? <Spinner /> : <MoveIcon />}
                {applying ? '正在移入' : applyResult ? '再次应用选择' : '确认并移入'}
              </button>
            </div>
          </>
        )}
      </main>

      {confirmOpen && applyEnabled && (
        <div className="dialog-backdrop" role="presentation" onMouseDown={() => setConfirmOpen(false)}>
          <div className="dialog" role="dialog" aria-modal="true" aria-labelledby="confirm-title" onMouseDown={(event) => event.stopPropagation()}>
            <span className="dialog-icon"><MoveIcon /></span>
            <h2 id="confirm-title">确认移入 {selected.size} 个文件？</h2>
            <p>文件将从附加片库移入待整理目录。操作会逐项执行，部分失败不会回滚已完成的文件。</p>
            <div className="confirm-list">
              {selectedCandidates.slice(0, 5).map((candidate) => <span key={candidate.candidate_id}>{candidate.file_name}</span>)}
              {selectedCandidates.length > 5 && <span>另有 {selectedCandidates.length - 5} 个文件</span>}
            </div>
            <div className="dialog-actions">
              <button className="button secondary" type="button" onClick={() => setConfirmOpen(false)}>取消</button>
              <button className="button primary" type="button" onClick={() => void confirmApply()}>确认移入</button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}

function ProgressPanel({
  job,
  planId,
  now,
  pollWarning,
  submitting,
  cancelling,
  onCancel,
}: {
  job: PlanJob | null
  planId: string | null
  now: number
  pollWarning: string | null
  submitting: boolean
  cancelling: boolean
  onCancel: () => void
}) {
  const progress = job?.progress
  const value = progressValue(progress)
  const state = jobState(job)
  const stage = progress?.stage ?? null
  const title = submitting
    ? '正在提交任务'
    : state === 'queued'
      ? '任务已排队'
      : stage
        ? (STAGE_LABELS[stage] ?? '正在处理扫描任务')
        : '正在恢复扫描状态'
  const count = progressCount(progress)
  const elapsed = stageElapsed(progress, now)
  const eta = remainingEta(progress)
  const progressAge = lastProgressAge(progress, now)
  const heartbeatAge = secondsSince(job?.updated_at, now)
  const progressWarning = state === 'running'
    && progressAge !== null
    && progressAge >= BUSINESS_PROGRESS_WARNING_SECONDS
  const heartbeatWarning = state === 'running'
    && heartbeatAge !== null
    && heartbeatAge >= HEARTBEAT_WARNING_SECONDS
  const valueText = count ?? (value === null ? '进度计算中' : `${Math.round(value)}%`)
  const canCancel = Boolean(planId && (state === 'queued' || state === 'running'))

  return (
    <section className="progress-panel" aria-busy="true" aria-labelledby="scan-progress-title">
      <div className="progress-orbit"><Spinner /></div>
      <div className="progress-body">
        <div className="progress-copy" role="status" aria-live="polite" aria-atomic="true">
          <strong id="scan-progress-title">{title}</strong>
          <span>{progress?.current ? `当前：${progress.current}` : '正在等待最新进度…'}</span>
        </div>

        <div className="progress-meter">
          <div
            className="progress-track"
            role="progressbar"
            aria-label={`${title}进度`}
            aria-valuemin={0}
            aria-valuemax={100}
            aria-valuenow={value === null ? undefined : Math.round(value)}
            aria-valuetext={valueText}
          >
            <span
              className={value === null ? 'indeterminate' : ''}
              style={value === null ? undefined : { width: `${value}%` }}
            />
          </div>
          <b>{value === null ? '计算中' : `${Math.round(value)}%`}</b>
        </div>

        <dl className="progress-meta">
          <div><dt>阶段进度</dt><dd>{count ?? '等待统计'}</dd></div>
          <div><dt>阶段已用时</dt><dd>{elapsed === null ? '等待统计' : durationText(elapsed)}</dd></div>
          <div><dt>当前阶段 ETA</dt><dd>{eta === null ? '计算中' : `约 ${durationText(eta)}`}</dd></div>
          <div><dt>最后进展</dt><dd>{progressAge === null ? '等待首个结果' : `${durationText(progressAge)}前`}</dd></div>
        </dl>

        {(pollWarning || progressWarning || heartbeatWarning) && (
          <div className="progress-warnings" role="status" aria-live="polite">
            {pollWarning && <p>{pollWarning}</p>}
            {progressWarning && <p>较长时间无新结果，仍可能在等待外部服务。</p>}
            {heartbeatWarning && <p>执行器心跳已较长时间未更新，执行可能已经中断。</p>}
          </div>
        )}
        {canCancel && (
          <div className="progress-actions">
            <button
              className="button secondary cancel-scan-button"
              type="button"
              disabled={cancelling}
              onClick={onCancel}
            >
              {cancelling && <Spinner />}
              {cancelling ? '正在取消' : '取消扫描'}
            </button>
          </div>
        )}
      </div>
    </section>
  )
}

function consumeEnvelope(
  envelope: PlanEnvelope,
  setPlan: (plan: FillActorPlan | null) => void,
  setPlanId: (id: string | null) => void,
  setJob: (job: PlanJob | null) => void,
  setFeeds: (feeds: ActorFeedStatus[]) => void,
  setError: (error: string | null) => void,
) {
  setPlanId(envelope.planId)
  setJob(envelope.job)
  setFeeds(envelope.feeds)
  if (envelope.plan) setPlan(envelope.plan)
  const state = jobState(envelope.job)
  if (isJobCancelled(envelope.job)) {
    setError(null)
    return
  }
  if ((state === 'failed' || state === 'partial_failed') && !envelope.plan) {
    setError(envelope.job?.error_code ? `扫描任务失败：${envelope.job.error_code}` : '扫描任务未能完成。')
  }
}

function PlanSummary({ plan }: { plan: FillActorPlan }) {
  const actorFailures = plan.actors.filter((actor) => actor.error_code).length
  const counts = Object.fromEntries(VIDEO_GROUPS.map(({ state }) => [state, plan.videos.filter((video) => video.state === state).length]))
  return (
    <section className="summary-strip" aria-label="扫描摘要">
      <div><span>演员</span><strong>{plan.actors.length}</strong>{actorFailures > 0 && <small>{actorFailures} 个失败</small>}</div>
      <div><span>作品</span><strong>{plan.videos.length}</strong></div>
      <div><span>已入库</span><strong>{counts.exists}</strong></div>
      <div><span>可处理</span><strong>{counts.additional_found + counts.magnet_found}</strong></div>
      <div><span>计划有效至</span><strong className="expiry">{new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' }).format(new Date(plan.expires_at))}</strong></div>
    </section>
  )
}

function ActorFeeds({ feeds }: { feeds: ActorFeedStatus[] }) {
  return (
    <section className="feed-panel" aria-labelledby="feed-title">
      <div className="feed-panel-heading">
        <div>
          <span className="feed-panel-icon"><FeedIcon /></span>
          <div>
            <h2 id="feed-title">RSSHub 缓存</h2>
            <p>演员订阅源准备状态</p>
          </div>
        </div>
        <span>{feeds.length} 位演员</span>
      </div>
      <ul className="feed-list">
        {feeds.map((feed) => {
          const freshrssAddUrl = feed.state === 'ready' ? safeFreshRssUrl(feed.freshrss_add_url) : null
          const freshrssUrl = feed.state === 'ready' ? safeFreshRssUrl(feed.freshrss_url) : null
          return (
            <li className={`feed-row feed-${feed.state}`} key={feed.actor_id}>
              <div className="feed-actor">
                <strong>{feed.actor_id}</strong>
                <span>已尝试 {feed.attempts} 次 · {formatFeedUpdatedAt(feed.updated_at)}</span>
              </div>
              <span className="feed-state" role="status" aria-live="polite">
                <FeedStateIcon state={feed.state} />{FEED_STATE_LABELS[feed.state]}
              </span>
              {feed.state === 'warming' && <p className="feed-detail">RSSHub 正在预热缓存，页面会自动更新。</p>}
              {feed.state === 'failed' && (
                <p className="feed-detail">{feed.error_code ? `错误：${feed.error_code}` : '缓存预热未能完成。'}</p>
              )}
              {(freshrssAddUrl || freshrssUrl) && (
                <div className="feed-actions">
                  {freshrssAddUrl && (
                    <a className="button secondary freshrss-button" href={freshrssAddUrl} target="_blank" rel="noopener noreferrer">
                      <ExternalIcon />一键添加到 FreshRSS
                    </a>
                  )}
                  {freshrssUrl && (
                    <a className="button secondary freshrss-button" href={freshrssUrl} target="_blank" rel="noopener noreferrer">
                      <ExternalIcon />打开 FreshRSS
                    </a>
                  )}
                </div>
              )}
            </li>
          )
        })}
      </ul>
    </section>
  )
}

function formatFeedUpdatedAt(value: string): string {
  const timestamp = Date.parse(value)
  if (!Number.isFinite(timestamp)) return '更新时间未知'
  return `更新于 ${new Intl.DateTimeFormat('zh-CN', { hour: '2-digit', minute: '2-digit' }).format(new Date(timestamp))}`
}

function VideoGroup({
  group,
  videos,
  selected,
  toggleCandidate,
  applyResult,
}: {
  group: (typeof VIDEO_GROUPS)[number]
  videos: VideoPlan[]
  selected: Set<string>
  toggleCandidate: (candidate: MoveCandidate) => void
  applyResult: ApplyResult | null
}) {
  const [expanded, setExpanded] = useState(true)
  return (
    <section className={`video-group tone-${group.tone}`}>
      <button className="group-header" type="button" onClick={() => setExpanded((value) => !value)} aria-expanded={expanded}>
        <span className="group-status"><StatusIcon state={group.state} /></span>
        <span className="group-title"><strong>{group.label}</strong><small>{group.description}</small></span>
        <span className="group-count">{videos.length}</span>
        <ChevronIcon expanded={expanded} />
      </button>
      {expanded && (
        <div className="video-list">
          {videos.map((video) => (
            <VideoRow
              key={video.video_id}
              video={video}
              selected={selected}
              toggleCandidate={toggleCandidate}
              applyResult={applyResult}
            />
          ))}
        </div>
      )}
    </section>
  )
}

function VideoRow({
  video,
  selected,
  toggleCandidate,
  applyResult,
}: {
  video: VideoPlan
  selected: Set<string>
  toggleCandidate: (candidate: MoveCandidate) => void
  applyResult: ApplyResult | null
}) {
  const magnet = safeMagnet(video.magnet)
  return (
    <article className="video-row">
      <div className="video-identity">
        <strong>{video.video_id}</strong>
        <span>{video.actor_ids.join(' · ')}</span>
      </div>
      <div className="video-detail">
        {video.existing_files.map((file) => <span className="file-chip" key={file}><FileIcon />{file}</span>)}
        {video.move_candidates.map((candidate) => {
          const result = applyResult?.results.find((item) => item.candidate_id === candidate.candidate_id)
          return (
            <label className={`candidate ${candidate.destination_conflict ? 'conflict' : ''}`} key={candidate.candidate_id}>
              <input
                type="checkbox"
                checked={selected.has(candidate.candidate_id)}
                disabled={candidate.destination_conflict}
                onChange={() => toggleCandidate(candidate)}
              />
              <span className="custom-check"><CheckIcon /></span>
              <span className="candidate-copy">
                <strong>{candidate.file_name}</strong>
                <small>{candidate.source_label}</small>
              </span>
              {candidate.destination_conflict && <span className="warning-pill"><AlertIcon />目标位置已有同名文件</span>}
              {result && (
                <span className={`result-pill result-${result.state}`}>
                  {(result.error_code && MOVE_ERROR_LABELS[result.error_code]) ?? MOVE_LABELS[result.state]}
                </span>
              )}
            </label>
          )
        })}
        {magnet && (
          <div className="magnet-row">
            <span className="magnet-text" title={magnet}>{magnet}</span>
          </div>
        )}
        {!video.move_candidates.length && !magnet && !video.existing_files.length && (
          <span className="empty-detail">{video.warnings.length ? video.warnings.join(' · ') : '暂无可用结果'}</span>
        )}
      </div>
    </article>
  )
}

function ApplySummary({ result }: { result: ApplyResult }) {
  const moved = result.results.filter((item) => item.state === 'moved').length
  const failed = result.results.length - moved
  const unknown = result.results.some((item) => item.error_code === 'cloud_move_status_unknown')
  return (
    <section className={`apply-summary ${failed ? 'has-errors' : ''}`} aria-live="polite">
      <span className="apply-icon">{failed ? <AlertIcon /> : <CheckIcon />}</span>
      <div>
        <strong>{unknown ? '部分远端状态仍在核验' : failed ? '文件处理完成，部分项目需要注意' : '所选文件已全部移入'}</strong>
        <p>{moved} 个成功{unknown ? `，${failed} 个状态未知；系统只会观察，不会自动重复移动。` : failed ? `，${failed} 个未移动。失败项目已在列表中标记。` : '。可继续选择其他文件或重新扫描。'}</p>
      </div>
    </section>
  )
}

function Notice({ tone, title, body, action }: { tone: 'error' | 'warning' | 'neutral'; title: string; body: string; action?: React.ReactNode }) {
  return (
    <div className={`notice notice-${tone}`} role={tone === 'neutral' ? 'status' : 'alert'}>
      <span>{tone === 'neutral' ? <CancelIcon /> : <AlertIcon />}</span>
      <div><strong>{title}</strong><p>{body}</p></div>
      {action}
    </div>
  )
}

function Spinner() { return <span className="spinner" aria-hidden="true" /> }
function ScanIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="6"/><path d="m16 16 4 4M11 8v6M8 11h6"/></svg> }
function MoveIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M4 12h15m-5-5 5 5-5 5"/><path d="M4 5v14"/></svg> }
function ArrowIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m9 18 6-6-6-6"/></svg> }
function ChevronIcon({ expanded }: { expanded: boolean }) { return <svg className={`chevron ${expanded ? 'expanded' : ''}`} viewBox="0 0 24 24" aria-hidden="true"><path d="m9 6 6 6-6 6"/></svg> }
function CheckIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="m5 12 4 4L19 6"/></svg> }
function AlertIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3 2.7 20h18.6L12 3Z"/><path d="M12 9v5m0 3h.01"/></svg> }
function CancelIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="m9 9 6 6m0-6-6 6"/></svg> }
function CopyIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><rect x="8" y="8" width="11" height="11" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2"/></svg> }
function ExternalIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 4h6v6m0-6-9 9"/><path d="M19 13v6a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1h6"/></svg> }
function FeedIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M5 5a14 14 0 0 1 14 14M5 11a8 8 0 0 1 8 8"/><circle cx="5" cy="19" r="1"/></svg> }
function FeedStateIcon({ state }: { state: ActorFeedStatus['state'] }) {
  if (state === 'ready') return <CheckIcon />
  if (state === 'failed') return <AlertIcon />
  if (state === 'warming') return <Spinner />
  return <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="12" cy="12" r="8"/><path d="M12 8v5l3 2"/></svg>
}
function FileIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 3h8l4 4v14H6z"/><path d="M14 3v5h4"/></svg> }
function StatusIcon({ state }: { state: VideoState }) {
  if (state === 'exists') return <CheckIcon />
  if (state === 'additional_found') return <MoveIcon />
  if (state === 'magnet_found') return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3v8a5 5 0 0 0 10 0V3M7 7h4m2 0h4"/></svg>
  if (state === 'missing') return <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m16 16 4 4"/></svg>
  return <AlertIcon />
}
