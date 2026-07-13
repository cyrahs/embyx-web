import { useEffect, useMemo, useRef, useState } from 'react'
import {
  ApiError,
  applyCandidates,
  createPlan,
  getHealth,
  getPlan,
  hasApiToken,
  setApiToken as storeApiToken,
  type HealthStatus,
} from './api'
import type {
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
const STALE_CODES = new Set(['expired_plan', 'revision_mismatch', 'unknown_plan'])

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

function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message
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

export default function App() {
  const [input, setInput] = useState('')
  const [apiTokenInput, setApiTokenInput] = useState('')
  const [authRequired, setAuthRequired] = useState(false)
  const [authConfigured, setAuthConfigured] = useState(hasApiToken)
  const [health, setHealth] = useState<HealthStatus | null>(null)
  const [healthFailed, setHealthFailed] = useState(false)
  const [plan, setPlan] = useState<FillActorPlan | null>(null)
  const [planId, setPlanId] = useState<string | null>(null)
  const [job, setJob] = useState<PlanJob | null>(null)
  const [submitting, setSubmitting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [selected, setSelected] = useState<Set<string>>(new Set())
  const [confirmOpen, setConfirmOpen] = useState(false)
  const [applying, setApplying] = useState(false)
  const [applyResult, setApplyResult] = useState<ApplyResult | null>(null)
  const [needsFreshPlan, setNeedsFreshPlan] = useState(false)
  const [copiedMagnet, setCopiedMagnet] = useState<string | null>(null)
  const [now, setNow] = useState(Date.now())
  const lastAutoSelectedRevision = useRef<string | null>(null)
  const pollFailures = useRef(0)
  const parsed = useMemo(() => parseActorIds(input), [input])
  const candidates = useMemo(() => candidateMap(plan), [plan])
  const selectedCandidates = [...selected].map((id) => candidates.get(id)).filter(Boolean) as MoveCandidate[]
  const planExpired = Boolean(plan && new Date(plan.expires_at).getTime() <= now)
  const progress = progressValue(job?.progress)
  const healthReady = Boolean(health && ['ok', 'healthy', 'ready'].includes(health.status.toLowerCase()))

  useEffect(() => {
    let mounted = true
    void getHealth()
      .then((value) => {
        if (mounted) setHealth(value)
      })
      .catch(() => {
        if (mounted) setHealthFailed(true)
      })
    return () => {
      mounted = false
    }
  }, [])

  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 30_000)
    return () => window.clearInterval(timer)
  }, [])

  useEffect(() => {
    if (!plan || lastAutoSelectedRevision.current === plan.revision) return
    lastAutoSelectedRevision.current = plan.revision
    const safeIds = plan.videos.flatMap((video) =>
      video.move_candidates.filter((candidate) => !candidate.destination_conflict).map((candidate) => candidate.candidate_id),
    )
    setSelected(new Set(safeIds))
  }, [plan])

  useEffect(() => {
    if (!planId || plan || !isJobPending(job) || authRequired) return
    const controller = new AbortController()
    const delay = Math.min(800 * 2 ** pollFailures.current, 10_000)
    const timer = window.setTimeout(() => {
      void getPlan(planId, controller.signal)
        .then((envelope) => {
          pollFailures.current = 0
          consumeEnvelope(envelope, setPlan, setPlanId, setJob, setError)
        })
        .catch((pollError: unknown) => {
          if (pollError instanceof DOMException && pollError.name === 'AbortError') return
          setError(errorMessage(pollError))
          if (pollError instanceof ApiError && pollError.code === 'unauthorized') {
            setAuthRequired(true)
          } else if (pollError instanceof ApiError && STALE_CODES.has(pollError.code)) {
            setNeedsFreshPlan(true)
            setJob((current) => current ? { ...current, state: 'failed', error_code: pollError.code } : current)
          } else {
            pollFailures.current += 1
            setJob((current) => current ? { ...current } : current)
          }
        })
    }, delay)
    return () => {
      window.clearTimeout(timer)
      controller.abort()
    }
  }, [authRequired, job, plan, planId])

  async function startScan() {
    if (!parsed.actorIds.length || parsed.invalid.length || parsed.actorIds.length > MAX_ACTORS) return
    setSubmitting(true)
    setError(null)
    setPlan(null)
    setPlanId(null)
    setJob(null)
    setApplyResult(null)
    setSelected(new Set())
    setNeedsFreshPlan(false)
    lastAutoSelectedRevision.current = null
    pollFailures.current = 0
    try {
      consumeEnvelope(await createPlan(parsed.actorIds), setPlan, setPlanId, setJob, setError)
    } catch (scanError) {
      if (scanError instanceof ApiError && scanError.code === 'unauthorized') setAuthRequired(true)
      setError(errorMessage(scanError))
    } finally {
      setSubmitting(false)
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
    if (!plan || !selected.size) return
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
      setError(errorMessage(applyError))
    } finally {
      setApplying(false)
    }
  }

  async function copyMagnet(magnet: string) {
    const safe = safeMagnet(magnet)
    if (!safe) return
    try {
      await navigator.clipboard.writeText(safe)
      setCopiedMagnet(safe)
      window.setTimeout(() => setCopiedMagnet((current) => (current === safe ? null : current)), 1600)
    } catch {
      setError('浏览器不允许访问剪贴板，请手动复制磁力链接。')
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
              disabled={submitting || isJobPending(job)}
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
            disabled={!parsed.actorIds.length || Boolean(parsed.invalid.length) || parsed.actorIds.length > MAX_ACTORS || submitting || isJobPending(job)}
            onClick={() => void startScan()}
          >
            {submitting || isJobPending(job) ? <Spinner /> : <ScanIcon />}
            {submitting ? '正在提交' : isJobPending(job) ? '正在扫描' : '开始扫描'}
          </button>
        </section>

        {(submitting || isJobPending(job)) && (
          <section className="progress-panel" aria-live="polite">
            <div className="progress-orbit"><Spinner /></div>
            <div className="progress-copy">
              <strong>{jobState(job) === 'queued' ? '任务已排队' : '正在检查作品与文件'}</strong>
              <span>{job?.progress?.current ?? '这可能需要一点时间，请保持页面开启。'}</span>
            </div>
            <div className="progress-track" aria-label="扫描进度">
              <span className={progress === null ? 'indeterminate' : ''} style={progress === null ? undefined : { width: `${progress}%` }} />
            </div>
            {progress !== null && <b>{progress}%</b>}
          </section>
        )}

        {error && <Notice tone="error" title="操作未完成" body={error} />}
        {(needsFreshPlan || planExpired) && (
          <Notice
            tone="warning"
            title="扫描结果已失效"
            body="文件状态或计划版本已经变化。请重新扫描后再选择文件，避免使用过期结果。"
            action={<button className="text-button" type="button" onClick={() => void startScan()}>重新扫描 <ArrowIcon /></button>}
          />
        )}

        {plan && (
          <>
            <PlanSummary plan={plan} />
            <section className="results-section" aria-labelledby="results-title">
              <div className="section-heading">
                <div>
                  <span className="step-number">02</span>
                  <h2 id="results-title">扫描结果</h2>
                </div>
                <span className="result-total">共 {plan.videos.length} 部作品</span>
              </div>
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
                      copiedMagnet={copiedMagnet}
                      copyMagnet={copyMagnet}
                      applyResult={applyResult}
                    />
                  )
                })}
              </div>
            </section>

            {applyResult && <ApplySummary result={applyResult} />}

            <div className="action-dock">
              <div>
                <span>已选择</span>
                <strong>{selected.size}</strong>
                <span>个文件</span>
              </div>
              <button
                className="button primary"
                type="button"
                disabled={!selected.size || applying || needsFreshPlan || planExpired}
                onClick={() => setConfirmOpen(true)}
              >
                {applying ? <Spinner /> : <MoveIcon />}
                {applying ? '正在移入' : applyResult ? '再次应用选择' : '确认并移入'}
              </button>
            </div>
          </>
        )}
      </main>

      {confirmOpen && (
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

function consumeEnvelope(
  envelope: PlanEnvelope,
  setPlan: (plan: FillActorPlan | null) => void,
  setPlanId: (id: string | null) => void,
  setJob: (job: PlanJob | null) => void,
  setError: (error: string | null) => void,
) {
  setPlanId(envelope.planId)
  setJob(envelope.job)
  if (envelope.plan) setPlan(envelope.plan)
  const state = jobState(envelope.job)
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

function VideoGroup({
  group,
  videos,
  selected,
  toggleCandidate,
  copiedMagnet,
  copyMagnet,
  applyResult,
}: {
  group: (typeof VIDEO_GROUPS)[number]
  videos: VideoPlan[]
  selected: Set<string>
  toggleCandidate: (candidate: MoveCandidate) => void
  copiedMagnet: string | null
  copyMagnet: (magnet: string) => Promise<void>
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
              copiedMagnet={copiedMagnet}
              copyMagnet={copyMagnet}
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
  copiedMagnet,
  copyMagnet,
  applyResult,
}: {
  video: VideoPlan
  selected: Set<string>
  toggleCandidate: (candidate: MoveCandidate) => void
  copiedMagnet: string | null
  copyMagnet: (magnet: string) => Promise<void>
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
              {result && <span className={`result-pill result-${result.state}`}>{MOVE_LABELS[result.state]}</span>}
            </label>
          )
        })}
        {magnet && (
          <div className="magnet-row">
            <span className="magnet-text" title={magnet}>{magnet}</span>
            <button type="button" aria-label={`复制 ${video.video_id} 磁力链接`} onClick={() => void copyMagnet(magnet)}>
              {copiedMagnet === magnet ? <CheckIcon /> : <CopyIcon />}{copiedMagnet === magnet ? '已复制' : '复制'}
            </button>
            <a href={magnet} aria-label={`打开 ${video.video_id} 磁力链接`} target="_blank" rel="noopener noreferrer"><OpenIcon />打开</a>
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
  return (
    <section className={`apply-summary ${failed ? 'has-errors' : ''}`} aria-live="polite">
      <span className="apply-icon">{failed ? <AlertIcon /> : <CheckIcon />}</span>
      <div>
        <strong>{failed ? '文件处理完成，部分项目需要注意' : '所选文件已全部移入'}</strong>
        <p>{moved} 个成功{failed ? `，${failed} 个未移动。失败项目已在列表中标记。` : '。可继续选择其他文件或重新扫描。'}</p>
      </div>
    </section>
  )
}

function Notice({ tone, title, body, action }: { tone: 'error' | 'warning'; title: string; body: string; action?: React.ReactNode }) {
  return (
    <div className={`notice notice-${tone}`} role="alert">
      <span><AlertIcon /></span>
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
function CopyIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><rect x="8" y="8" width="11" height="11" rx="2"/><path d="M16 8V6a2 2 0 0 0-2-2H6a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h2"/></svg> }
function OpenIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M14 4h6v6m0-6-9 9"/><path d="M19 13v6a1 1 0 0 1-1 1H5a1 1 0 0 1-1-1V6a1 1 0 0 1 1-1h6"/></svg> }
function FileIcon() { return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M6 3h8l4 4v14H6z"/><path d="M14 3v5h4"/></svg> }
function StatusIcon({ state }: { state: VideoState }) {
  if (state === 'exists') return <CheckIcon />
  if (state === 'additional_found') return <MoveIcon />
  if (state === 'magnet_found') return <svg viewBox="0 0 24 24" aria-hidden="true"><path d="M7 3v8a5 5 0 0 0 10 0V3M7 7h4m2 0h4"/></svg>
  if (state === 'missing') return <svg viewBox="0 0 24 24" aria-hidden="true"><circle cx="11" cy="11" r="7"/><path d="m16 16 4 4"/></svg>
  return <AlertIcon />
}
