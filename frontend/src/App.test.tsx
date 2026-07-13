import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'
import { normalizePlanEnvelope } from './api'
import type { FillActorPlan } from './types'

const plan: FillActorPlan = {
  plan_id: 'plan-1',
  revision: 'revision-1',
  created_at: '2026-07-13T10:00:00Z',
  expires_at: '2099-07-13T11:00:00Z',
  actors: [
    { actor_id: 'A123', scraped_count: 5, video_ids: ['ABC-001'], error_code: null },
    { actor_id: 'B456', scraped_count: 2, video_ids: ['XYZ-002'], error_code: null },
  ],
  videos: [
    {
      video_id: 'ABC-001',
      actor_ids: ['A123'],
      state: 'additional_found',
      existing_files: [],
      move_candidates: [
        { candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', source_label: 'additional-1', destination_conflict: false },
        { candidate_id: 'conflict-1', video_id: 'ABC-001', file_name: 'ABC-001-CD2.mp4', source_label: 'additional-2', destination_conflict: true },
      ],
      magnet: null,
      warnings: [],
    },
    {
      video_id: 'XYZ-002',
      actor_ids: ['B456'],
      state: 'magnet_found',
      existing_files: [],
      move_candidates: [],
      magnet: 'magnet:?xt=urn:btih:123456',
      warnings: [],
    },
    {
      video_id: 'DONE-003',
      actor_ids: ['A123'],
      state: 'exists',
      existing_files: ['DONE-003.mkv'],
      move_candidates: [],
      magnet: null,
      warnings: [],
    },
  ],
}

const magnetPlan: FillActorPlan = {
  ...plan,
  videos: [
    ...plan.videos,
    {
      video_id: 'XYZ-003',
      actor_ids: ['B456'],
      state: 'magnet_found',
      existing_files: [],
      move_candidates: [],
      magnet: 'magnet:?xt=urn:btih:ABCDEF',
      warnings: [],
    },
    {
      video_id: 'XYZ-004',
      actor_ids: ['B456'],
      state: 'magnet_found',
      existing_files: [],
      move_candidates: [],
      magnet: 'magnet:?xt=urn:btih:123456',
      warnings: [],
    },
    {
      video_id: 'XYZ-005',
      actor_ids: ['B456'],
      state: 'magnet_found',
      existing_files: [],
      move_candidates: [],
      magnet: 'https://example.com/not-a-magnet',
      warnings: [],
    },
  ],
}

function jsonResponse(body: unknown, status = 200) {
  return Promise.resolve(new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } }))
}

describe('Fill Actor page', () => {
  beforeEach(() => {
    window.sessionStorage.clear()
    vi.stubGlobal('fetch', vi.fn().mockImplementation(() => jsonResponse({ status: 'ok', database: 'ok', roots: 'ok' })))
  })

  it('validates and deduplicates actor IDs before rendering grouped scan results', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' }, plan }))

    render(<App />)
    const input = screen.getByLabelText('演员 ID')
    await user.type(input, 'A123, B456 A123')
    expect(screen.getByText('1 个重复项已自动合并')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    await screen.findByText('扫描结果')
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/fill-actor/plans',
      expect.objectContaining({ body: JSON.stringify({ actor_ids: ['A123', 'B456'] }), method: 'POST' }),
    )
    expect(screen.getByText('可移入')).toBeInTheDocument()
    expect(screen.getByText('可下载')).toBeInTheDocument()
    expect(screen.getAllByText('已入库')).toHaveLength(2)
    expect(screen.getByText('目标位置已有同名文件')).toBeInTheDocument()
    expect(screen.getByRole('checkbox', { name: /ABC-001\.mp4/ })).toBeChecked()
    expect(screen.getByRole('checkbox', { name: /ABC-001-CD2\.mp4/ })).toBeDisabled()
  })

  it('shows queued progress and polls until the persisted plan is ready', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'queued' }, plan: null }, 202))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' }, plan }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('任务已排队')).toBeInTheDocument()
    expect(screen.getByRole('progressbar', { name: '任务已排队进度' })).not.toHaveAttribute('aria-valuenow')
    expect(screen.getByRole('progressbar', { name: '任务已排队进度' })).toHaveAttribute('aria-valuetext', '进度计算中')
    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
  })

  it('renders stage progress, timing, ETA, freshness warnings, and accessible progress semantics', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    const now = Date.now()
    const running = {
      job: {
        job_id: 'job-1',
        plan_id: 'plan-1',
        state: 'running',
        updated_at: new Date(now - 36_000).toISOString(),
        progress: {
          stage: 'library_scan',
          completed: 3,
          total: 10,
          unit: 'videos',
          current: 'ABC-004',
          stage_started_at: new Date(now - 125_000).toISOString(),
          updated_at: new Date(now - 61_000).toISOString(),
          percent: 30,
          eta_seconds: 95,
          elapsed_seconds: 125,
          last_progress_seconds: 61,
        },
      },
      plan: null,
    }
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementation(() => jsonResponse(running, 202))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('正在扫描本地片库')).toBeInTheDocument()
    expect(screen.getByText('当前：ABC-004')).toBeInTheDocument()
    expect(screen.getByText('3 / 10 个作品')).toBeInTheDocument()
    expect(screen.getByText(/2 分 [5-7] 秒/)).toBeInTheDocument()
    expect(screen.getByText('约 1 分 35 秒')).toBeInTheDocument()
    expect(screen.getByText(/1 分 [1-3] 秒前/)).toBeInTheDocument()
    expect(screen.getByText('较长时间无新结果，仍可能在等待外部服务。')).toBeInTheDocument()
    expect(screen.getByText('执行器心跳已较长时间未更新，执行可能已经中断。')).toBeInTheDocument()
    expect(screen.getByRole('progressbar', { name: '正在扫描本地片库进度' })).toHaveAttribute('aria-valuenow', '30')
    expect(screen.getByRole('progressbar', { name: '正在扫描本地片库进度' })).toHaveAttribute(
      'aria-valuetext',
      '3 / 10 个作品',
    )
    const elapsedValue = screen.getByText('阶段已用时').parentElement?.querySelector('dd') ?? null
    expect(elapsedValue).not.toBeNull()
    const initialElapsed = elapsedValue?.textContent ?? ''
    await waitFor(() => expect(elapsedValue).not.toHaveTextContent(initialElapsed), { timeout: 1_800 })
  })

  it('keeps a running task through a transient poll failure and retries automatically', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'running' }, plan: null }, 202))
      .mockImplementationOnce(() => Promise.reject(new TypeError('temporary offline')))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' }, plan }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('暂时无法刷新任务状态，将自动重试。', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '正在扫描' })).toBeDisabled()
    expect(screen.queryByText('操作未完成')).not.toBeInTheDocument()
    expect(await screen.findByText('扫描结果', {}, { timeout: 4_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
  })

  it('recovers an active scan from session storage and clears it after completion', async () => {
    window.sessionStorage.setItem('embyx-web-active-plan-id', 'resume-1')
    const resumedPlan = { ...plan, plan_id: 'resume-1' }
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ job: { job_id: 'resume-1', plan_id: 'resume-1', state: 'completed' }, plan: resumedPlan }))

    render(<App />)

    expect(await screen.findByText('正在恢复扫描状态')).toBeInTheDocument()
    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      '/api/fill-actor/plans/resume-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
    await waitFor(() => expect(window.sessionStorage.getItem('embyx-web-active-plan-id')).toBeNull())
  })

  it('requires confirmation and displays per-file apply results', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse(plan))
      .mockImplementationOnce(() =>
        jsonResponse({
          plan_id: 'plan-1',
          revision: 'revision-1',
          state: 'succeeded',
          results: [{ candidate_id: 'safe-1', video_id: 'ABC-001', file_name: 'ABC-001.mp4', state: 'moved', error_code: null }],
        }),
      )

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')

    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    const dialog = screen.getByRole('dialog')
    expect(within(dialog).getByText('确认移入 1 个文件？')).toBeInTheDocument()
    await user.click(within(dialog).getByRole('button', { name: '确认移入' }))

    await screen.findByText('所选文件已全部移入')
    expect(screen.getByText('已移入')).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1/apply',
      expect.objectContaining({ body: JSON.stringify({ revision: 'revision-1', candidate_ids: ['safe-1'] }) }),
    )
  })

  it('copies every valid unique magnet in plan order and has no per-row magnet actions', async () => {
    const user = userEvent.setup()
    const clipboardSpy = vi.spyOn(navigator.clipboard, 'writeText').mockResolvedValue()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse(magnetPlan))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')

    expect(screen.queryByRole('button', { name: /复制 .*磁力链接/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('link', { name: /打开 .*磁力链接/ })).not.toBeInTheDocument()
    expect(document.querySelector('a[href^="magnet:"]')).toBeNull()

    await user.click(screen.getByRole('button', { name: '复制全部磁力（2）' }))
    expect(clipboardSpy).toHaveBeenCalledTimes(1)
    expect(clipboardSpy).toHaveBeenCalledWith(
      'magnet:?xt=urn:btih:123456\nmagnet:?xt=urn:btih:ABCDEF',
    )
    expect(screen.getByRole('button', { name: '已复制 2 个磁力' })).toBeInTheDocument()
  })

  it('keeps polling completed plans while RSSHub warms and retains terminal feed states', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    const warmingFeed = {
      actor_id: 'A123',
      state: 'warming',
      attempts: 1,
      updated_at: '2026-07-13T10:01:00Z',
      error_code: null,
      freshrss_add_url: 'https://freshrss.example/i/?c=feed&a=add',
      freshrss_url: 'https://freshrss.example/',
    }
    const readyFeed = {
      ...warmingFeed,
      state: 'ready',
      attempts: 2,
      updated_at: '2026-07-13T10:02:00Z',
      freshrss_add_url: 'https://freshrss.example/i/?c=subscription&a=add&url_rss=https%3A%2F%2Frsshub.example%2Factress%2FA123',
    }
    const failedFeed = {
      actor_id: 'B456',
      state: 'failed',
      attempts: 3,
      updated_at: '2026-07-13T10:02:00Z',
      error_code: 'rsshub_timeout',
      freshrss_add_url: null,
      freshrss_url: null,
    }
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [warmingFeed],
      }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [readyFeed, failedFeed],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('缓存预热中')).toBeInTheDocument()
    expect(screen.getByText('RSSHub 正在预热缓存，页面会自动更新。')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '一键添加到 FreshRSS' })).not.toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '打开 FreshRSS' })).not.toBeInTheDocument()

    const addLink = await screen.findByRole('link', { name: '一键添加到 FreshRSS' }, { timeout: 2_000 })
    const freshrssLink = screen.getByRole('link', { name: '打开 FreshRSS' })
    expect(addLink).toHaveAttribute('href', readyFeed.freshrss_add_url)
    expect(addLink).toHaveAttribute('target', '_blank')
    expect(addLink).toHaveAttribute('rel', 'noopener noreferrer')
    expect(freshrssLink).toHaveAttribute('href', readyFeed.freshrss_url)
    expect(freshrssLink).toHaveAttribute('target', '_blank')
    expect(freshrssLink).toHaveAttribute('rel', 'noopener noreferrer')
    expect(screen.getByText('缓存已就绪')).toBeInTheDocument()
    expect(screen.getByText('缓存失败')).toBeInTheDocument()
    expect(screen.getByText('错误：rsshub_timeout')).toBeInTheDocument()
    expect(screen.getByText('已尝试 2 次', { exact: false })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ cache: 'no-store', signal: expect.any(AbortSignal) }),
    )
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('hides unsafe FreshRSS actions even when the feed is ready', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({
        job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
        plan,
        feeds: [{
          actor_id: 'A123',
          state: 'ready',
          attempts: 2,
          updated_at: '2026-07-13T10:02:00Z',
          error_code: null,
          freshrss_add_url: 'javascript:alert(1)',
          freshrss_url: 'data:text/html,unsafe',
        }],
      }))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    expect(await screen.findByText('缓存已就绪')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '一键添加到 FreshRSS' })).not.toBeInTheDocument()
    expect(screen.queryByRole('link', { name: '打开 FreshRSS' })).not.toBeInTheDocument()
  })

  it('normalizes old feed responses without a FreshRSS site URL', () => {
    const oldFeed = {
      actor_id: 'A123',
      state: 'ready',
      attempts: 2,
      updated_at: '2026-07-13T10:02:00Z',
      error_code: null,
      freshrss_add_url: 'https://freshrss.example/i/?c=feed&a=add',
    }

    expect(normalizePlanEnvelope({
      job: { job_id: 'job-1', plan_id: 'plan-1', state: 'completed' },
      plan: null,
      feeds: [oldFeed],
    }).feeds).toEqual([{ ...oldFeed, freshrss_url: null }])
  })

  it('shows an explicit recovery action when apply reports an expired plan', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse(plan))
      .mockImplementationOnce(() => jsonResponse({ error: { code: 'expired_plan' } }, 410))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('扫描结果')
    await user.click(screen.getByRole('button', { name: '确认并移入' }))
    await user.click(within(screen.getByRole('dialog')).getByRole('button', { name: '确认移入' }))

    await waitFor(() => expect(screen.getByText('扫描结果已失效')).toBeInTheDocument())
    expect(screen.getByRole('button', { name: /重新扫描/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: '确认并移入' })).toBeDisabled()
  })

  it('stores a required API token only for the browser session', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.mocked(fetch)
    fetchMock
      .mockImplementationOnce(() => jsonResponse({ status: 'ok' }))
      .mockImplementationOnce(() => jsonResponse({ error: { code: 'unauthorized' } }, 401))
      .mockImplementationOnce(() => jsonResponse(plan))

    render(<App />)
    await user.type(screen.getByLabelText('演员 ID'), 'A123')
    await user.click(screen.getByRole('button', { name: '开始扫描' }))
    await screen.findByText('需要 API Token')
    await user.type(screen.getByLabelText('API Token'), 'session-token')
    await user.click(screen.getByRole('button', { name: '保存 Token' }))
    await user.click(screen.getByRole('button', { name: '开始扫描' }))

    await screen.findByText('扫描结果')
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans',
      expect.objectContaining({
        headers: expect.objectContaining({ Authorization: 'Bearer session-token' }),
      }),
    )
    expect(window.sessionStorage.getItem('embyx-web-api-token')).toBe('session-token')
  })
})
