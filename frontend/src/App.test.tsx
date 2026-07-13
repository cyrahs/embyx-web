import { render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import App from './App'
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
    expect(await screen.findByText('扫描结果', {}, { timeout: 2_000 })).toBeInTheDocument()
    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      '/api/fill-actor/plans/plan-1',
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    )
  })

  it('requires confirmation, displays per-file apply results, and copies magnet text', async () => {
    const user = userEvent.setup()
    const clipboardSpy = vi.spyOn(navigator.clipboard, 'writeText')
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

    await user.click(screen.getByRole('button', { name: '复制 XYZ-002 磁力链接' }))
    expect(clipboardSpy).toHaveBeenCalledWith('magnet:?xt=urn:btih:123456')
    expect(screen.getByRole('link', { name: '打开 XYZ-002 磁力链接' })).toHaveAttribute('rel', 'noopener noreferrer')
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
