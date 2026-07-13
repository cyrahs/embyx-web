export type VideoState =
  | 'exists'
  | 'additional_found'
  | 'magnet_found'
  | 'missing'
  | 'invalid_video_id'
  | 'scan_failed'

export type MoveState = 'moved' | 'stale' | 'conflict' | 'invalid_path' | 'failed'
export type ApplyState = 'succeeded' | 'partial_failed' | 'failed'
export type JobState = 'queued' | 'running' | 'completed' | 'partial_failed' | 'failed'
export type ActorFeedState = 'queued' | 'warming' | 'ready' | 'failed'

export interface ActorPlan {
  actor_id: string
  scraped_count: number
  video_ids: string[]
  error_code: string | null
}
export interface MoveCandidate {
  candidate_id: string
  video_id: string
  file_name: string
  source_label: string
  destination_conflict: boolean
}

export interface VideoPlan {
  video_id: string
  actor_ids: string[]
  state: VideoState
  existing_files: string[]
  move_candidates: MoveCandidate[]
  magnet: string | null
  warnings: string[]
}

export interface FillActorPlan {
  plan_id: string
  revision: string
  created_at: string
  expires_at: string
  actors: ActorPlan[]
  videos: VideoPlan[]
}

export interface MoveResult {
  candidate_id: string
  video_id: string
  file_name: string
  state: MoveState
  error_code: string | null
}

export interface ApplyResult {
  plan_id: string
  revision: string
  state: ApplyState
  results: MoveResult[]
}

export interface JobProgress {
  stage?: string | null
  completed?: number
  total?: number | null
  unit?: string | null
  current?: string | null
  stage_started_at?: string | null
  updated_at?: string | null
  percent?: number | null
  eta_seconds?: number | null
  elapsed_seconds?: number | null
  last_progress_seconds?: number | null
}

export interface PlanJob {
  id?: string
  job_id?: string
  plan_id?: string
  state?: JobState
  status?: JobState
  error_code?: string | null
  updated_at?: string | null
  progress?: JobProgress | null
}

export interface ActorFeedStatus {
  actor_id: string
  state: ActorFeedState
  attempts: number
  updated_at: string
  error_code: string | null
  freshrss_add_url: string | null
  freshrss_url: string | null
}

export interface PlanEnvelope {
  plan: FillActorPlan | null
  job: PlanJob | null
  planId: string | null
  feeds: ActorFeedStatus[]
}
