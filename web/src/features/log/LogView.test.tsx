import {QueryClient, QueryClientProvider} from '@tanstack/react-query'
import {act, cleanup, render, screen} from '@testing-library/react'
import {afterEach, beforeEach, describe, expect, it, vi} from 'vitest'
import {TooltipProvider} from '@/components/ui/tooltip'
import type {Run} from '@/lib/types'
import {clearLogFilters, setLogFilters} from './filters-store'
import {LogView} from './LogView'

// The theme reads matchMedia when it loads, which jsdom does not have.
vi.hoisted(() => {
  window.matchMedia = ((query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  })) as unknown as typeof window.matchMedia
})

// What the daemon answers: the visible runs, and with hidden=1 the hidden ones too.
const lists = vi.hoisted(() => ({visible: [] as Run[], withHidden: [] as Run[]}))
vi.mock('@/lib/queries', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  useRuns: (withHidden = false) => ({data: withHidden ? lists.withHidden : lists.visible, isError: false}),
  useHealth: () => ({data: undefined}),
  useProviders: () => ({data: []}),
}))
vi.mock('@/features/dashboard/use-no-credit', () => ({useNoCredit: () => new Set<string>()}))

const tick: Run = {
  run_id: 'r1',
  task_id: 'watch',
  task_name: 'Feed watch',
  started_at: new Date().toISOString(),
  status: 'success',
  exit_code: 0,
  log_path: '',
  unread: false,
  quiet: true,
}

const show = () =>
  render(
    <QueryClientProvider client={new QueryClient()}>
      <TooltipProvider>
        <LogView />
      </TooltipProvider>
    </QueryClientProvider>,
  )

beforeEach(() => clearLogFilters())
afterEach(cleanup)

describe('LogView', () => {
  // An install whose only runs are a quiet watcher's successes lists nothing
  // by default; the filter that brings them back must still be reachable.
  it('keeps the filter bar when every run is hidden', () => {
    lists.visible = []
    lists.withHidden = [tick]
    show()
    expect(screen.getByRole('button', {name: /^Filter/})).toBeTruthy()
    expect(screen.getByText(/Show hidden runs/)).toBeTruthy()
  })

  // With the switch on, a list that comes up empty still shows the chip that
  // says so and turns it off.
  it('keeps the hidden-runs chip when the list with hidden runs is empty', () => {
    lists.visible = []
    lists.withHidden = []
    act(() => setLogFilters({hidden: true}))
    show()
    expect(screen.getByText('Hidden runs shown')).toBeTruthy()
    expect(screen.getByText('Nothing logged yet')).toBeTruthy()
    expect(screen.getByText(/hidden runs included/)).toBeTruthy()
  })
})
