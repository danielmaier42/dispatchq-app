import {cleanup, fireEvent, render, screen, waitFor} from '@testing-library/react'
import {afterEach, expect, it, vi} from 'vitest'
import type {Provider} from '@/lib/types'
import {ProviderEditDialog} from './ProviderEditDialog'
import {saveProvider} from './actions'

vi.mock('@/components/ui/toaster', () => ({toast: {success: vi.fn()}}))
vi.mock('./actions', () => ({saveProvider: vi.fn()}))
vi.mock('@/lib/queries', () => ({useProviders: () => ({data: []}), useModels: () => ({data: []})}))

const provider: Provider = {
  id: 'claude',
  kind: 'claude-code',
  name: 'Claude',
  binary_path: '',
  config_dir: '',
  default_model: '',
  fallback_provider: '',
  fallback_model: '',
  enabled: true,
  health: {state: 'ready', checked_at: ''},
  default: true,
  reasoning_effort: true,
  asides: true,
  type_name: 'Claude',
  env: [],
}
afterEach(() => {
  cleanup()
  vi.resetAllMocks()
})

it('keeps a rejected save visible in the fixed footer, outside the scrolling fields', async () => {
  vi.mocked(saveProvider).mockRejectedValue(new Error('Provider settings could not be saved'))
  const done = vi.fn()
  render(<ProviderEditDialog provider={provider} open onOpenChange={done} />)
  const save = screen.getByRole('button', {name: 'Save'})
  const form = save.closest('form')!
  const body = form.querySelector('.overflow-y-auto')!
  body.scrollTop = 1000
  fireEvent.click(save)
  await waitFor(() => expect(screen.getByText('Provider settings could not be saved')).toBeTruthy())
  const error = screen.getByText('Provider settings could not be saved')
  expect(body.contains(error)).toBe(false)
  expect(form.lastElementChild!.classList.contains('shrink-0')).toBe(true)
  expect(form.lastElementChild!.contains(error)).toBe(true)
  expect(done).not.toHaveBeenCalled()
  expect(save.hasAttribute('disabled')).toBe(false)
})
