import type React from 'react'
import {act, cleanup, fireEvent, render, screen} from '@testing-library/react'
import {afterEach, describe, expect, it, vi} from 'vitest'
import {TooltipProvider} from '@/components/ui/tooltip'
import {SourceFileField} from './SourceFileField'

const wrap = (ui: React.ReactElement) => render(<TooltipProvider>{ui}</TooltipProvider>)

const {apiMock} = vi.hoisted(() => {
  window.matchMedia = ((query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  })) as unknown as typeof window.matchMedia
  return {apiMock: vi.fn()}
})
vi.mock('@/lib/api', async (importOriginal) => ({
  ...(await importOriginal<object>()),
  api: apiMock,
}))

afterEach(() => {
  cleanup()
  apiMock.mockReset()
})

describe('SourceFileField', () => {
  it('fills in the file chosen in the native dialog', async () => {
    apiMock.mockResolvedValueOnce({path: '/Users/me/scripts/watch.sh'})
    const onChange = vi.fn()
    wrap(<SourceFileField kind="script" value="/Users/me/scripts/old.sh" onChange={onChange} />)
    await act(async () => {
      fireEvent.click(screen.getByRole('button', {name: 'Choose file'}))
    })
    expect(apiMock).toHaveBeenCalledWith(
      'POST',
      '/api/fs/choose-file?kind=script&path=%2FUsers%2Fme%2Fscripts%2Fold.sh',
    )
    expect(onChange).toHaveBeenCalledWith('/Users/me/scripts/watch.sh')
  })

  it('keeps the field as it was when the dialog is cancelled', async () => {
    apiMock.mockResolvedValueOnce(undefined)
    const onChange = vi.fn()
    wrap(<SourceFileField kind="agent" value="" onChange={onChange} />)
    await act(async () => {
      fireEvent.click(screen.getByRole('button', {name: 'Choose file'}))
    })
    expect(apiMock).toHaveBeenCalledWith('POST', '/api/fs/choose-file?kind=agent')
    expect(onChange).not.toHaveBeenCalled()
  })

  it('leaves the path typeable', () => {
    const onChange = vi.fn()
    wrap(<SourceFileField kind="agent" value="" onChange={onChange} />)
    fireEvent.change(screen.getByLabelText('Prompt file'), {target: {value: '/briefs/x.md'}})
    expect(onChange).toHaveBeenCalledWith('/briefs/x.md')
  })
})
