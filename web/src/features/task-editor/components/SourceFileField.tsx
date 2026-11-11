import {FolderOpen} from 'lucide-react'
import {useState} from 'react'
import {Button} from '@/components/ui/button'
import {Field} from '@/components/ui/field'
import {Input} from '@/components/ui/input'
import {toast} from '@/components/ui/toaster'
import {Tooltip} from '@/components/ui/tooltip'
import {api, errorMessage} from '@/lib/api'
import type {PathResult, TaskKind} from '@/lib/types'

// The native "open file" dialog, opened by the daemon like the folder dialog.
// It starts in the folder of the file already in the field; 204 means it was
// cancelled. The path stays editable, so a browser without the dialog (a dev
// server against a daemon that has none) can still type it.
async function chooseFile(kind: TaskKind, current: string): Promise<string | null> {
  const q = new URLSearchParams({kind})
  if (current) q.set('path', current)
  const r = await api<PathResult | undefined>('POST', '/api/fs/choose-file?' + q.toString())
  return r?.path || null
}

export function SourceFileField({
  kind,
  value,
  error,
  onChange,
}: {
  kind: TaskKind
  value: string
  error?: string
  onChange: (path: string) => void
}) {
  const script = kind === 'script'
  const [choosing, setChoosing] = useState(false)
  const choose = async () => {
    setChoosing(true)
    try {
      const path = await chooseFile(kind, value.trim())
      if (path) onChange(path)
    } catch (e) {
      toast.error('File dialog unavailable', {description: errorMessage(e) + ' Type the full path instead.'})
    } finally {
      setChoosing(false)
    }
  }
  return (
    <Field
      label={script ? 'Script file' : 'Prompt file'}
      htmlFor="te-sourceFile"
      error={error}
      hint="Read at the start of every run, so edits to the file apply to the next run. If the file is missing then, the run fails and says so."
    >
      <div className="flex gap-2">
        <Input
          id="te-sourceFile"
          value={value}
          aria-invalid={!!error}
          placeholder={script ? 'Choose the script file…' : 'Choose the prompt file…'}
          spellCheck={false}
          autoComplete="off"
          className="font-mono text-[12.5px]"
          onChange={(e) => onChange(e.target.value)}
        />
        <Tooltip content={script ? 'Choose the script file' : 'Choose the prompt file'}>
          <Button type="button" onClick={choose} loading={choosing} aria-label="Choose file">
            {!choosing && <FolderOpen />}
            Choose…
          </Button>
        </Tooltip>
      </div>
    </Field>
  )
}
