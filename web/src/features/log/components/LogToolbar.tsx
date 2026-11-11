import {DateRange, dateRangeLabel} from '@/components/ui/date-range'
import {FilterChip} from '@/components/ui/filter-chip'
import {FilterPopover, FilterSelect, optionLabel} from '@/components/ui/filter-popover'
import {Field} from '@/components/ui/field'
import {SearchInput} from '@/components/ui/search-input'
import {Segmented} from '@/components/ui/segmented'
import {Switch} from '@/components/ui/switch'
import {setLogFilters} from '../filters-store'
import type {FilterMenus, LogFilters, Show} from '../runs'

// The filter row under the Log's title: search and the All | Running | Failed switch
// stay in sight, the menus (filled from the runs there are) and the days sit
// behind the Filter button, and whatever is set there shows as a chip.
// `f` is what the list applies: a stale menu choice already fell back to all.
export function LogToolbar({f, menus}: {f: LogFilters; menus: FilterMenus}) {
  const menu = [
    {key: 'group', label: 'Group', value: f.group, options: menus.groups},
    {key: 'task', label: 'Task', value: f.task, options: menus.tasks},
    {key: 'provider', label: 'Provider', value: f.provider, options: menus.providers},
    {key: 'status', label: 'Outcome', value: f.status, options: menus.statuses},
  ] as const
  const days = dateRangeLabel(f.from, f.to)
  const active = menu.filter((m) => m.value).length + (days ? 1 : 0) + (f.hidden ? 1 : 0)

  return (
    <>
      <SearchInput
        value={f.query}
        onChange={(query) => setLogFilters({query})}
        placeholder="Search runs"
        hotkey
        className="w-60"
      />
      <Segmented<Show>
        aria-label="Show"
        value={f.show}
        onValueChange={(show) => setLogFilters({show})}
        options={[
          {value: '', label: 'All', tip: 'Show every run'},
          {value: 'running', label: 'Running', tip: 'Show only the runs that are executing right now'},
          {
            value: 'failed',
            label: 'Failed',
            tip: 'Show only the runs that ended in an error, auth errors included',
          },
        ]}
      />
      <FilterPopover
        active={active}
        onReset={() =>
          setLogFilters({group: '', task: '', provider: '', status: '', from: '', to: '', hidden: false})
        }
      >
        <FilterSelect
          label="Group"
          all="All groups"
          value={f.group}
          options={menus.groups}
          onChange={(group) => setLogFilters({group, task: ''})}
        />
        <FilterSelect
          label="Task"
          hint={f.group ? 'Tasks of the chosen group' : undefined}
          all="All tasks"
          value={f.task}
          options={menus.tasks}
          onChange={(task) => setLogFilters({task})}
        />
        <FilterSelect
          label="Provider"
          all="All providers"
          value={f.provider}
          options={menus.providers}
          onChange={(provider) => setLogFilters({provider})}
        />
        <FilterSelect
          label="Outcome"
          all="All outcomes"
          value={f.status}
          options={menus.statuses}
          onChange={(status) => setLogFilters({status})}
        />
        <Field label="Started">
          <DateRange
            from={f.from}
            to={f.to}
            onFrom={(from) => setLogFilters({from})}
            onTo={(to) => setLogFilters({to})}
          />
        </Field>
        <Field
          label="Hidden runs"
          hint="Successful runs of quiet-history tasks, kept for 48 hours. Hidden again when the window reloads."
        >
          <label className="flex cursor-pointer items-center justify-between gap-3 text-[13px]">
            Show hidden runs
            <Switch
              checked={f.hidden}
              aria-label="Show hidden runs"
              onCheckedChange={(hidden) => setLogFilters({hidden})}
            />
          </label>
        </Field>
      </FilterPopover>
      {menu.map(
        (m) =>
          m.value && (
            <FilterChip
              key={m.key}
              label={m.label}
              value={optionLabel(m.options, m.value)}
              onRemove={() => setLogFilters({[m.key]: ''})}
            />
          ),
      )}
      {days && <FilterChip value={days} onRemove={() => setLogFilters({from: '', to: ''})} />}
      {f.hidden && <FilterChip value="Hidden runs shown" onRemove={() => setLogFilters({hidden: false})} />}
    </>
  )
}
