import { LemonTag, Tooltip } from '@posthog/lemon-ui'

import { dayjs } from 'lib/dayjs'

import type {
    ScoutOriginEnumApi,
    SignalScoutConfigApi as SignalScoutConfig,
} from 'products/signals/frontend/generated/api.schemas'

/** Canonical (PostHog-maintained) vs Custom (team-authored) scout badge. */
export function ScoutOriginBadge({ origin }: { origin: ScoutOriginEnumApi }): JSX.Element {
    return (
        <Tooltip
            title={
                origin === 'canonical'
                    ? 'Part of the standard scout troop built and maintained by PostHog'
                    : 'A scout your team created as a signals-scout-* skill in this project'
            }
        >
            <LemonTag type={origin === 'canonical' ? 'muted' : 'highlight'} size="small">
                {origin === 'canonical' ? 'Canonical' : 'Custom'}
            </LemonTag>
        </Tooltip>
    )
}

/**
 * Where the scout stands with the inactivity sweep: paused after a stretch of surfacing nothing, or
 * heading that way. Nothing renders for a scout that's producing.
 */
export function ScoutInactivityBadge({ config }: { config: SignalScoutConfig }): JSX.Element | null {
    if (config.auto_paused_at) {
        return (
            <Tooltip
                title={`Paused on ${dayjs(config.auto_paused_at).format('MMMM D, YYYY')} because it went two weeks without surfacing anything anyone picked up. Switch it back on to resume it.`}
            >
                <LemonTag type="warning" size="small">
                    Paused
                </LemonTag>
            </Tooltip>
        )
    }
    if (config.auto_pause_warned_at) {
        return (
            <Tooltip title="It hasn't surfaced anything in the last two weeks, so it pauses in a week unless it finds something. Turn on 'keep running while quiet' in its settings to leave it alone.">
                <LemonTag type="caution" size="small">
                    Quiet
                </LemonTag>
            </Tooltip>
        )
    }
    return null
}
