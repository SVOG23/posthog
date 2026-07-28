import { LemonTag, Tooltip } from '@posthog/lemon-ui'

import type { ScoutOriginEnumApi } from 'products/signals/frontend/generated/api.schemas'

/**
 * Shown when a scout paused itself after failing repeatedly. Without it the pause is only
 * visible in the API response, and a scout that has silently stopped looks the same as one
 * that is running and finding nothing.
 */
export function ScoutPausedBadge({ reason }: { reason: string }): JSX.Element {
    return (
        <Tooltip
            title={
                <div className="flex flex-col gap-1 max-w-sm">
                    <span>
                        This scout paused itself because its last few runs all failed. It retries once a day, and picks
                        up its normal schedule again as soon as a run succeeds.
                    </span>
                    {reason ? <span className="text-muted line-clamp-3">Last error: {reason}</span> : null}
                </div>
            }
        >
            <LemonTag type="warning" size="small">
                Paused
            </LemonTag>
        </Tooltip>
    )
}

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
