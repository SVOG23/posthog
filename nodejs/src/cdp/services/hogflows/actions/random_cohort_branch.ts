import { createHash } from 'node:crypto'

import { HogFlowAction } from '~/cdp/schema/hogflow'
import { CyclotronJobInvocationHogFlow } from '~/cdp/types'

import { findNextAction } from '../hogflow-utils'
import { ActionHandler, ActionHandlerOptions, ActionHandlerResult } from './action.interface'

type Action = Extract<HogFlowAction, { type: 'random_cohort_branch' }>

// The top 60 bits of a sha1 digest, the divisor used by feature-flag bucketing.
const LONG_SCALE = 0xfffffffffffffff

/**
 * Deterministic [0, 1) value for a key, using the same algorithm feature flags bucket a distinct id
 * with (calculate_hash in rust/feature-flags/src/flags/flag_matching_utils.rs). Keeping the two
 * aligned means "30% of people" is computed the same way in a workflow split and in a flag rollout.
 */
function hashToUnitInterval(key: string): number {
    const digest = createHash('sha1').update(key).digest()
    // Top 8 bytes shifted right by 4 is the first 15 hex characters, as the Rust implementation does.
    // BigInt because a 60-bit integer doesn't fit exactly in a double before the division.
    return Number(digest.readBigUInt64BE(0) >> 4n) / LONG_SCALE
}

/** The 0-100 value a cohort is selected with: stable per person when sticky, otherwise random. */
function getBucketValue(invocation: CyclotronJobInvocationHogFlow, action: Action): number {
    if (!action.config.sticky_assignment) {
        return Math.random() * 100 // 0-100
    }

    const personId = invocation.person?.id
    if (!personId) {
        // Sticky assignment has nothing stable to key on. Person-less (row-scoped) triggers can't
        // reach this node at all because the API rejects them, so this only happens when a person
        // failed to resolve. Falling back to random keeps that run moving instead of failing it.
        return Math.random() * 100
    }

    // Salted per flow and action so two splits in the same workflow assign independently, instead of
    // sending every person down the same relative branch in both.
    return hashToUnitInterval(`${invocation.hogFlow.id}.${action.id}.${personId}`) * 100
}

export class RandomCohortBranchHandler implements ActionHandler {
    execute({
        invocation,
        action,
    }: ActionHandlerOptions<Extract<HogFlowAction, { type: 'random_cohort_branch' }>>): ActionHandlerResult {
        const nextAction = getRandomCohort(invocation, action)
        return { nextAction, result: { assigned_cohort: nextAction.id } }
    }
}

export function getRandomCohort(invocation: CyclotronJobInvocationHogFlow, action: Action): HogFlowAction {
    // Programmatically-authored nodes can be stored without their cohorts array (the API doesn't
    // require it on lenient saves); assign nothing and fall through the continue edge instead of
    // crashing the run.
    const cohorts = Array.isArray(action.config.cohorts) ? action.config.cohorts : []
    if (cohorts.length === 0) {
        return findNextAction(invocation.hogFlow, action.id)
    }

    const random = getBucketValue(invocation, action)
    let cumulativePercentage = 0

    for (const [index, cohort] of cohorts.entries()) {
        cumulativePercentage += cohort.percentage
        if (random <= cumulativePercentage) {
            return findNextAction(invocation.hogFlow, action.id, index)
        }
    }

    // If we somehow get here (shouldn't happen if percentages add up to 100),
    // go to the last cohort
    return findNextAction(invocation.hogFlow, action.id, cohorts.length - 1)
}
