import { parseFilterGroup } from '../sampling/compile-rules'
import { type CompiledRetentionRule, type CompiledRetentionRuleSet, VALID_RETENTION_DAYS } from './evaluate-retention'

export type RetentionRuleRow = {
    id: string
    config: Record<string, unknown>
    /** Row version from DB; used by the cache watermark only, ignored by compileRetentionRuleSet. */
    version?: number
}

export function compileRetentionRuleSet(rows: RetentionRuleRow[]): CompiledRetentionRuleSet {
    const rules: CompiledRetentionRule[] = []
    for (const row of rows) {
        const config = row.config ?? {}
        const retentionDays = config.retention_days
        // Skip rows the API validator would have rejected — robustness against legacy or
        // hand-crafted rows. A `true` boolean is not accepted (typeof true === 'boolean').
        if (typeof retentionDays !== 'number' || !Number.isInteger(retentionDays)) {
            continue
        }
        if (!VALID_RETENTION_DAYS.has(retentionDays)) {
            continue
        }
        // parseFilterGroup bounds depth/breadth and pre-compiles regex leaves; returns null for
        // a missing/malformed group, which evaluateRetentionDays treats as match-nothing.
        const filterGroup = parseFilterGroup(config.filter_group)
        rules.push({ id: row.id, filterGroup, retentionDays })
    }
    return { rules }
}
