import type { MCPToolPivotApi, MCPToolPivotClusterEntryApi } from '../generated/api.schemas'
import { weightedMeanFit } from './mcpClusteringLogic'

function entry(overrides: Partial<MCPToolPivotClusterEntryApi>): MCPToolPivotClusterEntryApi {
    return {
        cluster_id: 0,
        label: 'intent',
        calls: 1,
        capture_pct: 100,
        rank: 1,
        cluster_call_count: 1,
        cluster_entropy: 0,
        description_fit: null,
        top_competitor: null,
        ...overrides,
    }
}

function pivot(clusters: MCPToolPivotClusterEntryApi[]): MCPToolPivotApi {
    return {
        tool: 't',
        call_count: clusters.reduce((sum, c) => sum + c.calls, 0),
        error_count: 0,
        session_count: 1,
        contested_score: null,
        advertised_sessions: 0,
        called_when_advertised: 0,
        discovery_rate_pct: null,
        description: null,
        clusters,
    }
}

describe('mcpClusteringLogic helpers', () => {
    // The scatter plots one fit value per tool; if the weighting silently broke,
    // a tool's dominant intent would no longer dominate its plotted position.
    it('weightedMeanFit weights each cluster fit by its call volume', () => {
        const tool = pivot([
            entry({ cluster_id: 0, calls: 9, description_fit: 1.0 }),
            entry({ cluster_id: 1, calls: 1, description_fit: 0.0 }),
        ])
        expect(weightedMeanFit(tool)).toBeCloseTo(0.9)
    })

    it.each([
        ['no cluster has a fit', [entry({ description_fit: null })]],
        ['no clusters at all', [] as MCPToolPivotClusterEntryApi[]],
        ['fits exist but carry zero calls', [entry({ calls: 0, description_fit: 0.5 })]],
    ])('weightedMeanFit is null when %s', (_name, clusters) => {
        expect(weightedMeanFit(pivot(clusters))).toBeNull()
    })

    it('weightedMeanFit ignores fitless clusters instead of treating them as zero', () => {
        const tool = pivot([
            entry({ cluster_id: 0, calls: 1, description_fit: 0.8 }),
            entry({ cluster_id: 1, calls: 99, description_fit: null }),
        ])
        expect(weightedMeanFit(tool)).toBeCloseTo(0.8)
    })
})
