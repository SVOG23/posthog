import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
    buildBlocks,
    enrich,
    fetchCandidatePools,
    flakyTestsUrl,
    REPORT_RUNNERS,
    selectReportCandidates,
    tableRows,
} from './weekly-flaky-report.mjs'

describe('weekly flaky report', () => {
    it('builds runner-specific endpoint URLs before the endpoint limit', () => {
        const pytestUrl = flakyTestsUrl('pytest')
        const jestUrl = flakyTestsUrl('jest')

        assert.deepEqual(REPORT_RUNNERS, ['pytest'])
        assert.equal(pytestUrl.searchParams.get('runner'), 'pytest')
        assert.equal(jestUrl.searchParams.get('runner'), 'jest')
        assert.equal(jestUrl.searchParams.get('repo'), 'PostHog/posthog')
        assert.equal(jestUrl.searchParams.get('limit'), '100')
    })

    it('builds a Slack table with supported cells and structured links', () => {
        const rows = tableRows(
            [
                {
                    selector: 'posthog/test/test_example.py::TestExample::test_report',
                    classification: 'confirmed_flake',
                    quarantined_failed_run_count: 0,
                    failed_run_count: 4,
                },
            ],
            () => ({ owner: 'team-devex', repoPath: 'posthog/test/test_example.py' }),
            () => ({
                runsRescued: 2,
                evidence: [
                    { runId: 10, jobId: 20 },
                    { runId: 11, jobId: 21 },
                ],
            })
        )
        const blocks = buildBlocks(new Date('2026-07-27T00:00:00Z'), rows)
        const table = blocks.find((block) => block.type === 'table')

        assert.ok(table)
        for (const tableCell of table.rows.flat()) {
            assert.ok(['raw_text', 'raw_number', 'rich_text'].includes(tableCell.type))
        }
        assert.deepEqual(rows[0][0], {
            type: 'rich_text',
            elements: [
                {
                    type: 'rich_text_section',
                    elements: [
                        {
                            type: 'link',
                            url: 'https://github.com/PostHog/posthog/blob/master/posthog/test/test_example.py',
                            text: 'test_report',
                        },
                    ],
                },
            ],
        })
        assert.deepEqual(rows[0][4], {
            type: 'rich_text',
            elements: [
                {
                    type: 'rich_text_section',
                    elements: [
                        {
                            type: 'link',
                            url: 'https://github.com/PostHog/posthog/actions/runs/10/job/20',
                            text: '1',
                        },
                        { type: 'text', text: ' ' },
                        {
                            type: 'link',
                            url: 'https://github.com/PostHog/posthog/actions/runs/11/job/21',
                            text: '2',
                        },
                    ],
                },
            ],
        })
    })

    it('selects proved flakes for the requested runner', () => {
        const common = {
            failed_run_count: 4,
            failed_pr_count: 1,
            master_failed_run_count: 3,
            quarantined_failed_run_count: 0,
        }
        const items = [
            { ...common, runner: 'pytest', selector: 'test_proved.py::test_proved', classification: 'confirmed_flake' },
            {
                ...common,
                runner: 'pytest',
                selector: 'test_burst.py::test_burst',
                classification: 'suspected_regression',
            },
            { ...common, runner: 'jest', selector: 'test_report.ts', classification: 'confirmed_flake' },
        ]

        assert.deepEqual(
            selectReportCandidates(items, 'pytest').map((candidate) => candidate.selector),
            ['test_proved.py::test_proved']
        )
        assert.deepEqual(
            selectReportCandidates(items, 'jest').map((candidate) => candidate.selector),
            ['test_report.ts']
        )
    })

    it('fetches each runner into its own candidate pool', async () => {
        const requestedRunners = []
        const pools = await fetchCandidatePools(['pytest', 'jest'], async (runner) => {
            requestedRunners.push(runner)
            return {
                items: [
                    { runner, selector: `${runner}.test`, classification: 'confirmed_flake' },
                    { runner: runner === 'pytest' ? 'jest' : 'pytest', selector: 'other.test' },
                ],
            }
        })

        assert.deepEqual(requestedRunners, ['pytest', 'jest'])
        assert.deepEqual(
            pools.map(({ runner, candidates }) => [runner, candidates.map((candidate) => candidate.selector)]),
            [
                ['pytest', ['pytest.test']],
                ['jest', ['jest.test']],
            ]
        )
    })

    it('scopes enrichment to the current repository', async () => {
        let request
        await enrich([{ selector: 'products/example/backend/test_report.py::test_report' }], async (query, values) => {
            request = { query, values }
            return { results: [] }
        })

        assert.match(request.query, /lower\(f\.repo\) = lower\(\{repository\}\)/)
        assert.equal(request.values.repository, 'PostHog/posthog')
        assert.deepEqual(request.values.selectors, [
            'products/example/backend/test_report.py::test_report',
            'backend/test_report.py::test_report',
        ])
    })
})
