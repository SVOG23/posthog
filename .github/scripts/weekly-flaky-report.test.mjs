import assert from 'node:assert/strict'
import { describe, it } from 'node:test'

import {
    buildBlocks,
    buildRunnerReports,
    enrich,
    enrichRunnerCandidates,
    fetchCandidatePools,
    flakyTestsUrl,
    REPORT_RUNNERS,
    repoPathResolver,
    selectReportCandidates,
    tableRows,
    trackedTestPaths,
} from './weekly-flaky-report.mjs'

describe('weekly flaky report', () => {
    it('builds runner-specific endpoint URLs before the endpoint limit', () => {
        const pytestUrl = flakyTestsUrl('pytest')
        const jestUrl = flakyTestsUrl('jest')

        assert.deepEqual(REPORT_RUNNERS, ['pytest', 'jest'])
        assert.equal(pytestUrl.searchParams.get('runner'), 'pytest')
        assert.equal(jestUrl.searchParams.get('runner'), 'jest')
        assert.equal(jestUrl.searchParams.get('repo'), 'PostHog/posthog')
        assert.equal(jestUrl.searchParams.get('limit'), '100')
    })

    it('builds a Slack table with supported cells and structured links', () => {
        const rows = tableRows(
            [
                {
                    runner: 'pytest',
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
        assert.deepEqual(
            table.rows[0].map((tableCell) => tableCell.text),
            ['test', 'runner', 'owner', 'rescued', 'fails', 'logs']
        )
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
        assert.deepEqual(rows[0][1], { type: 'raw_text', text: 'pytest' })
        assert.deepEqual(rows[0][5], {
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

    it('ranks and limits each runner independently', async () => {
        const candidatePools = ['pytest', 'jest'].map((runner) => ({
            runner,
            candidates: Array.from({ length: 12 }, (_, index) => ({
                runner,
                selector: `${runner}-${index}`,
                failed_run_count: index === 10 ? 20 : 5,
            })),
        }))
        const runnerReports = await buildRunnerReports(candidatePools, async () => (item) => ({
            runsRescued: item.selector.endsWith('-11') ? 1 : 0,
            evidence: [],
        }))

        for (const { runner, candidates } of runnerReports) {
            assert.equal(candidates.length, 10)
            assert.deepEqual(
                candidates.map((candidate) => candidate.selector),
                [`${runner}-11`, `${runner}-10`, ...Array.from({ length: 8 }, (_, index) => `${runner}-${index}`)]
            )
        }
        assert.deepEqual(
            runnerReports.flatMap(({ candidates }) => candidates.map((candidate) => candidate.runner)),
            [...Array(10).fill('pytest'), ...Array(10).fill('jest')]
        )
    })

    it('resolves tracked Python and JavaScript-family test paths', () => {
        let gitArguments
        const expectedPaths = [
            'posthog/test/test_report.py',
            'frontend/src/report.test.js',
            'frontend/src/report.test.jsx',
            'frontend/src/report.test.ts',
            'frontend/src/report.test.tsx',
        ]
        const trackedPaths = trackedTestPaths((command, args) => {
            gitArguments = { command, args }
            return expectedPaths.join('\n')
        })
        const toRepoPaths = repoPathResolver(trackedPaths)

        assert.deepEqual(gitArguments, {
            command: 'git',
            args: ['ls-files', '*.py', '*.js', '*.jsx', '*.ts', '*.tsx'],
        })
        for (const path of expectedPaths) {
            assert.deepEqual(toRepoPaths(path), [path])
        }
        assert.deepEqual(toRepoPaths('src/report.test.tsx'), ['frontend/src/report.test.tsx'])
    })

    it('omits unsupported Jest enrichment and renders fallback cells', async () => {
        let enrichmentRequested = false
        const item = {
            runner: 'jest',
            selector: 'frontend/src/report.test.ts::renders the report',
            classification: 'confirmed_flake',
            quarantined_failed_run_count: 0,
            failed_run_count: 3,
        }
        const extrasFor = await enrichRunnerCandidates('jest', [item], async () => {
            enrichmentRequested = true
            return { results: [] }
        })
        const [row] = tableRows(
            [item],
            () => ({ owner: 'team-devex', repoPath: 'frontend/src/report.test.ts' }),
            extrasFor
        )

        assert.equal(enrichmentRequested, false)
        assert.deepEqual(row[1], { type: 'raw_text', text: 'Jest' })
        assert.deepEqual(row[3], { type: 'raw_text', text: '-' })
        assert.deepEqual(row[5], { type: 'raw_text', text: '-' })
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
