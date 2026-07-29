import { expectLogic } from 'kea-test-utils'

import { useMocks } from '~/mocks/jest'
import { initKeaTests } from '~/test/init'

import type { ReplayScannerTemplateApi } from '../generated/api.schemas'
import { scannerTemplatesLogic } from './scannerTemplatesLogic'

const template = (id: string, sourceScanner: string): ReplayScannerTemplateApi => ({
    id,
    name: `Template ${id}`,
    description: 'Reusable scanner configuration',
    scanner_type: 'monitor',
    scanner_config: { prompt: 'What did the user do next?' },
    query: { kind: 'RecordingsQuery' },
    sampling_rate: 1,
    sampling_mode: 'comprehensive',
    provider: 'google',
    model: 'gemini-3-flash-preview',
    emits_signals: false,
    source_scanner: sourceScanner,
    created_by: null,
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
})

describe('scannerTemplatesLogic', () => {
    let logic: ReturnType<typeof scannerTemplatesLogic.build>

    beforeEach(() => {
        useMocks({
            get: {
                '/api/projects/:team/vision/scanner_templates/': {
                    count: 1,
                    next: null,
                    previous: null,
                    results: [template('saved', 'scanner-1')],
                },
            },
            post: {
                '/api/projects/:team/vision/scanners/:id/save_as_template/': () => [201, template('new', 'scanner-2')],
            },
            delete: {
                '/api/projects/:team/vision/scanner_templates/:id/': () => [204, null],
            },
        })
        initKeaTests()
        logic = scannerTemplatesLogic()
        logic.mount()
    })

    afterEach(() => logic.unmount())

    it('loads, saves, and deletes reusable scanner templates', async () => {
        await expectLogic(logic)
            .toDispatchActions(['loadTemplatesSuccess'])
            .toMatchValues({
                customTemplates: [expect.objectContaining({ id: 'saved' })],
            })

        await expectLogic(logic, () => logic.actions.saveTemplate('scanner-2'))
            .toFinishAllListeners()
            .toMatchValues({
                customTemplates: [expect.objectContaining({ id: 'new' }), expect.objectContaining({ id: 'saved' })],
                savingScannerIds: [],
            })

        await expectLogic(logic, () => logic.actions.deleteTemplate('saved'))
            .toFinishAllListeners()
            .toMatchValues({
                customTemplates: [expect.objectContaining({ id: 'new' })],
                deletingTemplateIds: [],
            })
    })
})
