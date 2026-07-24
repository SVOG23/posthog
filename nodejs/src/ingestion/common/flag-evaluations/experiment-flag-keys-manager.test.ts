import { defaultConfig } from '~/common/config/config'
import { PostgresRouter } from '~/common/utils/db/postgres'
import { commonOrganizationId, createTeam, insertRow, resetTestDatabase } from '~/tests/helpers/sql'

import { ExperimentFlagKeysManager } from './experiment-flag-keys-manager'

describe('ExperimentFlagKeysManager', () => {
    let postgres: PostgresRouter
    let manager: ExperimentFlagKeysManager
    let teamId: number

    const insertFeatureFlag = async (team: number, key: string, deleted = false): Promise<number> => {
        const row = await insertRow(postgres, 'posthog_featureflag', {
            key,
            name: '',
            filters: '{}',
            created_at: new Date().toISOString(),
            deleted,
            active: true,
            archived: false,
            team_id: team,
        })
        return row.id
    }

    const insertExperiment = async (team: number, featureFlagId: number, deleted = false): Promise<void> => {
        await insertRow(postgres, 'posthog_experiment', {
            name: 'Test experiment',
            filters: '{}',
            created_at: new Date().toISOString(),
            updated_at: new Date().toISOString(),
            archived: false,
            feature_flag_id: featureFlagId,
            team_id: team,
            deleted,
            only_count_matured_users: false,
            feature_flag_auto_archived: false,
        })
    }

    // The manager exposes batched per-team sets; these tests are about one flag at a time.
    const hasExperimentFlag = async (team: number, key: string): Promise<boolean> =>
        (await manager.getExperimentFlagKeys([team]))[String(team)]?.has(key) ?? false

    beforeEach(async () => {
        await resetTestDatabase()
        postgres = new PostgresRouter(defaultConfig)
        manager = new ExperimentFlagKeysManager(postgres)
        teamId = await createTeam(postgres, commonOrganizationId)
    })

    afterEach(async () => {
        await postgres.end()
    })

    it.each([
        ['returns true when a live experiment backs the flag', false, false, true],
        ['returns false when the experiment linking the flag is deleted', false, true, false],
        ['returns false when the feature flag itself is deleted', true, false, false],
    ])('%s', async (_case, flagDeleted, experimentDeleted, expected) => {
        const flagId = await insertFeatureFlag(teamId, 'my-experiment-flag', flagDeleted)
        await insertExperiment(teamId, flagId, experimentDeleted)

        const result = await hasExperimentFlag(teamId, 'my-experiment-flag')

        expect(result).toBe(expected)
    })

    it('returns false for a team with no experiments at all', async () => {
        const result = await hasExperimentFlag(teamId, 'nonexistent-flag')

        expect(result).toBe(false)
    })

    it("does not leak another team's experiment flag key", async () => {
        const otherTeamId = await createTeam(postgres, commonOrganizationId)
        const flagId = await insertFeatureFlag(otherTeamId, 'shared-key-name')
        await insertExperiment(otherTeamId, flagId)

        const result = await hasExperimentFlag(teamId, 'shared-key-name')

        expect(result).toBe(false)
    })
})
