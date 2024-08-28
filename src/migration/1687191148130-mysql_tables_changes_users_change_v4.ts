import { MigrationInterface, QueryRunner } from 'typeorm';

export class MysqlTablesChangesUsersChangeV41687191148130
  implements MigrationInterface
{
  name = 'MysqlTablesChangesUsersChangeV41687191148130';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` ADD \`legacyUsers\` tinyint NOT NULL DEFAULT 0`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` DROP COLUMN \`legacyUsers\``,
    );
  }
}
