import { MigrationInterface, QueryRunner } from 'typeorm';

export class MysqlTablesChangesUsersAndBonusTableV21686891324505
  implements MigrationInterface
{
  name = 'MysqlTablesChangesUsersAndBonusTableV21686891324505';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_bonus\` ADD \`onHold\` tinyint NOT NULL DEFAULT 0`,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` ADD \`lastBoughtPackPrice\` int NOT NULL DEFAULT '0'`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` DROP COLUMN \`lastBoughtPackPrice\``,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_bonus\` DROP COLUMN \`onHold\``,
    );
  }
}
