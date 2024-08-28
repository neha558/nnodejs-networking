import { MigrationInterface, QueryRunner } from 'typeorm';

export class MysqlTablesChangesUsersAndBonusTable1686860298123
  implements MigrationInterface
{
  name = 'MysqlTablesChangesUsersAndBonusTable1686860298123';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_bonus\` ADD \`canClaim\` tinyint NOT NULL DEFAULT 0`,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_bonus\` ADD \`flushAmount\` float NOT NULL`,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` ADD \`sponsorTree\` text NOT NULL`,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` ADD \`lastBoughtPack\` int NOT NULL DEFAULT '0'`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` DROP COLUMN \`lastBoughtPack\``,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` DROP COLUMN \`sponsorTree\``,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_bonus\` DROP COLUMN \`flushAmount\``,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_bonus\` DROP COLUMN \`canClaim\``,
    );
  }
}
