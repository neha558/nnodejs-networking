import { MigrationInterface, QueryRunner } from 'typeorm';

export class MysqlTablesModuleAmountAdded1688060410405
  implements MigrationInterface
{
  name = 'MysqlTablesModuleAmountAdded1688060410405';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_withdrawal\` ADD \`amount\` float NOT NULL DEFAULT '0'`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_user_withdrawal\` DROP COLUMN \`amount\``,
    );
  }
}
