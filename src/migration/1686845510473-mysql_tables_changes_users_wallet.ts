import { MigrationInterface, QueryRunner } from 'typeorm';

export class MysqlTablesChangesUsersWallet1686845510473
  implements MigrationInterface
{
  name = 'MysqlTablesChangesUsersWallet1686845510473';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `DROP INDEX \`IDX_bed3d0a9c8a1989953e5d17274\` ON \`module_web3_wallet_users\``,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE UNIQUE INDEX \`IDX_bed3d0a9c8a1989953e5d17274\` ON \`module_web3_wallet_users\` (\`_id\`)`,
    );
  }
}
