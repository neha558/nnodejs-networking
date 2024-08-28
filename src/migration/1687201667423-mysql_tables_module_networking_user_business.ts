import { MigrationInterface, QueryRunner } from 'typeorm';

export class MysqlTablesModuleNetworkingUserBusiness1687201667423
  implements MigrationInterface
{
  name = 'MysqlTablesModuleNetworkingUserBusiness1687201667423';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `CREATE TABLE \`module_networking_user_business\` (\`id\` int NOT NULL AUTO_INCREMENT, \`isActive\` tinyint NOT NULL DEFAULT 1, \`isArchived\` tinyint NOT NULL DEFAULT 0, \`createDateTime\` datetime(6) NOT NULL DEFAULT current_timestamp(6), \`createdBy\` varchar(300) NULL, \`lastChangedDateTime\` datetime(6) NOT NULL DEFAULT current_timestamp(6) ON UPDATE current_timestamp(6), \`lastChangedBy\` varchar(300) NULL, \`internalComment\` varchar(300) NULL, \`node\` int NOT NULL, \`account_address\` text NOT NULL, \`joined_account_address\` text NOT NULL, \`businessIncome\` float NOT NULL DEFAULT '0', PRIMARY KEY (\`id\`)) ENGINE=InnoDB`,
    );
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(`DROP TABLE \`module_networking_user_business\``);
  }
}
