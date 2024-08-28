import { Pack } from 'src/models/pack.entity';
import { Rank } from 'src/models/rank.entity';
import { MigrationInterface, QueryRunner } from 'typeorm';
import { ranks } from './seeds/ranks.seeds';
import { packs } from './seeds/packs.seeds';

export class MysqlTablesChangesUsersAndBonusTableV31687016184598
  implements MigrationInterface
{
  name = 'MysqlTablesChangesUsersAndBonusTableV31687016184598';

  public async up(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.query(
      `ALTER TABLE \`module_networking_ranks\` ADD \`directSponsorRequired\` int NOT NULL`,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` ADD \`rankBusinessATeamBucket\` int NOT NULL DEFAULT '0'`,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` ADD \`rankBusinessBTeamBucket\` int NOT NULL DEFAULT '0'`,
    );
    // // fifth
    await queryRunner.connection.getRepository(Rank).save(ranks);
    await queryRunner.connection.getRepository(Pack).save(packs);
  }

  public async down(queryRunner: QueryRunner): Promise<void> {
    await queryRunner.connection.getRepository(Rank).delete({});
    await queryRunner.connection.getRepository(Pack).delete({});

    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` DROP COLUMN \`rankBusinessBTeamBucket\``,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_users\` DROP COLUMN \`rankBusinessATeamBucket\``,
    );
    await queryRunner.query(
      `ALTER TABLE \`module_networking_ranks\` DROP COLUMN \`directSponsorRequired\``,
    );
  }
}
